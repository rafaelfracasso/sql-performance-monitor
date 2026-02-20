"""
Coleta queries em execução e métricas de performance via DMVs do SQL Server.
"""
from typing import List, Dict
from ..core.base_collector import BaseQueryCollector


class SQLServerCollector(BaseQueryCollector):
    """Coleta queries ativas e suas métricas usando Dynamic Management Views do SQL Server."""

    def __init__(self, connection):
        """
        Inicializa coletor.

        Args:
            connection: Conexão ativa com SQL Server.
        """
        super().__init__(connection)

    def collect_active_queries(self, min_duration_seconds: int = 5) -> List[Dict]:
        """
        Coleta queries atualmente em execução.

        Args:
            min_duration_seconds: Duração mínima em segundos para coletar.

        Returns:
            Lista de dicionários com informações das queries.
        """
        query = """
        SELECT
            r.session_id,
            r.request_id,
            r.start_time,
            r.status,
            r.command,
            DATEDIFF(SECOND, r.start_time, GETDATE()) as duration_seconds,
            r.cpu_time as cpu_time_ms,
            r.logical_reads,
            r.reads as physical_reads,
            r.writes,
            r.total_elapsed_time as elapsed_time_ms,
            DB_NAME(r.database_id) as database_name,
            SUBSTRING(
                qt.text,
                (r.statement_start_offset/2) + 1,
                ((CASE r.statement_end_offset
                    WHEN -1 THEN DATALENGTH(qt.text)
                    ELSE r.statement_end_offset
                END - r.statement_start_offset)/2) + 1
            ) as query_text,
            qt.text as full_query_text,
            qp.query_plan,
            s.host_name,
            s.program_name,
            s.login_name,
            s.client_interface_name,
            r.row_count,
            r.wait_time as wait_time_ms,
            r.wait_type,
            r.blocking_session_id,
            COALESCE(mg.granted_memory_kb / 1024.0, 0) as memory_mb
        FROM sys.dm_exec_requests r
        CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) qt
        CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) qp
        LEFT JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
        LEFT JOIN sys.dm_exec_query_memory_grants mg ON r.session_id = mg.session_id AND r.request_id = mg.request_id
        WHERE r.session_id != @@SPID  -- Exclui a própria query
            AND r.session_id > 50      -- Exclui sessões do sistema
            AND DB_NAME(r.database_id) NOT IN ('master', 'tempdb', 'model', 'msdb', 'distribution')
            AND DATEDIFF(SECOND, r.start_time, GETDATE()) >= ?
        ORDER BY r.total_elapsed_time DESC
        """

        results = self.connection.execute_query(query, (min_duration_seconds,))

        if not results:
            return []

        queries = []
        for row in results:
            query_info = {
                'session_id': row[0],
                'request_id': row[1],
                'start_time': row[2],
                'status': row[3],
                'command': row[4],
                'duration_seconds': row[5],
                'cpu_time_ms': row[6],
                'logical_reads': row[7],
                'physical_reads': row[8],
                'writes': row[9],
                'elapsed_time_ms': row[10],
                'duration_ms': row[10],  # Alias para compatibilidade
                'database_name': row[11],
                'query_text': row[12].strip() if row[12] else '',
                'full_query_text': row[13].strip() if row[13] else '',
                'query_plan': str(row[14]) if row[14] else None,
                'host_name': row[15] if row[15] else 'N/A',
                'program_name': row[16] if row[16] else 'N/A',
                'login_name': row[17] if row[17] else 'N/A',
                'client_interface_name': row[18] if row[18] else 'N/A',
                'query_type': row[4].strip() if row[4] else 'unknown',
                'row_count': row[19] or 0,
                'wait_time_ms': row[20] or 0,
                'wait_type': row[21],
                'blocking_session_id': row[22],
                'memory_mb': row[23] or 0
            }
            queries.append(query_info)

        return queries

    def collect_recent_expensive_queries(self, top_n: int = 10) -> List[Dict]:
        """
        Coleta queries recentes mais caras em termos de recursos.
        Inclui metricas de memoria, rows e tempdb spills (SQL Server 2016+).

        Args:
            top_n: Numero de queries a retornar.

        Returns:
            Lista de queries mais caras.
        """
        # Query com metricas estendidas (SQL Server 2016+)
        query = f"""
        SELECT TOP {top_n}
            qs.execution_count,
            qs.total_worker_time / 1000 as total_cpu_time_ms,
            qs.total_worker_time / qs.execution_count / 1000 as avg_cpu_time_ms,
            qs.total_logical_reads,
            qs.total_logical_reads / qs.execution_count as avg_logical_reads,
            qs.total_physical_reads,
            qs.total_physical_reads / qs.execution_count as avg_physical_reads,
            qs.total_elapsed_time / 1000 as total_elapsed_time_ms,
            qs.total_elapsed_time / qs.execution_count / 1000 as avg_elapsed_time_ms,
            qs.creation_time,
            qs.last_execution_time,
            DB_NAME(qt.dbid) as database_name,
            OBJECT_NAME(qt.objectid, qt.dbid) as object_name,
            qt.text as query_text,
            qp.query_plan,
            -- Metricas de rows
            qs.total_rows,
            qs.total_rows / NULLIF(qs.execution_count, 0) as avg_rows,
            -- Metricas de memoria (SQL Server 2016+)
            COALESCE(qs.total_grant_kb, 0) / 1024.0 as total_grant_mb,
            COALESCE(qs.total_grant_kb / NULLIF(qs.execution_count, 0), 0) / 1024.0 as avg_grant_mb,
            COALESCE(qs.total_used_grant_kb, 0) / 1024.0 as total_used_grant_mb,
            COALESCE(qs.total_ideal_grant_kb, 0) / 1024.0 as total_ideal_grant_mb,
            -- Tempdb spills (SQL Server 2016+)
            COALESCE(qs.total_spills, 0) as total_spills,
            COALESCE(qs.total_spills / NULLIF(qs.execution_count, 0), 0) as avg_spills,
            -- Writes
            qs.total_logical_writes,
            qs.total_logical_writes / NULLIF(qs.execution_count, 0) as avg_logical_writes
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
        CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
        WHERE qs.last_execution_time >= DATEADD(HOUR, -1, GETDATE())
            AND DB_NAME(qt.dbid) NOT IN ('master', 'tempdb', 'model', 'msdb', 'distribution')
        ORDER BY qs.total_worker_time DESC
        """

        try:
            results = self.connection.execute_query(query)
        except Exception as e:
            # Fallback para SQL Server mais antigo sem colunas de grant/spills
            if 'total_grant_kb' in str(e) or 'total_spills' in str(e):
                return self._collect_recent_expensive_queries_legacy(top_n)
            raise

        if not results:
            return []

        queries = []
        for row in results:
            query_info = {
                'execution_count': row[0],
                'total_cpu_time_ms': row[1],
                'avg_cpu_time_ms': row[2],
                'cpu_time_ms': row[2],  # Alias para compatibilidade
                'total_logical_reads': row[3],
                'avg_logical_reads': row[4],
                'logical_reads': row[4],  # Alias
                'total_physical_reads': row[5],
                'avg_physical_reads': row[6],
                'physical_reads': row[6],  # Alias
                'total_elapsed_time_ms': row[7],
                'avg_elapsed_time_ms': row[8],
                'duration_ms': row[8],  # Alias
                'creation_time': row[9],
                'last_execution_time': row[10],
                'database_name': row[11],
                'object_name': row[12],
                'query_text': row[13].strip() if row[13] else '',
                'query_plan': str(row[14]) if row[14] else None,
                # Metricas de rows
                'total_rows': row[15] or 0,
                'avg_rows': row[16] or 0,
                'row_count': row[16] or 0,  # Alias
                # Metricas de memoria
                'total_grant_mb': row[17] or 0,
                'avg_grant_mb': row[18] or 0,
                'memory_mb': row[18] or 0,  # Alias
                'total_used_grant_mb': row[19] or 0,
                'total_ideal_grant_mb': row[20] or 0,
                # Tempdb spills
                'total_spills': row[21] or 0,
                'avg_spills': row[22] or 0,
                # Writes
                'total_logical_writes': row[23] or 0,
                'avg_logical_writes': row[24] or 0,
                'writes': row[24] or 0,  # Alias
                # Tipo de query
                'query_type': row[13].strip().split()[0].upper() if row[13] and row[13].strip() else 'unknown',
                # Campos para compatibilidade
                'wait_time_ms': 0,
                'wait_type': None
            }
            queries.append(query_info)

        return queries

    def _collect_recent_expensive_queries_legacy(self, top_n: int = 10) -> List[Dict]:
        """Fallback para SQL Server < 2016 sem metricas de grant/spills."""
        query = f"""
        SELECT TOP {top_n}
            qs.execution_count,
            qs.total_worker_time / 1000 as total_cpu_time_ms,
            qs.total_worker_time / qs.execution_count / 1000 as avg_cpu_time_ms,
            qs.total_logical_reads,
            qs.total_logical_reads / qs.execution_count as avg_logical_reads,
            qs.total_physical_reads,
            qs.total_elapsed_time / 1000 as total_elapsed_time_ms,
            qs.total_elapsed_time / qs.execution_count / 1000 as avg_elapsed_time_ms,
            qs.creation_time,
            qs.last_execution_time,
            DB_NAME(qt.dbid) as database_name,
            OBJECT_NAME(qt.objectid, qt.dbid) as object_name,
            qt.text as query_text,
            qp.query_plan,
            qs.total_rows,
            qs.total_logical_writes
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
        CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
        WHERE qs.last_execution_time >= DATEADD(HOUR, -1, GETDATE())
            AND DB_NAME(qt.dbid) NOT IN ('master', 'tempdb', 'model', 'msdb', 'distribution')
        ORDER BY qs.total_worker_time DESC
        """

        results = self.connection.execute_query(query)
        if not results:
            return []

        queries = []
        for row in results:
            query_info = {
                'execution_count': row[0],
                'total_cpu_time_ms': row[1],
                'avg_cpu_time_ms': row[2],
                'cpu_time_ms': row[2],
                'total_logical_reads': row[3],
                'avg_logical_reads': row[4],
                'logical_reads': row[4],
                'total_physical_reads': row[5],
                'total_elapsed_time_ms': row[6],
                'avg_elapsed_time_ms': row[7],
                'duration_ms': row[7],
                'creation_time': row[8],
                'last_execution_time': row[9],
                'database_name': row[10],
                'object_name': row[11],
                'query_text': row[12].strip() if row[12] else '',
                'query_plan': str(row[13]) if row[13] else None,
                'total_rows': row[14] or 0,
                'total_logical_writes': row[15] or 0,
                'writes': (row[15] or 0) / max(row[0], 1),
                'query_type': row[12].strip().split()[0].upper() if row[12] and row[12].strip() else 'unknown',
                # Campos vazios para compatibilidade
                'memory_mb': 0,
                'avg_spills': 0,
                'wait_time_ms': 0,
                'wait_type': None
            }
            queries.append(query_info)

        return queries

    def get_table_scan_queries(self) -> List[Dict]:
        """
        Identifica queries fazendo table scans (sem usar índices).

        IMPORTANTE: Esta query foi otimizada para SQL Server 2016+ para evitar
        overhead de CPU/memória ao fazer CAST de todos os planos XML.

        Returns:
            Lista de queries com table scans.
        """
        query = """
        SELECT TOP 20
            DB_NAME(qt.dbid) as database_name,
            OBJECT_NAME(qt.objectid, qt.dbid) as object_name,
            qs.execution_count,
            qs.total_logical_reads,
            qs.total_logical_reads / qs.execution_count as avg_logical_reads,
            qs.total_worker_time / 1000 as total_cpu_time_ms,
            qt.text as query_text,
            qp.query_plan
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
        CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
        WHERE qs.last_execution_time >= DATEADD(HOUR, -1, GETDATE())
            AND qs.total_logical_reads > 10000  -- Filtro ANTES do CAST para performance
            AND qp.query_plan.exist('//RelOp[@PhysicalOp="Table Scan"]') = 1
            AND DB_NAME(qt.dbid) NOT IN ('master', 'tempdb', 'model', 'msdb', 'distribution')
        ORDER BY qs.total_logical_reads DESC
        """

        results = self.connection.execute_query(query)

        if not results:
            return []

        queries = []
        for row in results:
            query_info = {
                'database_name': row[0],
                'object_name': row[1],
                'execution_count': row[2],
                'total_logical_reads': row[3],
                'avg_logical_reads': row[4],
                'total_cpu_time_ms': row[5],
                'query_text': row[6].strip() if row[6] else '',
                'query_plan': str(row[7]) if row[7] else None,
                'has_table_scan': True
            }
            queries.append(query_info)

        return queries

    def get_database_list(self) -> List[str]:
        """
        Retorna lista de databases disponíveis.

        Returns:
            Lista de nomes de databases.
        """
        query = """
        SELECT name
        FROM sys.databases
        WHERE state_desc = 'ONLINE'
            AND name NOT IN ('master', 'tempdb', 'model', 'msdb')
        ORDER BY name
        """

        results = self.connection.execute_query(query)

        if not results:
            return []

        return [row[0] for row in results]

    def get_blocking_sessions(self) -> List[Dict]:
        """
        Identifica sessoes bloqueando outras (locks).

        Returns:
            Lista de sessoes com bloqueios ativos.
        """
        query = """
        SELECT
            r.session_id as blocked_session_id,
            r.blocking_session_id,
            r.wait_time / 1000.0 as wait_time_seconds,
            r.wait_type,
            DB_NAME(r.database_id) as database_name,
            SUBSTRING(
                qt.text,
                (r.statement_start_offset/2) + 1,
                ((CASE r.statement_end_offset
                    WHEN -1 THEN DATALENGTH(qt.text)
                    ELSE r.statement_end_offset
                END - r.statement_start_offset)/2) + 1
            ) as blocked_query,
            bs.host_name as blocking_host,
            bs.program_name as blocking_program,
            bs.login_name as blocking_login,
            COALESCE(
                (SELECT TOP 1 SUBSTRING(
                    qt2.text,
                    (br.statement_start_offset/2) + 1,
                    ((CASE br.statement_end_offset
                        WHEN -1 THEN DATALENGTH(qt2.text)
                        ELSE br.statement_end_offset
                    END - br.statement_start_offset)/2) + 1
                )
                FROM sys.dm_exec_requests br
                CROSS APPLY sys.dm_exec_sql_text(br.sql_handle) qt2
                WHERE br.session_id = r.blocking_session_id),
                (SELECT TOP 1 qt3.text
                FROM sys.dm_exec_connections bc
                CROSS APPLY sys.dm_exec_sql_text(bc.most_recent_sql_handle) qt3
                WHERE bc.session_id = r.blocking_session_id)
            ) as blocking_query
        FROM sys.dm_exec_requests r
        CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) qt
        LEFT JOIN sys.dm_exec_sessions bs ON r.blocking_session_id = bs.session_id
        WHERE r.blocking_session_id > 0
            AND r.session_id != @@SPID
        ORDER BY r.wait_time DESC
        """

        results = self.connection.execute_query(query)

        if not results:
            return []

        blocks = []
        for row in results:
            block_info = {
                'blocked_session_id': row[0],
                'blocking_session_id': row[1],
                'wait_time_seconds': row[2],
                'wait_type': row[3],
                'database_name': row[4],
                'blocked_query': row[5].strip() if row[5] else '',
                'blocking_host': row[6] if row[6] else 'N/A',
                'blocking_program': row[7] if row[7] else 'N/A',
                'blocking_login': row[8] if row[8] else 'N/A',
                'blocking_query': row[9].strip() if row[9] else ''
            }
            blocks.append(block_info)

        return blocks

    def get_missing_indexes(self) -> List[Dict]:
        """
        Retorna indices sugeridos pelo SQL Server.

        Returns:
            Lista de indices faltantes com impacto estimado.
        """
        query = """
        SELECT TOP 20
            DB_NAME(mid.database_id) as database_name,
            OBJECT_NAME(mid.object_id, mid.database_id) as table_name,
            mid.equality_columns,
            mid.inequality_columns,
            mid.included_columns,
            migs.user_seeks,
            migs.user_scans,
            migs.avg_total_user_cost,
            migs.avg_user_impact,
            migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans) as improvement_measure
        FROM sys.dm_db_missing_index_details mid
        INNER JOIN sys.dm_db_missing_index_groups mig ON mid.index_handle = mig.index_handle
        INNER JOIN sys.dm_db_missing_index_group_stats migs ON mig.index_group_handle = migs.group_handle
        WHERE mid.database_id = DB_ID()
            AND DB_NAME(mid.database_id) NOT IN ('master', 'tempdb', 'model', 'msdb')
        ORDER BY improvement_measure DESC
        """

        results = self.connection.execute_query(query)

        if not results:
            return []

        indexes = []
        for row in results:
            index_info = {
                'database_name': row[0],
                'table_name': row[1],
                'equality_columns': row[2],
                'inequality_columns': row[3],
                'included_columns': row[4],
                'user_seeks': row[5],
                'user_scans': row[6],
                'avg_total_user_cost': row[7],
                'avg_user_impact': row[8],
                'improvement_measure': row[9]
            }
            indexes.append(index_info)

        return indexes

    def get_high_memory_grants(self, min_memory_mb: int = 100) -> List[Dict]:
        """
        Identifica queries com alto consumo de memoria (memory grants).

        Args:
            min_memory_mb: Memoria minima em MB para filtrar.

        Returns:
            Lista de queries com alto memory grant.
        """
        query = """
        SELECT
            mg.session_id,
            mg.request_id,
            DB_NAME(r.database_id) as database_name,
            mg.requested_memory_kb / 1024.0 as requested_memory_mb,
            mg.granted_memory_kb / 1024.0 as granted_memory_mb,
            mg.used_memory_kb / 1024.0 as used_memory_mb,
            mg.max_used_memory_kb / 1024.0 as max_used_memory_mb,
            mg.ideal_memory_kb / 1024.0 as ideal_memory_mb,
            mg.wait_time_ms,
            mg.is_small,
            SUBSTRING(
                qt.text,
                (r.statement_start_offset/2) + 1,
                ((CASE r.statement_end_offset
                    WHEN -1 THEN DATALENGTH(qt.text)
                    ELSE r.statement_end_offset
                END - r.statement_start_offset)/2) + 1
            ) as query_text,
            s.host_name,
            s.program_name,
            s.login_name
        FROM sys.dm_exec_query_memory_grants mg
        INNER JOIN sys.dm_exec_requests r ON mg.session_id = r.session_id AND mg.request_id = r.request_id
        CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) qt
        LEFT JOIN sys.dm_exec_sessions s ON mg.session_id = s.session_id
        WHERE mg.granted_memory_kb / 1024.0 >= ?
            AND mg.session_id != @@SPID
        ORDER BY mg.granted_memory_kb DESC
        """

        results = self.connection.execute_query(query, (min_memory_mb,))

        if not results:
            return []

        grants = []
        for row in results:
            grant_info = {
                'session_id': row[0],
                'request_id': row[1],
                'database_name': row[2],
                'requested_memory_mb': row[3],
                'granted_memory_mb': row[4],
                'used_memory_mb': row[5],
                'max_used_memory_mb': row[6],
                'ideal_memory_mb': row[7],
                'wait_time_ms': row[8],
                'is_small': row[9],
                'query_text': row[10].strip() if row[10] else '',
                'host_name': row[11] if row[11] else 'N/A',
                'program_name': row[12] if row[12] else 'N/A',
                'login_name': row[13] if row[13] else 'N/A'
            }
            grants.append(grant_info)

        return grants

    def get_wait_statistics(self) -> List[Dict]:
        """
        Retorna estatisticas de wait types mais significativos.

        Returns:
            Lista de wait types com metricas.
        """
        query = """
        SELECT TOP 20
            wait_type,
            wait_time_ms / 1000.0 as wait_time_seconds,
            signal_wait_time_ms / 1000.0 as signal_wait_time_seconds,
            (wait_time_ms - signal_wait_time_ms) / 1000.0 as resource_wait_time_seconds,
            waiting_tasks_count,
            CASE WHEN waiting_tasks_count > 0
                THEN wait_time_ms * 1.0 / waiting_tasks_count
                ELSE 0
            END as avg_wait_time_ms
        FROM sys.dm_os_wait_stats
        WHERE wait_type NOT IN (
            'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK',
            'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR', 'LOGMGR_QUEUE',
            'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT',
            'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT', 'CLR_AUTO_EVENT',
            'DISPATCHER_QUEUE_SEMAPHORE', 'FT_IFTS_SCHEDULER_IDLE_WAIT',
            'XE_DISPATCHER_WAIT', 'XE_DISPATCHER_JOIN', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
            'ONDEMAND_TASK_QUEUE', 'BROKER_EVENTHANDLER', 'SLEEP_BPOOL_FLUSH',
            'SLEEP_DBSTARTUP', 'DIRTY_PAGE_POLL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
            'SP_SERVER_DIAGNOSTICS_SLEEP', 'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
            'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', 'QDS_SHUTDOWN_QUEUE'
        )
            AND wait_time_ms > 0
        ORDER BY wait_time_ms DESC
        """

        results = self.connection.execute_query(query)

        if not results:
            return []

        waits = []
        for row in results:
            wait_info = {
                'wait_type': row[0],
                'wait_time_seconds': row[1],
                'signal_wait_time_seconds': row[2],
                'resource_wait_time_seconds': row[3],
                'waiting_tasks_count': row[4],
                'avg_wait_time_ms': row[5]
            }
            waits.append(wait_info)

        return waits
