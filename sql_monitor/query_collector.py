"""
Coleta queries em execução e métricas de performance via DMVs do SQL Server.
"""
from typing import List, Dict, Optional
from .connection import SQLServerConnection


class QueryCollector:
    """Coleta queries ativas e suas métricas usando Dynamic Management Views."""

    def __init__(self, connection: SQLServerConnection):
        """
        Inicializa coletor.

        Args:
            connection: Conexão ativa com SQL Server.
        """
        self.connection = connection

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
            s.client_interface_name
        FROM sys.dm_exec_requests r
        CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) qt
        CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) qp
        LEFT JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
        WHERE r.session_id != @@SPID  -- Exclui a própria query
            AND r.session_id > 50      -- Exclui sessões do sistema
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
                'database_name': row[11],
                'query_text': row[12].strip() if row[12] else '',
                'full_query_text': row[13].strip() if row[13] else '',
                'query_plan': str(row[14]) if row[14] else None,
                'host_name': row[15] if row[15] else 'N/A',
                'program_name': row[16] if row[16] else 'N/A',
                'login_name': row[17] if row[17] else 'N/A',
                'client_interface_name': row[18] if row[18] else 'N/A'
            }
            queries.append(query_info)

        return queries

    def collect_recent_expensive_queries(self, top_n: int = 10) -> List[Dict]:
        """
        Coleta queries recentes mais caras em termos de recursos.

        Args:
            top_n: Número de queries a retornar.

        Returns:
            Lista de queries mais caras.
        """
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
            qp.query_plan
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
        CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
        WHERE qs.last_execution_time >= DATEADD(HOUR, -1, GETDATE())  -- Última hora
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
                'total_logical_reads': row[3],
                'avg_logical_reads': row[4],
                'total_physical_reads': row[5],
                'total_elapsed_time_ms': row[6],
                'avg_elapsed_time_ms': row[7],
                'creation_time': row[8],
                'last_execution_time': row[9],
                'database_name': row[10],
                'object_name': row[11],
                'query_text': row[12].strip() if row[12] else '',
                'query_plan': str(row[13]) if row[13] else None
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
