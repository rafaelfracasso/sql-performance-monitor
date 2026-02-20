"""
Coleta queries em execução e métricas de performance via system views do SAP HANA.
"""
from typing import List, Dict
from datetime import datetime
from ..core.base_collector import BaseQueryCollector


class HANACollector(BaseQueryCollector):
    """Coleta queries ativas e suas métricas usando system views do SAP HANA."""

    def __init__(self, connection):
        """
        Inicializa coletor.

        Args:
            connection: Conexão ativa com SAP HANA.
        """
        super().__init__(connection)
        self._fallback_warned = False

    def collect_active_queries(self, min_duration_seconds: int = 5) -> List[Dict]:
        """
        Coleta queries atualmente em execução via M_ACTIVE_STATEMENTS.
        HANA armazena tempos em microssegundos (us).

        Args:
            min_duration_seconds: Duração mínima em segundos para coletar.

        Returns:
            Lista de dicionários com informações das queries.
        """
        query = """
        SELECT
            s.CONNECTION_ID,
            s.STATEMENT_ID,
            s.START_MVCC_TIMESTAMP,
            s.STATEMENT_STATUS,
            s.STATEMENT_STRING,
            CURRENT_SCHEMA,
            c.USER_NAME,
            c.CLIENT_HOST,
            c.CLIENT_PID,
            s.TOTAL_EXECUTION_TIME / 1000000.0 as DURATION_SECONDS,
            s.TOTAL_EXECUTION_TIME / 1000.0 as ELAPSED_TIME_MS,
            s.ALLOCATED_MEMORY_SIZE
        FROM SYS.M_ACTIVE_STATEMENTS s
        LEFT JOIN SYS.M_CONNECTIONS c ON s.CONNECTION_ID = c.CONNECTION_ID AND s.HOST = c.HOST
        WHERE s.TOTAL_EXECUTION_TIME / 1000000.0 >= ?
            AND s.STATEMENT_STATUS = 'ACTIVE'
        ORDER BY s.TOTAL_EXECUTION_TIME DESC
        """

        try:
            results = self.connection.execute_query(query, (float(min_duration_seconds),))
        except Exception as e:
            print(f"[ERROR] Erro ao coletar queries ativas HANA: {e}")
            return []

        if not results:
            return []

        queries = []
        for row in results:
            try:
                # Tenta inferir schema do contexto ou usa o da conexão
                schema_context = row[5] if row[5] else 'N/A'

                query_info = {
                    'session_id': row[0],
                    'request_id': row[1],
                    'start_time': row[2],
                    'status': row[3],
                    'command': 'SELECT',
                    'duration_seconds': float(row[9]) if row[9] else 0.0,
                    'cpu_time_ms': 0.0,  # Não disponível em M_ACTIVE_STATEMENTS
                    'logical_reads': 0,  # Não disponível em M_ACTIVE_STATEMENTS
                    'physical_reads': 0,
                    'writes': 0,
                    'elapsed_time_ms': float(row[10]) if row[10] else 0.0,
                    'database_name': schema_context,
                    'schema_name': schema_context,
                    'table_name': None,
                    'query_text': row[4].strip() if row[4] else '',
                    'full_query_text': row[4].strip() if row[4] else '',
                    'query_plan': None,
                    'host_name': row[7] if row[7] else 'N/A',
                    'program_name': f"PID:{row[8]}" if row[8] else 'N/A',
                    'login_name': row[6] if row[6] else 'N/A',
                    'client_interface_name': 'SAP HANA',
                    'allocated_memory_bytes': row[11] if row[11] else 0,
                    'memory_mb': (float(row[11]) / 1024.0 / 1024.0) if row[11] else 0.0,
                    'query_type': 'active'
                }
                queries.append(query_info)
            except Exception as e:
                print(f"[ERROR] Erro ao processar linha HANA: {e}")
                continue

        return queries

    def collect_recent_expensive_queries(self, top_n: int = 10) -> List[Dict]:
        """
        Coleta queries caras do histórico via M_SQL_PLAN_CACHE.

        M_SQL_PLAN_CACHE mantém estatísticas agregadas de execução de queries
        (total, média, máx, min), ideal para análise de performance.

        Args:
            top_n: Número de queries a retornar.

        Returns:
            Lista de queries mais caras.
        """
        # HANA 2.0 usa M_SQL_PLAN_CACHE como fonte principal para análise
        # (tem métricas agregadas: TOTAL_, AVG_, MAX_, MIN_)
        return self._collect_from_plan_cache_fallback(top_n)


    def _collect_from_plan_cache_fallback(self, top_n: int = 10) -> List[Dict]:
        """
        Coleta queries de M_SQL_PLAN_CACHE (fonte principal para HANA 2.0).

        M_SQL_PLAN_CACHE contém estatísticas agregadas de execução,
        incluindo totais, médias, máximos e mínimos.

        Args:
            top_n: Número de queries a retornar.

        Returns:
            Lista de queries mais caras.
        """
        query = f"""
        SELECT
            STATEMENT_HASH,
            EXECUTION_COUNT,
            TOTAL_EXECUTION_TIME / 1000.0 as TOTAL_ELAPSED_TIME_MS,
            AVG_EXECUTION_TIME / 1000.0 as AVG_ELAPSED_TIME_MS,
            LAST_EXECUTION_TIMESTAMP,
            SCHEMA_NAME,
            ACCESSED_TABLES,
            STATEMENT_STRING,
            MAX_EXECUTION_TIME / 1000.0 as MAX_ELAPSED_MS,
            TOTAL_TABLE_LOAD_TIME_DURING_PREPARATION / 1000.0 as TOTAL_LOGICAL_READS_MS,
            TOTAL_LOCK_WAIT_DURATION / 1000.0 as TOTAL_LOCK_WAIT_MS,
            USER_NAME,
            APPLICATION_NAME,
            TOTAL_EXECUTION_MEMORY_SIZE / 1024.0 / 1024.0 as TOTAL_MEMORY_MB,
            AVG_EXECUTION_MEMORY_SIZE / 1024.0 / 1024.0 as AVG_MEMORY_MB,
            TOTAL_RESULT_RECORD_COUNT as TOTAL_ROWS,
            ACCESSED_TABLE_NAMES
        FROM SYS.M_SQL_PLAN_CACHE
        WHERE EXECUTION_COUNT > 0
            AND STATEMENT_STRING IS NOT NULL
            AND TOTAL_EXECUTION_TIME > 1000000
            AND SCHEMA_NAME NOT IN ('SYS', '_SYS_STATISTICS', '_SYS_REPO')
            AND STATEMENT_STRING NOT LIKE '%M_SQL_PLAN_CACHE%'
            AND STATEMENT_STRING NOT LIKE '%M_ACTIVE_STATEMENTS%'
            AND STATEMENT_STRING NOT LIKE '%M_CS_TABLES%'
        ORDER BY TOTAL_EXECUTION_TIME DESC
        LIMIT {top_n}
        """

        try:
            results = self.connection.execute_query(query)
        except Exception as e:
            print(f"[ERROR] Erro ao coletar plan cache HANA: {e}")
            return []

        if not results:
            return []

        queries = []
        for row in results:
            try:
                statement_hash = row[0] if row[0] else None
                exec_count = int(row[1]) if row[1] else 1
                total_elapsed_ms = float(row[2]) if row[2] else 0.0
                avg_elapsed_ms = float(row[3]) if row[3] else 0.0
                last_execution = row[4]
                schema_name = row[5] if row[5] else 'N/A'
                accessed_tables = row[6] if row[6] else ''
                statement_string = row[7].strip() if row[7] else ''
                max_elapsed_ms = float(row[8]) if row[8] else 0.0
                total_load_ms = float(row[9]) if row[9] else 0.0
                total_lock_wait_ms = float(row[10]) if row[10] else 0.0
                user_name = row[11] if row[11] else 'N/A'
                application_name = row[12] if row[12] else 'N/A'
                total_memory_mb = float(row[13]) if row[13] else 0.0
                avg_memory_mb = float(row[14]) if row[14] else 0.0
                total_rows = int(row[15]) if row[15] else 0
                accessed_table_names = row[16] if row[16] else ''

                # Extrair primeira tabela
                first_table_raw = accessed_table_names.split(',')[0].strip().replace('"', '')
                table_name = 'unknown'
                if first_table_raw:
                    if '.' in first_table_raw:
                        table_name = first_table_raw.split('.')[-1]
                    else:
                        table_name = first_table_raw

                # Calcular médias
                avg_rows = int(total_rows / exec_count) if exec_count > 0 else 0
                avg_cpu_ms = 0.0  # M_SQL_PLAN_CACHE não tem CPU agregado em HANA 2.0

                query_info = {
                    'statement_hash': statement_hash,
                    'execution_count': exec_count,
                    'total_cpu_time_ms': 0.0,  # Não disponível em M_SQL_PLAN_CACHE
                    'avg_cpu_time_ms': 0.0,
                    'cpu_time_ms': 0.0,
                    'total_logical_reads': int(total_load_ms),
                    'avg_logical_reads': int(total_load_ms / exec_count) if exec_count > 0 else 0,
                    'logical_reads': int(total_load_ms / exec_count) if exec_count > 0 else 0,
                    'physical_reads': 0,
                    'writes': 0,
                    'duration_seconds': avg_elapsed_ms / 1000.0,
                    'total_elapsed_time_ms': total_elapsed_ms,
                    'avg_elapsed_time_ms': avg_elapsed_ms,
                    'elapsed_time_ms': avg_elapsed_ms,
                    'duration_ms': avg_elapsed_ms,
                    'max_elapsed_time_ms': max_elapsed_ms,
                    'memory_mb': avg_memory_mb,
                    'total_memory_mb': total_memory_mb,
                    'avg_memory_mb': avg_memory_mb,
                    'row_count': avg_rows,
                    'total_rows': total_rows,
                    'avg_rows': avg_rows,
                    'total_lock_wait_time_ms': total_lock_wait_ms,
                    'creation_time': None,
                    'last_execution_time': last_execution,
                    'database_name': schema_name,
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'object_name': accessed_table_names,
                    'query_text': statement_string,
                    'query_plan': None,
                    'login_name': user_name,
                    'program_name': application_name,
                    'host_name': 'N/A',
                    'query_type': 'expensive'
                }
                queries.append(query_info)
            except Exception as e:
                print(f"[ERROR] Erro ao processar linha plan cache HANA: {e}")
                continue

        return queries


    def get_table_scan_queries(self) -> List[Dict]:
        """
        Identifica queries específicas que fazem scans em tabelas grandes.

        Combina M_SQL_PLAN_CACHE com M_CS_TABLES para identificar QUAIS queries
        estão causando table scans, não apenas estatísticas genéricas de tabelas.

        Returns:
            Lista de queries que fazem scans em tabelas grandes.
        """
        query = """
        WITH expensive_queries AS (
            SELECT
                STATEMENT_HASH,
                STATEMENT_STRING,
                SCHEMA_NAME,
                ACCESSED_TABLE_NAMES,
                EXECUTION_COUNT,
                TOTAL_EXECUTION_TIME / 1000.0 as TOTAL_TIME_MS,
                AVG_EXECUTION_TIME / 1000.0 as AVG_TIME_MS,
                TOTAL_RESULT_RECORD_COUNT as TOTAL_ROWS,
                LAST_EXECUTION_TIMESTAMP,
                USER_NAME,
                APPLICATION_NAME,
                CASE
                    WHEN LOCATE(ACCESSED_TABLE_NAMES, ',') > 0
                    THEN SUBSTRING(ACCESSED_TABLE_NAMES, 1, LOCATE(ACCESSED_TABLE_NAMES, ',') - 1)
                    ELSE ACCESSED_TABLE_NAMES
                END as FIRST_TABLE
            FROM SYS.M_SQL_PLAN_CACHE
            WHERE EXECUTION_COUNT > 5
                AND ACCESSED_TABLE_NAMES IS NOT NULL
                AND SCHEMA_NAME NOT IN ('SYS', '_SYS_STATISTICS', '_SYS_REPO')
                AND STATEMENT_STRING NOT LIKE '%M_SQL_PLAN_CACHE%'
                AND STATEMENT_STRING NOT LIKE '%M_ACTIVE_STATEMENTS%'
                AND STATEMENT_STRING NOT LIKE '%M_CS_TABLES%'
        ),
        large_tables AS (
            SELECT
                SCHEMA_NAME,
                TABLE_NAME,
                RECORD_COUNT,
                MEMORY_SIZE_IN_TOTAL / 1024.0 / 1024.0 as SIZE_MB,
                READ_COUNT
            FROM SYS.M_CS_TABLES
            WHERE RECORD_COUNT > 100000
                AND READ_COUNT > 100
        )
        SELECT
            eq.STATEMENT_HASH,
            eq.STATEMENT_STRING,
            eq.SCHEMA_NAME,
            lt.TABLE_NAME,
            eq.EXECUTION_COUNT,
            eq.TOTAL_TIME_MS,
            eq.AVG_TIME_MS,
            eq.TOTAL_ROWS,
            lt.RECORD_COUNT as TABLE_RECORDS,
            lt.SIZE_MB as TABLE_SIZE_MB,
            CASE
                WHEN eq.TOTAL_ROWS > lt.RECORD_COUNT * eq.EXECUTION_COUNT * 0.5 THEN 'FULL_SCAN'
                WHEN eq.TOTAL_ROWS > lt.RECORD_COUNT * eq.EXECUTION_COUNT * 0.1 THEN 'LARGE_SCAN'
                ELSE 'SELECTIVE'
            END as SCAN_TYPE,
            eq.LAST_EXECUTION_TIMESTAMP,
            eq.USER_NAME,
            eq.APPLICATION_NAME
        FROM expensive_queries eq
        INNER JOIN large_tables lt
            ON eq.FIRST_TABLE LIKE '%' || lt.TABLE_NAME || '%'
        WHERE eq.TOTAL_ROWS > 10000
        ORDER BY eq.TOTAL_TIME_MS DESC
        LIMIT 20
        """

        try:
            results = self.connection.execute_query(query)
        except Exception as e:
            print(f"[ERROR] Erro ao coletar table scans HANA: {e}")
            print(f"[INFO] Usando fallback de M_CS_TABLES (estatísticas genéricas)")
            return self._get_table_scan_fallback()

        if not results:
            return []

        queries = []
        for row in results:
            try:
                statement_hash = row[0] if row[0] else None
                statement_string = row[1].strip() if row[1] else ''
                schema_name = row[2] if row[2] else 'N/A'
                table_name = row[3] if row[3] else 'unknown'
                execution_count = int(row[4]) if row[4] else 0
                total_time_ms = float(row[5]) if row[5] else 0.0
                avg_time_ms = float(row[6]) if row[6] else 0.0
                total_rows = int(row[7]) if row[7] else 0
                table_records = int(row[8]) if row[8] else 0
                table_size_mb = float(row[9]) if row[9] else 0.0
                scan_type = row[10] if row[10] else 'UNKNOWN'
                last_execution = row[11]
                user_name = row[12] if row[12] else 'N/A'
                application_name = row[13] if row[13] else 'N/A'

                # Calcular média de rows por execução
                avg_rows = int(total_rows / execution_count) if execution_count > 0 else 0

                query_info = {
                    'statement_hash': statement_hash,
                    'database_name': schema_name,
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'object_name': f"{schema_name}.{table_name}",
                    'execution_count': execution_count,
                    'duration_seconds': avg_time_ms / 1000.0,
                    'total_elapsed_time_ms': total_time_ms,
                    'avg_elapsed_time_ms': avg_time_ms,
                    'elapsed_time_ms': avg_time_ms,
                    'duration_ms': avg_time_ms,
                    'total_cpu_time_ms': 0.0,  # Não disponível
                    'avg_cpu_time_ms': 0.0,
                    'cpu_time_ms': 0.0,
                    'row_count': avg_rows,
                    'total_rows': total_rows,
                    'avg_rows': avg_rows,
                    'table_records': table_records,
                    'table_size_mb': table_size_mb,
                    'scan_type': scan_type,
                    'logical_reads': 0,
                    'physical_reads': 0,
                    'writes': 0,
                    'query_text': statement_string,
                    'query_plan': None,
                    'has_table_scan': True,
                    'query_type': 'table_scan',
                    'last_execution_time': last_execution,
                    'login_name': user_name,
                    'host_name': 'N/A',
                    'program_name': application_name
                }
                queries.append(query_info)

            except (IndexError, TypeError) as e:
                print(f"[ERROR] Erro ao processar linha table scan: {e}")
                continue

        return queries

    def _get_table_scan_fallback(self) -> List[Dict]:
        """
        Fallback para quando M_EXPENSIVE_STATEMENTS não está disponível.

        Sem queries SQL reais, table scans genéricos não têm valor para análise LLM.
        Retorna lista vazia com log informativo.

        Returns:
            Lista vazia (table scans sem query real não servem para análise).
        """
        if not self._fallback_warned:
            print("[WARN] Fallback HANA: M_EXPENSIVE_STATEMENTS indisponível. "
                  "Table scans requerem queries reais para análise.")
            self._fallback_warned = True
        return []

    def get_database_list(self) -> List[str]:
        """
        Retorna lista de schemas disponíveis.

        Returns:
            Lista de nomes de schemas.
        """
        query = """
        SELECT SCHEMA_NAME
        FROM SYS.SCHEMAS
        WHERE SCHEMA_NAME NOT LIKE 'SYS%'
            AND SCHEMA_NAME NOT LIKE '_SYS_%'
        ORDER BY SCHEMA_NAME
        """

        results = self.connection.execute_query(query)

        if not results:
            return []

        return [row[0] for row in results]

    def get_wait_statistics(self) -> List[Dict]:
        """
        Coleta wait statistics agregadas via M_SERVICE_THREAD_SAMPLES.

        Agrupa por thread_state para obter visao geral de onde o HANA
        esta gastando tempo (Running, Network Read, Mutex Wait, etc).

        Returns:
            Lista de wait types com contagem de threads.
        """
        query = """
        SELECT
            THREAD_STATE,
            THREAD_TYPE,
            COUNT(*) as THREAD_COUNT,
            SUM(DURATION / 1000.0) as TOTAL_WAIT_MS
        FROM SYS.M_SERVICE_THREAD_SAMPLES
        WHERE TIMESTAMP > ADD_SECONDS(CURRENT_TIMESTAMP, -300)
            AND THREAD_STATE != 'Free'
        GROUP BY THREAD_STATE, THREAD_TYPE
        ORDER BY THREAD_COUNT DESC
        """

        try:
            results = self.connection.execute_query(query)
        except Exception as e:
            print(f"[HANA] Erro ao coletar wait statistics: {e}")
            return []

        if not results:
            return []

        waits = []
        for row in results:
            try:
                waits.append({
                    'thread_state': row[0] or 'Unknown',
                    'wait_type': f"{row[0] or 'Unknown'}.{row[1] or 'Unknown'}",
                    'thread_count': int(row[2]) if row[2] else 0,
                    'total_wait_ms': float(row[3]) if row[3] else 0.0,
                    'waiting_tasks_count': int(row[2]) if row[2] else 0
                })
            except (IndexError, TypeError) as e:
                print(f"[HANA] Erro ao processar wait stat: {e}")
                continue

        return waits
