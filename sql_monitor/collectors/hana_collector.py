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

    def collect_active_queries(self, min_duration_seconds: int = 5) -> List[Dict]:
        """
        Coleta queries atualmente em execução via M_ACTIVE_STATEMENTS.

        Args:
            min_duration_seconds: Duração mínima em segundos para coletar.

        Returns:
            Lista de dicionários com informações das queries.
        """
        query = """
        SELECT
            s.CONNECTION_ID,
            s.STATEMENT_ID,
            s.START_TIME,
            s.STATEMENT_STATUS,
            s.STATEMENT_STRING,
            s.SCHEMA_NAME,
            c.USER_NAME,
            c.CLIENT_HOST,
            c.CLIENT_PID,
            SECONDS_BETWEEN(s.START_TIME, CURRENT_TIMESTAMP) as DURATION_SECONDS,
            SECONDS_BETWEEN(s.START_TIME, CURRENT_TIMESTAMP) * 1000 as ELAPSED_TIME_MS
        FROM SYS.M_ACTIVE_STATEMENTS s
        LEFT JOIN SYS.M_CONNECTIONS c ON s.CONNECTION_ID = c.CONNECTION_ID
        WHERE SECONDS_BETWEEN(s.START_TIME, CURRENT_TIMESTAMP) >= ?
            AND s.STATEMENT_STATUS = 'ACTIVE'
        ORDER BY s.START_TIME ASC
        """

        results = self.connection.execute_query(query, (min_duration_seconds,))

        if not results:
            return []

        queries = []
        for row in results:
            try:
                if len(row) < 11:
                    print(f"⚠️  Linha com menos colunas que o esperado: {len(row)} colunas")
                    continue

                query_info = {
                    'session_id': row[0],  # CONNECTION_ID
                    'request_id': row[1],  # STATEMENT_ID
                    'start_time': row[2],
                    'status': row[3],  # STATEMENT_STATUS
                    'command': 'SELECT',  # HANA não distingue, inferir do statement
                    'duration_seconds': row[9] if row[9] else 0,
                    'cpu_time_ms': 0,  # M_ACTIVE_STATEMENTS não fornece CPU time diretamente
                    'logical_reads': 0,  # Não disponível em M_ACTIVE_STATEMENTS
                    'physical_reads': 0,  # Não disponível em M_ACTIVE_STATEMENTS
                    'writes': 0,  # Não disponível em M_ACTIVE_STATEMENTS
                    'elapsed_time_ms': row[10] if row[10] else 0,
                    'database_name': row[5] if row[5] else 'N/A',  # SCHEMA_NAME
                    'query_text': row[4].strip() if row[4] else '',  # STATEMENT_STRING
                    'full_query_text': row[4].strip() if row[4] else '',
                    'query_plan': None,  # M_ACTIVE_STATEMENTS não fornece plano
                    'host_name': row[7] if row[7] else 'N/A',  # CLIENT_HOST
                    'program_name': f"PID:{row[8]}" if row[8] else 'N/A',  # CLIENT_PID
                    'login_name': row[6] if row[6] else 'N/A',  # USER_NAME
                    'client_interface_name': 'SAP HANA'
                }
                queries.append(query_info)

            except (IndexError, TypeError) as e:
                print(f"⚠️  Erro ao processar linha: {e}")
                continue

        return queries

    def collect_recent_expensive_queries(self, top_n: int = 10) -> List[Dict]:
        """
        Coleta queries recentes mais caras via M_SQL_PLAN_CACHE.

        Args:
            top_n: Número de queries a retornar.

        Returns:
            Lista de queries mais caras.
        """
        query = f"""
        SELECT
            EXECUTION_COUNT,
            TOTAL_CPU_TIME / 1000 as TOTAL_CPU_TIME_MS,
            AVG_CPU_TIME / 1000 as AVG_CPU_TIME_MS,
            TOTAL_PREPARATION_TIME + TOTAL_EXECUTION_TIME as TOTAL_ELAPSED_TIME_MS,
            (TOTAL_PREPARATION_TIME + TOTAL_EXECUTION_TIME) / EXECUTION_COUNT as AVG_ELAPSED_TIME_MS,
            LAST_EXECUTION_TIMESTAMP,
            SCHEMA_NAME,
            TABLE_NAMES,
            STATEMENT_STRING
        FROM SYS.M_SQL_PLAN_CACHE
        WHERE EXECUTION_COUNT > 0
            AND STATEMENT_STRING IS NOT NULL
        ORDER BY TOTAL_CPU_TIME DESC
        LIMIT {top_n}
        """

        results = self.connection.execute_query(query)

        if not results:
            return []

        queries = []
        for row in results:
            try:
                query_info = {
                    'execution_count': row[0],
                    'total_cpu_time_ms': row[1] if row[1] else 0,
                    'avg_cpu_time_ms': row[2] if row[2] else 0,
                    'total_logical_reads': 0,  # M_SQL_PLAN_CACHE não tem essa métrica
                    'avg_logical_reads': 0,
                    'total_physical_reads': 0,
                    'total_elapsed_time_ms': row[3] if row[3] else 0,
                    'avg_elapsed_time_ms': row[4] if row[4] else 0,
                    'creation_time': None,
                    'last_execution_time': row[5],
                    'database_name': row[6] if row[6] else 'N/A',  # SCHEMA_NAME
                    'object_name': row[7] if row[7] else 'N/A',  # TABLE_NAMES
                    'query_text': row[8].strip() if row[8] else '',
                    'query_plan': None
                }
                queries.append(query_info)

            except (IndexError, TypeError) as e:
                print(f"⚠️  Erro ao processar linha: {e}")
                continue

        return queries

    def get_table_scan_queries(self) -> List[Dict]:
        """
        Identifica tabelas com muitos table scans via M_TABLE_STATISTICS.

        HANA não vincula scans diretamente a queries específicas,
        então retornamos estatísticas de tabelas com alto número de scans.

        Returns:
            Lista de informações de tabelas com table scans.
        """
        query = """
        SELECT
            SCHEMA_NAME,
            TABLE_NAME,
            READ_COUNT as EXECUTION_COUNT,
            0 as TOTAL_LOGICAL_READS,
            0 as AVG_LOGICAL_READS,
            0 as TOTAL_CPU_TIME_MS
        FROM SYS.M_TABLE_STATISTICS
        WHERE READ_COUNT > 100
            AND SCHEMA_NAME NOT IN ('SYS', '_SYS_STATISTICS', '_SYS_REPO')
        ORDER BY READ_COUNT DESC
        LIMIT 20
        """

        results = self.connection.execute_query(query)

        if not results:
            return []

        queries = []
        for row in results:
            try:
                query_info = {
                    'database_name': 'current',  # HANA context é sempre database atual
                    'object_name': f"{row[0]}.{row[1]}",  # schema.table
                    'execution_count': row[2],
                    'total_logical_reads': row[3],
                    'avg_logical_reads': row[4],
                    'total_cpu_time_ms': row[5],
                    'query_text': f"-- Table {row[0]}.{row[1]} has {row[2]} reads (potential table scans)",
                    'query_plan': None,
                    'has_table_scan': True
                }
                queries.append(query_info)

            except (IndexError, TypeError) as e:
                print(f"⚠️  Erro ao processar linha: {e}")
                continue

        return queries

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
