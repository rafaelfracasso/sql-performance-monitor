"""
Coleta queries em execução e métricas de performance via views do PostgreSQL.
"""
from typing import List, Dict
from datetime import datetime
from ..core.base_collector import BaseQueryCollector


class PostgreSQLCollector(BaseQueryCollector):
    """Coleta queries ativas e suas métricas usando views do PostgreSQL."""

    def __init__(self, connection):
        """
        Inicializa coletor.

        Args:
            connection: Conexão ativa com PostgreSQL.
        """
        super().__init__(connection)

    def collect_active_queries(self, min_duration_seconds: int = 5) -> List[Dict]:
        """
        Coleta queries atualmente em execução via pg_stat_activity.

        Args:
            min_duration_seconds: Duração mínima em segundos para coletar.

        Returns:
            Lista de dicionários com informações das queries.
        """
        query = """
        SELECT
            pid,
            query_start,
            state,
            query,
            datname,
            usename,
            application_name,
            COALESCE(client_addr::text, 'local'),
            EXTRACT(EPOCH FROM (NOW() - query_start))::INTEGER as duration_seconds,
            EXTRACT(EPOCH FROM (NOW() - query_start))::INTEGER * 1000 as elapsed_time_ms
        FROM pg_stat_activity
        WHERE state = 'active'
            AND pid != pg_backend_pid()
            AND query NOT LIKE '%%pg_stat_activity%%'
            AND EXTRACT(EPOCH FROM (NOW() - query_start))::INTEGER >= %s
        ORDER BY query_start ASC
        """

        results = self.connection.execute_query(query, (min_duration_seconds,))

        if not results:
            return []

        queries = []

        # Debug: verificar estrutura da primeira linha
        if results and len(results) > 0:
            first_row = results[0]
            if len(first_row) != 10:
                print(f"⚠️  Query retornou {len(first_row)} colunas, esperado 10")
                print(f"    Colunas: {first_row}")
                return []
        for row in results:
            try:
                # Garantir que temos todas as 10 colunas esperadas
                if len(row) < 10:
                    print(f"⚠️  Linha com menos colunas que o esperado: {len(row)} colunas")
                    continue

                query_info = {
                    'session_id': row[0],  # pid
                    'request_id': row[0],  # PostgreSQL não tem request_id, usando pid
                    'start_time': row[1],
                    'status': row[2],  # state
                    'command': 'SELECT',  # PostgreSQL não distingue, inferir do query text
                    'duration_seconds': row[8] if row[8] else 0,
                    'cpu_time_ms': 0,  # PostgreSQL pg_stat_activity não fornece CPU time diretamente
                    'logical_reads': 0,  # Não disponível em pg_stat_activity
                    'physical_reads': 0,  # Não disponível em pg_stat_activity
                    'writes': 0,  # Não disponível em pg_stat_activity
                    'elapsed_time_ms': row[9] if row[9] else 0,
                    'database_name': row[4] if row[4] else 'N/A',
                    'query_text': row[3].strip() if row[3] else '',
                    'full_query_text': row[3].strip() if row[3] else '',
                    'query_plan': None,  # pg_stat_activity não fornece plano
                    'host_name': row[7] if row[7] else 'N/A',
                    'program_name': row[6] if row[6] else 'N/A',
                    'login_name': row[5] if row[5] else 'N/A',
                    'client_interface_name': 'PostgreSQL'
                }
                queries.append(query_info)
            except (IndexError, TypeError) as e:
                print(f"⚠️  Erro ao processar linha: {e}")
                continue

        return queries

    def collect_recent_expensive_queries(self, top_n: int = 10) -> List[Dict]:
        """
        Coleta queries recentes mais caras via pg_stat_statements.

        NOTA: Requer extensão pg_stat_statements instalada.
        Para instalar: CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

        Args:
            top_n: Número de queries a retornar.

        Returns:
            Lista de queries mais caras.
        """
        # Verifica se pg_stat_statements está disponível
        check_query = """
        SELECT EXISTS (
            SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
        )
        """

        extension_exists = self.connection.execute_scalar(check_query)

        if not extension_exists:
            print("⚠️  Extensão pg_stat_statements não instalada. Execute: CREATE EXTENSION pg_stat_statements;")
            return []

        query = f"""
        SELECT
            calls as execution_count,
            (total_exec_time)::BIGINT as total_cpu_time_ms,
            (mean_exec_time)::BIGINT as avg_cpu_time_ms,
            shared_blks_read + shared_blks_hit as total_logical_reads,
            (shared_blks_read + shared_blks_hit) / GREATEST(calls, 1) as avg_logical_reads,
            shared_blks_read as total_physical_reads,
            (total_exec_time)::BIGINT as total_elapsed_time_ms,
            (mean_exec_time)::BIGINT as avg_elapsed_time_ms,
            NULL as creation_time,
            NULL as last_execution_time,
            NULL as database_name,
            NULL as object_name,
            query as query_text,
            NULL as query_plan
        FROM pg_stat_statements
        WHERE calls > 0
        ORDER BY total_exec_time DESC
        LIMIT {top_n}
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
                'query_plan': row[13]
            }
            queries.append(query_info)

        return queries

    def get_table_scan_queries(self) -> List[Dict]:
        """
        Identifica tabelas com muitos sequential scans (table scans).

        PostgreSQL não vincula scans diretamente a queries no pg_stat_user_tables,
        então retornamos estatísticas de tabelas com alto número de sequential scans.

        Returns:
            Lista de informações de tabelas com table scans.
        """
        query = """
        SELECT
            schemaname,
            relname,
            seq_scan as execution_count,
            seq_tup_read as total_logical_reads,
            CASE WHEN seq_scan > 0 THEN seq_tup_read / seq_scan ELSE 0 END as avg_logical_reads,
            0 as total_cpu_time_ms,
            '' as query_text,
            NULL as query_plan
        FROM pg_stat_user_tables
        WHERE seq_scan > 100
            AND schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY seq_tup_read DESC
        LIMIT 20
        """

        results = self.connection.execute_query(query)

        if not results:
            return []

        queries = []
        for row in results:
            query_info = {
                'database_name': 'current',  # PostgreSQL context é sempre database atual
                'object_name': f"{row[0]}.{row[1]}",  # schema.table
                'execution_count': row[2],
                'total_logical_reads': row[3],
                'avg_logical_reads': row[4],
                'total_cpu_time_ms': row[5],
                'query_text': f"-- Table {row[0]}.{row[1]} has {row[2]} sequential scans reading {row[3]} rows",
                'query_plan': row[7],
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
        SELECT datname
        FROM pg_database
        WHERE datistemplate = false
            AND datname NOT IN ('postgres')
        ORDER BY datname
        """

        results = self.connection.execute_query(query)

        if not results:
            return []

        return [row[0] for row in results]
