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
        self.extensions = self._check_extensions()

    def _check_extensions(self) -> Dict[str, bool]:
        """
        Verifica quais extensões de monitoramento estão instaladas.
        Evita falhas ao tentar acessar views que não existem.
        """
        try:
            query = """
            SELECT extname
            FROM pg_extension
            WHERE extname IN ('pg_stat_statements', 'pg_kcache', 'pg_wait_sampling')
            """
            rows = self.connection.execute_query(query)
            installed = {row[0] for row in rows} if rows else set()
            return {
                'pg_stat_statements': 'pg_stat_statements' in installed,
                'pg_kcache': 'pg_kcache' in installed,
                'pg_wait_sampling': 'pg_wait_sampling' in installed
            }
        except Exception as e:
            print(f"[WARN] Erro ao verificar extensoes: {e}")
            return {}

    def collect_active_queries(self, min_duration_seconds: int = 5) -> List[Dict]:
        """
        Coleta queries atualmente em execução via pg_stat_activity.
        Foca em queries realmente ativas e seus eventos de espera.

        Args:
            min_duration_seconds: Duração mínima em segundos para coletar.

        Returns:
            Lista de dicionários com informações das queries.
        """
        # PostgreSQL >= 9.6 usa wait_event_type e wait_event
        # Usamos state_change para medir tempo no estado atual
        query = """
        SELECT
            pid,
            state,
            datname,
            usename,
            application_name,
            COALESCE(client_addr::text, 'local'),
            query,
            EXTRACT(EPOCH FROM (clock_timestamp() - state_change))::NUMERIC(10,2) as duration_seconds,
            wait_event_type,
            wait_event,
            query_start
        FROM pg_stat_activity
        WHERE state != 'idle'
            AND pid != pg_backend_pid()
            AND query NOT LIKE '%%pg_stat_activity%%'
            AND EXTRACT(EPOCH FROM (clock_timestamp() - state_change)) >= %s
        ORDER BY duration_seconds DESC
        """

        try:
            results = self.connection.execute_query(query, (min_duration_seconds,))
        except Exception as e:
            print(f"[ERROR] Erro ao coletar queries ativas: {e}")
            return []

        if not results:
            return []

        queries = []
        for row in results:
            try:
                # wait info formatting
                wait_type = row[8]
                wait_event = row[9]
                wait_info = f"{wait_type}:{wait_event}" if wait_type else None

                query_info = {
                    'session_id': row[0],  # pid
                    'request_id': row[0],  # pid as request_id
                    'start_time': row[10], # query_start
                    'status': row[1],      # state
                    'command': 'SELECT',   # TODO: Inferir do texto se necessario
                    'duration_seconds': float(row[7]) if row[7] else 0.0,
                    'cpu_time_ms': 0,      # Nao disponivel em tempo real
                    'logical_reads': 0,    # Nao disponivel em tempo real
                    'physical_reads': 0,   # Nao disponivel em tempo real
                    'writes': 0,           # Nao disponivel em tempo real
                    'elapsed_time_ms': float(row[7]) * 1000 if row[7] else 0,
                    'database_name': row[2] if row[2] else 'N/A',
                    'query_text': row[6].strip() if row[6] else '',
                    'full_query_text': row[6].strip() if row[6] else '',
                    'query_plan': None,
                    'host_name': 'N/A', # row[5] is client_addr, not host_name
                    'program_name': row[4] if row[4] else 'N/A',
                    'login_name': row[3] if row[3] else 'N/A',
                    'client_interface_name': 'PostgreSQL',
                    'wait_info': wait_info # Campo extra util para PG
                }
                queries.append(query_info)
            except (IndexError, TypeError, ValueError) as e:
                print(f"[ERROR] Erro ao processar linha PG: {e}")
                continue

        return queries

    def collect_recent_expensive_queries(self, top_n: int = 10) -> List[Dict]:
        """
        Coleta queries recentes mais caras via pg_stat_statements (PG 13+).

        Args:
            top_n: Numero de queries a retornar.

        Returns:
            Lista de queries mais caras com metricas detalhadas.
        """
        if not self.extensions.get('pg_stat_statements'):
            print("[WARN] Extensao pg_stat_statements nao detectada. Pule coleta historica.")
            return []

        # Query completa para PG 13+ com metricas ricas
        # Inclui: rows, WAL stats, IO timing, buffer stats separados
        query = f"""
        SELECT
            pss.calls as execution_count,
            pss.total_exec_time as total_elapsed_time_ms,
            pss.mean_exec_time as avg_elapsed_time_ms,
            pss.rows as total_rows,
            CASE WHEN pss.calls > 0 THEN pss.rows / pss.calls ELSE 0 END as avg_rows,
            pss.shared_blks_hit,
            pss.shared_blks_read,
            pss.shared_blks_hit + pss.shared_blks_read as total_logical_reads,
            pss.shared_blks_dirtied,
            pss.shared_blks_written,
            pss.local_blks_hit,
            pss.local_blks_read,
            pss.temp_blks_read,
            pss.temp_blks_written,
            COALESCE(pss.blk_read_time, 0) as blk_read_time_ms,
            COALESCE(pss.blk_write_time, 0) as blk_write_time_ms,
            COALESCE(pss.wal_bytes, 0) as wal_bytes,
            COALESCE(pss.wal_records, 0) as wal_records,
            pss.query as query_text,
            pss.queryid,
            d.datname as database_name,
            pss.min_exec_time,
            pss.max_exec_time,
            pss.stddev_exec_time
        FROM pg_stat_statements pss
        LEFT JOIN pg_database d ON pss.dbid = d.oid
        WHERE pss.calls > 0
            AND pss.query NOT LIKE '%%pg_stat_statements%%'
        ORDER BY pss.total_exec_time DESC
        LIMIT {top_n}
        """

        try:
            results = self.connection.execute_query(query)
        except Exception as e:
            print(f"[ERROR] Erro ao consultar pg_stat_statements: {e}")
            return []

        if not results:
            return []

        queries = []
        for row in results:
            try:
                calls = int(row[0]) if row[0] else 1
                total_time_ms = float(row[1]) if row[1] else 0.0
                avg_time_ms = float(row[2]) if row[2] else 0.0
                total_rows = int(row[3]) if row[3] else 0
                avg_rows = int(row[4]) if row[4] else 0
                shared_blks_hit = int(row[5]) if row[5] else 0
                shared_blks_read = int(row[6]) if row[6] else 0
                total_logical = int(row[7]) if row[7] else 0
                shared_blks_dirtied = int(row[8]) if row[8] else 0
                shared_blks_written = int(row[9]) if row[9] else 0
                local_blks_hit = int(row[10]) if row[10] else 0
                local_blks_read = int(row[11]) if row[11] else 0
                temp_blks_read = int(row[12]) if row[12] else 0
                temp_blks_written = int(row[13]) if row[13] else 0
                blk_read_time_ms = float(row[14]) if row[14] else 0.0
                blk_write_time_ms = float(row[15]) if row[15] else 0.0
                wal_bytes = int(row[16]) if row[16] else 0
                wal_records = int(row[17]) if row[17] else 0
                query_text = row[18].strip() if row[18] else ''
                query_id = row[19]
                database_name = row[20] if row[20] else 'unknown'
                min_time_ms = float(row[21]) if row[21] else 0.0
                max_time_ms = float(row[22]) if row[22] else 0.0
                stddev_time_ms = float(row[23]) if row[23] else 0.0

                # Calcular hit ratio (eficiencia do buffer cache)
                total_buffer_access = shared_blks_hit + shared_blks_read
                hit_ratio = (shared_blks_hit / total_buffer_access * 100) if total_buffer_access > 0 else 100.0

                # Total temp = read + written (indica spill para disco)
                total_temp_blks = temp_blks_read + temp_blks_written

                query_info = {
                    'execution_count': calls,
                    # Tempo
                    'total_elapsed_time_ms': total_time_ms,
                    'avg_elapsed_time_ms': avg_time_ms,
                    'min_elapsed_time_ms': min_time_ms,
                    'max_elapsed_time_ms': max_time_ms,
                    'stddev_elapsed_time_ms': stddev_time_ms,
                    # CPU (PG nao separa, usar elapsed como aproximacao)
                    'cpu_time_ms': avg_time_ms,
                    'total_cpu_time_ms': total_time_ms,
                    'avg_cpu_time_ms': avg_time_ms,
                    'duration_ms': avg_time_ms,
                    # Rows
                    'total_rows': total_rows,
                    'avg_rows': avg_rows,
                    'row_count': avg_rows,
                    # Buffer I/O
                    'shared_blks_hit': shared_blks_hit,
                    'shared_blks_read': shared_blks_read,
                    'total_logical_reads': total_logical,
                    'avg_logical_reads': total_logical // calls if calls > 0 else 0,
                    'total_physical_reads': shared_blks_read,
                    'logical_reads': total_logical // calls if calls > 0 else 0,
                    'physical_reads': shared_blks_read // calls if calls > 0 else 0,
                    'hit_ratio_pct': round(hit_ratio, 2),
                    # Writes
                    'shared_blks_dirtied': shared_blks_dirtied,
                    'shared_blks_written': shared_blks_written,
                    'writes': shared_blks_written // calls if calls > 0 else 0,
                    # Local buffers (temp tables)
                    'local_blks_hit': local_blks_hit,
                    'local_blks_read': local_blks_read,
                    # Temp (spill to disk - indica work_mem insuficiente)
                    'temp_blks_read': temp_blks_read,
                    'temp_blks_written': temp_blks_written,
                    'total_temp_blks': total_temp_blks,
                    'has_temp_spill': total_temp_blks > 0,
                    # IO timing (requer track_io_timing = on)
                    'blk_read_time_ms': blk_read_time_ms,
                    'blk_write_time_ms': blk_write_time_ms,
                    'total_io_time_ms': blk_read_time_ms + blk_write_time_ms,
                    'wait_time_ms': blk_read_time_ms + blk_write_time_ms,
                    # WAL (importante para queries de escrita)
                    'wal_bytes': wal_bytes,
                    'wal_mb': wal_bytes / (1024 * 1024) if wal_bytes else 0.0,
                    'wal_records': wal_records,
                    # Memory (PG nao expoe diretamente, estimativa baseada em temp)
                    'memory_mb': (total_temp_blks * 8) / 1024 if total_temp_blks else 0.0,
                    # Metadata
                    'query_text': query_text,
                    'query_hash': str(query_id) if query_id else '',
                    'database_name': database_name,
                    'object_name': self._extract_table_from_query(query_text),
                    'query_plan': None,
                    'creation_time': None,
                    'last_execution_time': None
                }
                queries.append(query_info)
            except Exception as e:
                print(f"[ERROR] Erro ao processar linha pg_stat_statements: {e}")
                continue

        return queries

    def _extract_table_from_query(self, query_text: str) -> str:
        """Extrai nome da tabela principal de uma query."""
        import re
        if not query_text:
            return 'unknown'

        # Patterns comuns
        patterns = [
            r'FROM\s+(["\w]+\.)?(["\w]+)',
            r'INTO\s+(["\w]+\.)?(["\w]+)',
            r'UPDATE\s+(["\w]+\.)?(["\w]+)',
            r'DELETE\s+FROM\s+(["\w]+\.)?(["\w]+)',
        ]

        query_upper = query_text.upper()
        for pattern in patterns:
            match = re.search(pattern, query_upper, re.IGNORECASE)
            if match:
                table = match.group(2) if match.group(2) else match.group(1)
                return table.strip('"') if table else 'unknown'

        return 'unknown'

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

    def get_blocking_sessions(self) -> List[Dict]:
        """
        Detecta sessoes bloqueadas e suas sessoes bloqueadoras via pg_locks + pg_stat_activity.

        Returns:
            Lista de dicionarios com informacoes de bloqueio.
        """
        query = """
        SELECT
            bl.pid AS blocked_session_id,
            ka.pid AS blocking_session_id,
            EXTRACT(EPOCH FROM (clock_timestamp() - bl_activity.state_change))::NUMERIC(10,2) AS wait_time_seconds,
            bl_activity.query AS blocked_query,
            ka_activity.query AS blocking_query,
            bl_activity.datname AS database_name,
            COALESCE(ka_activity.client_addr::text, 'local') AS blocking_host,
            ka_activity.application_name AS blocking_program,
            ka_activity.usename AS blocking_login
        FROM pg_locks bl
        JOIN pg_stat_activity bl_activity ON bl.pid = bl_activity.pid
        JOIN pg_locks ka ON bl.locktype = ka.locktype
            AND bl.database IS NOT DISTINCT FROM ka.database
            AND bl.relation IS NOT DISTINCT FROM ka.relation
            AND bl.page IS NOT DISTINCT FROM ka.page
            AND bl.tuple IS NOT DISTINCT FROM ka.tuple
            AND bl.virtualxid IS NOT DISTINCT FROM ka.virtualxid
            AND bl.transactionid IS NOT DISTINCT FROM ka.transactionid
            AND bl.classid IS NOT DISTINCT FROM ka.classid
            AND bl.objid IS NOT DISTINCT FROM ka.objid
            AND bl.objsubid IS NOT DISTINCT FROM ka.objsubid
            AND bl.pid != ka.pid
        JOIN pg_stat_activity ka_activity ON ka.pid = ka_activity.pid
        WHERE NOT bl.granted
        ORDER BY wait_time_seconds DESC
        """
        try:
            results = self.connection.execute_query(query)
        except Exception as e:
            print(f"[PostgreSQL] Erro ao detectar bloqueios: {e}")
            return []

        if not results:
            return []

        blocks = []
        for row in results:
            try:
                blocks.append({
                    'blocked_session_id': row[0],
                    'blocking_session_id': row[1],
                    'wait_time_seconds': float(row[2]) if row[2] else 0.0,
                    'blocked_query': (row[3] or '')[:500],
                    'blocking_query': (row[4] or '')[:500],
                    'database_name': row[5] or 'N/A',
                    'blocking_host': row[6] or 'N/A',
                    'blocking_program': row[7] or 'N/A',
                    'blocking_login': row[8] or 'N/A'
                })
            except (IndexError, TypeError) as e:
                print(f"[PostgreSQL] Erro ao processar bloqueio: {e}")
                continue

        return blocks

    def get_wait_statistics(self) -> List[Dict]:
        """
        Coleta wait events agregados via pg_stat_activity (snapshot point-in-time).

        Returns:
            Lista de wait events com contagem de sessoes aguardando.
        """
        query = """
        SELECT
            wait_event_type,
            wait_event,
            COUNT(*) AS waiting_count,
            ARRAY_AGG(DISTINCT datname) AS databases
        FROM pg_stat_activity
        WHERE wait_event_type IS NOT NULL
            AND pid != pg_backend_pid()
            AND state = 'active'
        GROUP BY wait_event_type, wait_event
        ORDER BY waiting_count DESC
        """
        try:
            results = self.connection.execute_query(query)
        except Exception as e:
            print(f"[PostgreSQL] Erro ao coletar wait statistics: {e}")
            return []

        if not results:
            return []

        waits = []
        for row in results:
            try:
                databases = row[3] if row[3] else []
                if isinstance(databases, str):
                    databases = [databases]
                waits.append({
                    'wait_event_type': row[0] or 'Unknown',
                    'wait_event': row[1] or 'Unknown',
                    'waiting_count': int(row[2]),
                    'databases': databases
                })
            except (IndexError, TypeError) as e:
                print(f"[PostgreSQL] Erro ao processar wait stat: {e}")
                continue

        return waits

    def get_vacuum_stats(self) -> List[Dict]:
        """
        Coleta estatisticas de vacuum/autovacuum via pg_stat_user_tables.
        Filtra tabelas com problemas potenciais de bloat.

        Returns:
            Lista de tabelas com metricas de vacuum e alertas.
        """
        query = """
        SELECT
            schemaname,
            relname,
            n_live_tup,
            n_dead_tup,
            CASE WHEN n_live_tup + n_dead_tup > 0
                 THEN ROUND(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2)
                 ELSE 0 END AS dead_tuple_ratio,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze,
            vacuum_count,
            autovacuum_count,
            analyze_count,
            autoanalyze_count
        FROM pg_stat_user_tables
        WHERE n_dead_tup > 1000
           OR (last_autovacuum IS NULL AND n_live_tup > 10000)
        ORDER BY n_dead_tup DESC
        """
        try:
            results = self.connection.execute_query(query)
        except Exception as e:
            print(f"[PostgreSQL] Erro ao coletar vacuum stats: {e}")
            return []

        if not results:
            return []

        stats = []
        now = datetime.now()
        for row in results:
            try:
                dead_ratio = float(row[4]) if row[4] else 0.0
                last_autovacuum = row[6]
                n_live_tup = int(row[2]) if row[2] else 0
                n_dead_tup = int(row[3]) if row[3] else 0

                alerts = []
                if dead_ratio > 20:
                    alerts.append(f"dead_tuple_ratio alto: {dead_ratio}%")
                if last_autovacuum is None and n_live_tup > 10000:
                    alerts.append("autovacuum nunca executou nesta tabela")
                elif last_autovacuum and hasattr(last_autovacuum, 'timestamp'):
                    days_since = (now - last_autovacuum).days if hasattr(last_autovacuum, 'days') else 0
                    try:
                        days_since = (now - last_autovacuum).days
                    except TypeError:
                        days_since = 0
                    if days_since > 7:
                        alerts.append(f"autovacuum nao roda ha {days_since} dias")

                stats.append({
                    'schema': row[0],
                    'table': row[1],
                    'n_live_tup': n_live_tup,
                    'n_dead_tup': n_dead_tup,
                    'dead_tuple_ratio': dead_ratio,
                    'last_vacuum': str(row[5]) if row[5] else None,
                    'last_autovacuum': str(last_autovacuum) if last_autovacuum else None,
                    'last_analyze': str(row[7]) if row[7] else None,
                    'last_autoanalyze': str(row[8]) if row[8] else None,
                    'vacuum_count': int(row[9]) if row[9] else 0,
                    'autovacuum_count': int(row[10]) if row[10] else 0,
                    'analyze_count': int(row[11]) if row[11] else 0,
                    'autoanalyze_count': int(row[12]) if row[12] else 0,
                    'alerts': alerts
                })
            except (IndexError, TypeError, ValueError) as e:
                print(f"[PostgreSQL] Erro ao processar vacuum stat: {e}")
                continue

        return stats

    def get_connection_stats(self) -> Dict:
        """
        Coleta estatisticas de utilizacao de conexoes.

        Returns:
            Dicionario com contadores de conexoes e uso percentual.
        """
        query = """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE state = 'active') AS active,
            COUNT(*) FILTER (WHERE state = 'idle') AS idle,
            COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction,
            COUNT(*) FILTER (WHERE state = 'idle in transaction (aborted)') AS idle_in_transaction_aborted,
            COUNT(*) FILTER (WHERE wait_event_type IS NOT NULL AND state = 'active') AS waiting,
            (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_connections
        FROM pg_stat_activity
        WHERE backend_type = 'client backend'
        """
        try:
            results = self.connection.execute_query(query)
        except Exception as e:
            print(f"[PostgreSQL] Erro ao coletar connection stats: {e}")
            return {}

        if not results or not results[0]:
            return {}

        row = results[0]
        total = int(row[0]) if row[0] else 0
        max_conn = int(row[6]) if row[6] else 100
        usage_pct = round(100.0 * total / max_conn, 2) if max_conn > 0 else 0.0
        idle_in_tx = int(row[3]) if row[3] else 0

        alerts = []
        if usage_pct > 80:
            alerts.append(f"Uso de conexoes em {usage_pct}% (>{80}%)")
        if idle_in_tx > 10:
            alerts.append(f"{idle_in_tx} conexoes idle in transaction")

        return {
            'total': total,
            'active': int(row[1]) if row[1] else 0,
            'idle': int(row[2]) if row[2] else 0,
            'idle_in_transaction': idle_in_tx,
            'idle_in_transaction_aborted': int(row[4]) if row[4] else 0,
            'waiting': int(row[5]) if row[5] else 0,
            'max_connections': max_conn,
            'usage_percent': usage_pct,
            'alerts': alerts
        }

    def get_database_sizes(self) -> List[Dict]:
        """
        Coleta tamanhos de todas as databases.

        Returns:
            Lista de databases com tamanho em bytes e formatado.
        """
        query = """
        SELECT
            datname,
            pg_database_size(datname) AS size_bytes,
            pg_size_pretty(pg_database_size(datname)) AS size_pretty
        FROM pg_database
        WHERE datistemplate = false
        ORDER BY size_bytes DESC
        """
        try:
            results = self.connection.execute_query(query)
        except Exception as e:
            print(f"[PostgreSQL] Erro ao coletar tamanhos de databases: {e}")
            return []

        if not results:
            return []

        sizes = []
        for row in results:
            try:
                sizes.append({
                    'datname': row[0],
                    'size_bytes': int(row[1]) if row[1] else 0,
                    'size_pretty': row[2] or '0 bytes'
                })
            except (IndexError, TypeError) as e:
                print(f"[PostgreSQL] Erro ao processar tamanho de database: {e}")
                continue

        return sizes

    def get_replication_status(self) -> List[Dict]:
        """
        Coleta status de replicacao.
        - Se master: consulta pg_stat_replication
        - Se replica: consulta pg_last_xact_replay_timestamp()
        Retorna lista vazia se nao ha replicacao configurada.

        Returns:
            Lista de slots de replicacao com lag.
        """
        # Tentar como master primeiro
        query_master = """
        SELECT
            client_addr,
            state,
            sent_lsn,
            write_lsn,
            flush_lsn,
            replay_lsn,
            write_lag,
            flush_lag,
            replay_lag,
            application_name,
            sync_state
        FROM pg_stat_replication
        """
        try:
            results = self.connection.execute_query(query_master)
            if results:
                replicas = []
                for row in results:
                    try:
                        replay_lag = row[8]
                        replay_lag_seconds = 0.0
                        if replay_lag:
                            try:
                                replay_lag_seconds = replay_lag.total_seconds()
                            except AttributeError:
                                replay_lag_seconds = float(replay_lag) if replay_lag else 0.0

                        alerts = []
                        if replay_lag_seconds > 60:
                            alerts.append(f"replay_lag alto: {replay_lag_seconds:.1f}s")

                        replicas.append({
                            'client_addr': str(row[0]) if row[0] else 'N/A',
                            'state': row[1] or 'N/A',
                            'sent_lsn': str(row[2]) if row[2] else None,
                            'write_lsn': str(row[3]) if row[3] else None,
                            'flush_lsn': str(row[4]) if row[4] else None,
                            'replay_lsn': str(row[5]) if row[5] else None,
                            'write_lag': str(row[6]) if row[6] else None,
                            'flush_lag': str(row[7]) if row[7] else None,
                            'replay_lag': str(replay_lag) if replay_lag else None,
                            'replay_lag_seconds': replay_lag_seconds,
                            'application_name': row[9] or 'N/A',
                            'sync_state': row[10] or 'N/A',
                            'role': 'master',
                            'alerts': alerts
                        })
                    except (IndexError, TypeError) as e:
                        print(f"[PostgreSQL] Erro ao processar replication slot: {e}")
                        continue
                return replicas
        except Exception as e:
            print(f"[PostgreSQL] pg_stat_replication nao disponivel: {e}")

        # Tentar como replica
        query_replica = """
        SELECT
            CASE WHEN pg_is_in_recovery() THEN 'replica' ELSE 'standalone' END AS role,
            pg_last_xact_replay_timestamp() AS last_replay,
            EXTRACT(EPOCH FROM (clock_timestamp() - pg_last_xact_replay_timestamp()))::NUMERIC(10,2) AS replay_lag_seconds
        """
        try:
            results = self.connection.execute_query(query_replica)
            if results and results[0]:
                row = results[0]
                role = row[0]
                if role == 'replica':
                    lag_seconds = float(row[2]) if row[2] else 0.0
                    alerts = []
                    if lag_seconds > 60:
                        alerts.append(f"replay_lag alto: {lag_seconds:.1f}s")
                    return [{
                        'role': 'replica',
                        'last_replay_timestamp': str(row[1]) if row[1] else None,
                        'replay_lag_seconds': lag_seconds,
                        'alerts': alerts
                    }]
        except Exception as e:
            print(f"[PostgreSQL] Erro ao verificar status de replica: {e}")

        return []

    def get_index_usage_stats(self) -> List[Dict]:
        """
        Coleta estatisticas de uso de indices, detectando indices nao usados e duplicados.

        Returns:
            Lista de indices com metricas de uso.
        """
        query = """
        SELECT
            s.schemaname,
            s.relname AS table_name,
            s.indexrelname AS index_name,
            s.idx_scan,
            s.idx_tup_read,
            s.idx_tup_fetch,
            pg_relation_size(s.indexrelid) AS size_bytes,
            i.indexdef
        FROM pg_stat_user_indexes s
        JOIN pg_indexes i ON s.schemaname = i.schemaname
            AND s.relname = i.tablename
            AND s.indexrelname = i.indexname
        WHERE s.schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY s.idx_scan ASC, size_bytes DESC
        """
        try:
            results = self.connection.execute_query(query)
        except Exception as e:
            print(f"[PostgreSQL] Erro ao coletar index usage stats: {e}")
            return []

        if not results:
            return []

        indexes = []
        # Para detectar duplicados, agrupar por tabela e extrair colunas
        table_index_defs = {}

        for row in results:
            try:
                schema = row[0]
                table = row[1]
                index_name = row[2]
                idx_scan = int(row[3]) if row[3] else 0
                idx_tup_read = int(row[4]) if row[4] else 0
                idx_tup_fetch = int(row[5]) if row[5] else 0
                size_bytes = int(row[6]) if row[6] else 0
                indexdef = row[7] or ''

                is_unused = idx_scan == 0

                # Rastrear para deteccao de duplicados
                table_key = f"{schema}.{table}"
                if table_key not in table_index_defs:
                    table_index_defs[table_key] = []
                table_index_defs[table_key].append({
                    'index_name': index_name,
                    'indexdef': indexdef
                })

                indexes.append({
                    'schema': schema,
                    'table': table,
                    'index_name': index_name,
                    'idx_scan': idx_scan,
                    'idx_tup_read': idx_tup_read,
                    'idx_tup_fetch': idx_tup_fetch,
                    'size_bytes': size_bytes,
                    'size_pretty': self._format_size(size_bytes),
                    'indexdef': indexdef,
                    'is_unused': is_unused,
                    'is_duplicate': False
                })
            except (IndexError, TypeError, ValueError) as e:
                print(f"[PostgreSQL] Erro ao processar index stat: {e}")
                continue

        # Detectar indices duplicados (mesmas colunas na mesma tabela)
        import re
        for table_key, idx_list in table_index_defs.items():
            cols_map = {}
            for idx_info in idx_list:
                # Extrair colunas do indexdef: ... USING btree (col1, col2)
                match = re.search(r'\(([^)]+)\)', idx_info['indexdef'])
                if match:
                    cols = match.group(1).strip()
                    if cols in cols_map:
                        # Marcar ambos como duplicados
                        for idx in indexes:
                            if idx['index_name'] == idx_info['index_name'] or idx['index_name'] == cols_map[cols]:
                                idx['is_duplicate'] = True
                    else:
                        cols_map[cols] = idx_info['index_name']

        return indexes

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Formata tamanho em bytes para formato legivel."""
        if size_bytes < 1024:
            return f"{size_bytes} bytes"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} kB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"

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
