"""
Sistema de persistência de métricas e observabilidade usando DuckDB.

Substitui o sistema antigo de cache JSON e logs TXT por um banco de dados
analítico embarcado que permite:
- Histórico completo de queries e métricas
- Análises temporais e tendências
- Queries analíticas rápidas (OLAP)
- Export para dashboards (Prometheus/Grafana)
- Thread-safe e performático
"""
import os
import duckdb
import threading
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path


class MetricsStore:
    """
    Armazena métricas de performance e histórico de queries usando DuckDB.

    Tabelas principais:
    - queries_collected: Histórico de queries detectadas
    - query_metrics: Métricas de performance (CPU, duration, reads, etc.)
    - llm_analyses: Resultados de análises LLM
    - monitoring_cycles: Estatísticas de execuções
    - performance_alerts: Alertas de threshold violados
    - table_metadata: Metadados de tabelas (DDL, índices)

    Features:
    - Thread-safe com connection pooling
    - Queries analíticas otimizadas
    - Compressão automática de dados antigos
    - Export para formatos populares (CSV, Parquet, JSON)
    """

    # Schema SQL para criação das tabelas
    SCHEMA_SQL = """
    -- Histórico de queries coletadas
    CREATE TABLE IF NOT EXISTS queries_collected (
        id INTEGER PRIMARY KEY,
        query_hash VARCHAR(64) NOT NULL,
        collected_at TIMESTAMP NOT NULL,
        instance_name VARCHAR(100) NOT NULL,
        db_type VARCHAR(20) NOT NULL,
        database_name VARCHAR(100),
        schema_name VARCHAR(100),
        table_name VARCHAR(100),
        query_text TEXT,
        sanitized_query TEXT,
        query_preview VARCHAR(200),
        query_type VARCHAR(50),  -- active, expensive, table_scan
        INDEX idx_query_hash (query_hash),
        INDEX idx_collected_at (collected_at),
        INDEX idx_instance (instance_name)
    );

    -- Métricas de performance das queries
    CREATE TABLE IF NOT EXISTS query_metrics (
        id INTEGER PRIMARY KEY,
        query_hash VARCHAR(64) NOT NULL,
        collected_at TIMESTAMP NOT NULL,
        instance_name VARCHAR(100) NOT NULL,

        -- Métricas de execução
        cpu_time_ms DOUBLE,
        duration_ms DOUBLE,
        logical_reads BIGINT,
        physical_reads BIGINT,
        writes BIGINT,
        row_count BIGINT,

        -- Recursos
        memory_mb DOUBLE,
        wait_time_ms DOUBLE,
        blocking_session_id INTEGER,

        -- Status
        status VARCHAR(50),
        wait_type VARCHAR(100),
        execution_count INTEGER,

        INDEX idx_query_hash_metrics (query_hash),
        INDEX idx_collected_at_metrics (collected_at),
        INDEX idx_cpu_time (cpu_time_ms),
        INDEX idx_duration (duration_ms)
    );

    -- Análises LLM realizadas
    CREATE TABLE IF NOT EXISTS llm_analyses (
        id INTEGER PRIMARY KEY,
        query_hash VARCHAR(64) NOT NULL UNIQUE,
        analyzed_at TIMESTAMP NOT NULL,
        instance_name VARCHAR(100) NOT NULL,

        -- Contexto da análise
        database_name VARCHAR(100),
        schema_name VARCHAR(100),
        table_name VARCHAR(100),

        -- Resultado da análise
        analysis_text TEXT,
        recommendations TEXT,
        severity VARCHAR(20),  -- low, medium, high, critical
        estimated_impact VARCHAR(50),

        -- Metadados LLM
        model_used VARCHAR(50),
        tokens_used INTEGER,
        analysis_duration_ms DOUBLE,

        -- TTL e cache
        expires_at TIMESTAMP,
        last_seen TIMESTAMP,
        seen_count INTEGER DEFAULT 1,

        INDEX idx_query_hash_llm (query_hash),
        INDEX idx_analyzed_at (analyzed_at),
        INDEX idx_expires_at (expires_at),
        INDEX idx_severity (severity)
    );

    -- Ciclos de monitoramento
    CREATE TABLE IF NOT EXISTS monitoring_cycles (
        id INTEGER PRIMARY KEY,
        cycle_started_at TIMESTAMP NOT NULL,
        cycle_ended_at TIMESTAMP,
        instance_name VARCHAR(100) NOT NULL,
        db_type VARCHAR(20) NOT NULL,

        -- Estatísticas do ciclo
        queries_found INTEGER DEFAULT 0,
        queries_analyzed INTEGER DEFAULT 0,
        cache_hits INTEGER DEFAULT 0,
        errors INTEGER DEFAULT 0,

        -- Performance
        cycle_duration_ms DOUBLE,
        collection_duration_ms DOUBLE,
        analysis_duration_ms DOUBLE,

        -- Status
        status VARCHAR(20),  -- running, completed, failed
        error_message TEXT,

        INDEX idx_cycle_started (cycle_started_at),
        INDEX idx_instance_cycles (instance_name)
    );

    -- Alertas de performance (threshold violations)
    CREATE TABLE IF NOT EXISTS performance_alerts (
        id INTEGER PRIMARY KEY,
        alert_time TIMESTAMP NOT NULL,
        instance_name VARCHAR(100) NOT NULL,
        query_hash VARCHAR(64) NOT NULL,

        -- Tipo de alerta
        alert_type VARCHAR(50),  -- cpu_threshold, duration_threshold, etc.
        severity VARCHAR(20),

        -- Valores
        threshold_value DOUBLE,
        actual_value DOUBLE,

        -- Contexto
        database_name VARCHAR(100),
        table_name VARCHAR(100),
        query_preview VARCHAR(200),

        -- Notificação
        teams_notified BOOLEAN DEFAULT FALSE,

        INDEX idx_alert_time (alert_time),
        INDEX idx_instance_alerts (instance_name),
        INDEX idx_severity_alerts (severity)
    );

    -- Metadados de tabelas
    CREATE TABLE IF NOT EXISTS table_metadata (
        id INTEGER PRIMARY KEY,
        captured_at TIMESTAMP NOT NULL,
        instance_name VARCHAR(100) NOT NULL,
        database_name VARCHAR(100),
        schema_name VARCHAR(100),
        table_name VARCHAR(100) NOT NULL,

        -- DDL e estrutura
        column_count INTEGER,
        columns_json TEXT,  -- JSON com definição de colunas

        -- Índices
        indexes_json TEXT,  -- JSON com definição de índices
        missing_indexes_json TEXT,  -- Sugestões de índices

        -- Estatísticas
        row_count BIGINT,
        total_size_mb DOUBLE,
        index_size_mb DOUBLE,
        data_size_mb DOUBLE,

        INDEX idx_captured_at_meta (captured_at),
        INDEX idx_table_name_meta (table_name)
    );

    -- Execuções de otimizações (Weekly Optimizer)
    CREATE TABLE IF NOT EXISTS optimization_executions (
        id INTEGER PRIMARY KEY,
        executed_at TIMESTAMP NOT NULL,
        plan_id VARCHAR(50) NOT NULL,
        optimization_id VARCHAR(50) NOT NULL,
        instance_name VARCHAR(100),

        -- Execução
        status VARCHAR(20),  -- success, failed, rolled_back, error
        duration_seconds DOUBLE,
        error_message TEXT,

        -- Métricas antes e depois
        metrics_before_json TEXT,
        metrics_after_json TEXT,

        -- Impacto
        improvement_percent DOUBLE,
        degradation_percent DOUBLE,

        -- Rollback
        rolled_back BOOLEAN DEFAULT FALSE,
        rollback_reason TEXT,

        -- Auditoria
        executed_by VARCHAR(100),
        approved_by VARCHAR(100),

        INDEX idx_plan_id_exec (plan_id),
        INDEX idx_executed_at_exec (executed_at),
        INDEX idx_optimization_id_exec (optimization_id)
    );
    """

    def __init__(self, db_path: str = "logs/metrics.duckdb", enable_compression: bool = True):
        """
        Inicializa o MetricsStore.

        Args:
            db_path: Caminho para o arquivo DuckDB
            enable_compression: Habilita compressão de dados antigos
        """
        self.db_path = db_path
        self.enable_compression = enable_compression

        # Thread-safe: cada thread terá sua própria conexão
        self._local = threading.local()
        self._lock = threading.RLock()

        # Garantir que diretório existe
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)

        # Inicializar schema
        self._initialize_schema()

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Retorna conexão thread-local ao DuckDB.

        Returns:
            Conexão DuckDB específica da thread atual
        """
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = duckdb.connect(self.db_path)
        return self._local.conn

    def _initialize_schema(self):
        """Cria tabelas se não existirem."""
        conn = self._get_connection()
        with self._lock:
            conn.executescript(self.SCHEMA_SQL)

    def generate_query_hash(
        self,
        sanitized_query: str,
        database: str,
        schema: str,
        table: str
    ) -> str:
        """
        Gera hash SHA256 único da query.

        Args:
            sanitized_query: Query sanitizada (sem valores literais)
            database: Nome do database
            schema: Nome do schema
            table: Nome da tabela

        Returns:
            String hexadecimal do hash SHA256
        """
        key = f"{database}.{schema}.{table}::{sanitized_query}"
        return hashlib.sha256(key.encode('utf-8')).hexdigest()

    # ========== INSERÇÃO DE DADOS ==========

    def add_collected_query(
        self,
        query_hash: str,
        instance_name: str,
        db_type: str,
        query_text: str,
        sanitized_query: str,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        query_type: str = "unknown",
        metrics: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Adiciona uma query coletada ao histórico.

        Args:
            query_hash: Hash SHA256 da query
            instance_name: Nome da instância do banco
            db_type: Tipo do banco (sqlserver, postgresql, hana)
            query_text: Query original completa
            sanitized_query: Query sanitizada
            database_name: Nome do database
            schema_name: Nome do schema
            table_name: Nome da tabela
            query_type: Tipo (active, expensive, table_scan)
            metrics: Métricas opcionais de performance

        Returns:
            ID da query inserida
        """
        conn = self._get_connection()

        with self._lock:
            # Preview da query (primeiros 200 chars)
            query_preview = query_text[:200] if query_text else ""

            # Inserir query coletada
            result = conn.execute("""
                INSERT INTO queries_collected (
                    query_hash, collected_at, instance_name, db_type,
                    database_name, schema_name, table_name,
                    query_text, sanitized_query, query_preview, query_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
            """, [
                query_hash,
                datetime.now(),
                instance_name,
                db_type,
                database_name,
                schema_name,
                table_name,
                query_text,
                sanitized_query,
                query_preview,
                query_type
            ]).fetchone()

            query_id = result[0]

            # Se há métricas, inserir na tabela de métricas
            if metrics:
                self._add_query_metrics(query_hash, instance_name, metrics)

            return query_id

    def _add_query_metrics(
        self,
        query_hash: str,
        instance_name: str,
        metrics: Dict[str, Any]
    ):
        """
        Adiciona métricas de performance de uma query.

        Args:
            query_hash: Hash da query
            instance_name: Nome da instância
            metrics: Dicionário com métricas
        """
        conn = self._get_connection()

        conn.execute("""
            INSERT INTO query_metrics (
                query_hash, collected_at, instance_name,
                cpu_time_ms, duration_ms, logical_reads, physical_reads, writes,
                row_count, memory_mb, wait_time_ms, blocking_session_id,
                status, wait_type, execution_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            query_hash,
            datetime.now(),
            instance_name,
            metrics.get('cpu_time_ms'),
            metrics.get('duration_ms'),
            metrics.get('logical_reads'),
            metrics.get('physical_reads'),
            metrics.get('writes'),
            metrics.get('row_count'),
            metrics.get('memory_mb'),
            metrics.get('wait_time_ms'),
            metrics.get('blocking_session_id'),
            metrics.get('status'),
            metrics.get('wait_type'),
            metrics.get('execution_count')
        ])

    def add_llm_analysis(
        self,
        query_hash: str,
        instance_name: str,
        database_name: str,
        schema_name: str,
        table_name: str,
        analysis_text: str,
        recommendations: str,
        severity: str = "medium",
        ttl_hours: int = 24,
        model_used: str = "gemini-1.5-flash",
        tokens_used: Optional[int] = None,
        analysis_duration_ms: Optional[float] = None
    ) -> bool:
        """
        Adiciona resultado de análise LLM.

        Args:
            query_hash: Hash da query
            instance_name: Nome da instância
            database_name: Nome do database
            schema_name: Nome do schema
            table_name: Nome da tabela
            analysis_text: Texto completo da análise
            recommendations: Recomendações geradas
            severity: Severidade (low, medium, high, critical)
            ttl_hours: TTL do cache (horas)
            model_used: Modelo LLM usado
            tokens_used: Tokens consumidos
            analysis_duration_ms: Duração da análise (ms)

        Returns:
            True se inseriu, False se falhou
        """
        conn = self._get_connection()

        with self._lock:
            try:
                now = datetime.now()
                expires_at = now + timedelta(hours=ttl_hours)

                conn.execute("""
                    INSERT INTO llm_analyses (
                        query_hash, analyzed_at, instance_name,
                        database_name, schema_name, table_name,
                        analysis_text, recommendations, severity,
                        model_used, tokens_used, analysis_duration_ms,
                        expires_at, last_seen, seen_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (query_hash) DO UPDATE SET
                        last_seen = EXCLUDED.last_seen,
                        seen_count = llm_analyses.seen_count + 1
                """, [
                    query_hash, now, instance_name,
                    database_name, schema_name, table_name,
                    analysis_text, recommendations, severity,
                    model_used, tokens_used, analysis_duration_ms,
                    expires_at, now, 1
                ])

                return True

            except Exception as e:
                print(f"⚠️  Erro ao salvar análise LLM: {e}")
                return False

    def is_query_analyzed_and_valid(self, query_hash: str) -> bool:
        """
        Verifica se query já foi analisada e análise ainda é válida (TTL).

        Args:
            query_hash: Hash da query

        Returns:
            True se análise existe e é válida
        """
        conn = self._get_connection()

        result = conn.execute("""
            SELECT COUNT(*) FROM llm_analyses
            WHERE query_hash = ? AND expires_at > ?
        """, [query_hash, datetime.now()]).fetchone()

        return result[0] > 0

    def get_llm_analysis(self, query_hash: str) -> Optional[Dict[str, Any]]:
        """
        Retorna análise LLM de uma query.

        Args:
            query_hash: Hash da query

        Returns:
            Dicionário com análise ou None
        """
        conn = self._get_connection()

        result = conn.execute("""
            SELECT
                analyzed_at, instance_name, database_name, schema_name, table_name,
                analysis_text, recommendations, severity,
                model_used, tokens_used, analysis_duration_ms,
                expires_at, last_seen, seen_count
            FROM llm_analyses
            WHERE query_hash = ? AND expires_at > ?
        """, [query_hash, datetime.now()]).fetchone()

        if not result:
            return None

        return {
            'analyzed_at': result[0],
            'instance_name': result[1],
            'database_name': result[2],
            'schema_name': result[3],
            'table_name': result[4],
            'analysis_text': result[5],
            'recommendations': result[6],
            'severity': result[7],
            'model_used': result[8],
            'tokens_used': result[9],
            'analysis_duration_ms': result[10],
            'expires_at': result[11],
            'last_seen': result[12],
            'seen_count': result[13]
        }

    def start_monitoring_cycle(
        self,
        instance_name: str,
        db_type: str
    ) -> int:
        """
        Registra início de um ciclo de monitoramento.

        Args:
            instance_name: Nome da instância
            db_type: Tipo do banco

        Returns:
            ID do ciclo criado
        """
        conn = self._get_connection()

        with self._lock:
            result = conn.execute("""
                INSERT INTO monitoring_cycles (
                    cycle_started_at, instance_name, db_type, status
                ) VALUES (?, ?, ?, 'running')
                RETURNING id
            """, [datetime.now(), instance_name, db_type]).fetchone()

            return result[0]

    def end_monitoring_cycle(
        self,
        cycle_id: int,
        stats: Dict[str, Any],
        error_message: Optional[str] = None
    ):
        """
        Atualiza estatísticas de um ciclo concluído.

        Args:
            cycle_id: ID do ciclo
            stats: Estatísticas do ciclo
            error_message: Mensagem de erro se falhou
        """
        conn = self._get_connection()

        with self._lock:
            now = datetime.now()
            status = "failed" if error_message else "completed"

            conn.execute("""
                UPDATE monitoring_cycles
                SET cycle_ended_at = ?,
                    queries_found = ?,
                    queries_analyzed = ?,
                    cache_hits = ?,
                    errors = ?,
                    cycle_duration_ms = ?,
                    status = ?,
                    error_message = ?
                WHERE id = ?
            """, [
                now,
                stats.get('queries_found', 0),
                stats.get('queries_analyzed', 0),
                stats.get('cache_hits', 0),
                stats.get('errors', 0),
                stats.get('cycle_duration_ms'),
                status,
                error_message,
                cycle_id
            ])

    def add_performance_alert(
        self,
        instance_name: str,
        query_hash: str,
        alert_type: str,
        severity: str,
        threshold_value: float,
        actual_value: float,
        database_name: Optional[str] = None,
        table_name: Optional[str] = None,
        query_preview: Optional[str] = None,
        teams_notified: bool = False
    ) -> int:
        """
        Registra um alerta de performance.

        Args:
            instance_name: Nome da instância
            query_hash: Hash da query
            alert_type: Tipo de alerta (cpu_threshold, duration_threshold, etc.)
            severity: Severidade (low, medium, high, critical)
            threshold_value: Valor do threshold configurado
            actual_value: Valor real medido
            database_name: Nome do database
            table_name: Nome da tabela
            query_preview: Preview da query
            teams_notified: Se notificação Teams foi enviada

        Returns:
            ID do alerta criado
        """
        conn = self._get_connection()

        with self._lock:
            result = conn.execute("""
                INSERT INTO performance_alerts (
                    alert_time, instance_name, query_hash,
                    alert_type, severity,
                    threshold_value, actual_value,
                    database_name, table_name, query_preview,
                    teams_notified
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
            """, [
                datetime.now(), instance_name, query_hash,
                alert_type, severity,
                threshold_value, actual_value,
                database_name, table_name, query_preview,
                teams_notified
            ]).fetchone()

            return result[0]

    def add_table_metadata(
        self,
        instance_name: str,
        database_name: str,
        schema_name: str,
        table_name: str,
        columns_json: str,
        indexes_json: Optional[str] = None,
        missing_indexes_json: Optional[str] = None,
        row_count: Optional[int] = None,
        total_size_mb: Optional[float] = None
    ) -> int:
        """
        Adiciona metadados de uma tabela.

        Args:
            instance_name: Nome da instância
            database_name: Nome do database
            schema_name: Nome do schema
            table_name: Nome da tabela
            columns_json: JSON com definição de colunas
            indexes_json: JSON com índices existentes
            missing_indexes_json: JSON com sugestões de índices
            row_count: Número de linhas
            total_size_mb: Tamanho total (MB)

        Returns:
            ID do registro criado
        """
        conn = self._get_connection()

        with self._lock:
            result = conn.execute("""
                INSERT INTO table_metadata (
                    captured_at, instance_name, database_name, schema_name, table_name,
                    columns_json, indexes_json, missing_indexes_json,
                    row_count, total_size_mb
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
            """, [
                datetime.now(), instance_name, database_name, schema_name, table_name,
                columns_json, indexes_json, missing_indexes_json,
                row_count, total_size_mb
            ]).fetchone()

            return result[0]

    # ========== QUERIES ANALÍTICAS ==========

    def get_top_cpu_queries(
        self,
        instance_name: Optional[str] = None,
        hours: int = 24,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Retorna top queries por CPU time.

        Args:
            instance_name: Filtrar por instância (None = todas)
            hours: Últimas N horas
            limit: Número de queries

        Returns:
            Lista de dicionários com queries e métricas
        """
        conn = self._get_connection()

        cutoff = datetime.now() - timedelta(hours=hours)

        sql = """
            SELECT
                qc.query_hash,
                qc.instance_name,
                qc.database_name,
                qc.table_name,
                qc.query_preview,
                AVG(qm.cpu_time_ms) as avg_cpu_time,
                MAX(qm.cpu_time_ms) as max_cpu_time,
                COUNT(*) as occurrences
            FROM queries_collected qc
            JOIN query_metrics qm ON qc.query_hash = qm.query_hash
            WHERE qc.collected_at >= ?
        """

        params = [cutoff]

        if instance_name:
            sql += " AND qc.instance_name = ?"
            params.append(instance_name)

        sql += """
            GROUP BY qc.query_hash, qc.instance_name, qc.database_name, qc.table_name, qc.query_preview
            ORDER BY avg_cpu_time DESC
            LIMIT ?
        """
        params.append(limit)

        results = conn.execute(sql, params).fetchall()

        return [
            {
                'query_hash': row[0],
                'instance_name': row[1],
                'database_name': row[2],
                'table_name': row[3],
                'query_preview': row[4],
                'avg_cpu_time_ms': row[5],
                'max_cpu_time_ms': row[6],
                'occurrences': row[7]
            }
            for row in results
        ]

    def get_monitoring_stats(
        self,
        instance_name: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, Any]:
        """
        Retorna estatísticas agregadas de monitoramento.

        Args:
            instance_name: Filtrar por instância (None = todas)
            hours: Últimas N horas

        Returns:
            Dicionário com estatísticas
        """
        conn = self._get_connection()

        cutoff = datetime.now() - timedelta(hours=hours)

        where_clause = "WHERE cycle_started_at >= ?"
        params = [cutoff]

        if instance_name:
            where_clause += " AND instance_name = ?"
            params.append(instance_name)

        result = conn.execute(f"""
            SELECT
                COUNT(*) as total_cycles,
                SUM(queries_found) as total_queries_found,
                SUM(queries_analyzed) as total_queries_analyzed,
                SUM(cache_hits) as total_cache_hits,
                SUM(errors) as total_errors,
                AVG(cycle_duration_ms) as avg_cycle_duration_ms
            FROM monitoring_cycles
            {where_clause}
        """, params).fetchone()

        return {
            'total_cycles': result[0] or 0,
            'total_queries_found': result[1] or 0,
            'total_queries_analyzed': result[2] or 0,
            'total_cache_hits': result[3] or 0,
            'total_errors': result[4] or 0,
            'avg_cycle_duration_ms': result[5] or 0
        }

    def get_recent_alerts(
        self,
        instance_name: Optional[str] = None,
        severity: Optional[str] = None,
        hours: int = 24,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Retorna alertas recentes.

        Args:
            instance_name: Filtrar por instância
            severity: Filtrar por severidade
            hours: Últimas N horas
            limit: Número máximo de alertas

        Returns:
            Lista de alertas
        """
        conn = self._get_connection()

        cutoff = datetime.now() - timedelta(hours=hours)

        sql = "SELECT * FROM performance_alerts WHERE alert_time >= ?"
        params = [cutoff]

        if instance_name:
            sql += " AND instance_name = ?"
            params.append(instance_name)

        if severity:
            sql += " AND severity = ?"
            params.append(severity)

        sql += " ORDER BY alert_time DESC LIMIT ?"
        params.append(limit)

        results = conn.execute(sql, params).fetchall()

        return [dict(zip([col[0] for col in conn.description], row)) for row in results]

    def cleanup_expired_analyses(self) -> int:
        """
        Remove análises LLM expiradas.

        Returns:
            Número de análises removidas
        """
        conn = self._get_connection()

        with self._lock:
            result = conn.execute("""
                DELETE FROM llm_analyses
                WHERE expires_at < ?
            """, [datetime.now()])

            return result.fetchone()[0] if result else 0

    def vacuum_database(self):
        """Compacta o banco de dados DuckDB."""
        conn = self._get_connection()
        with self._lock:
            conn.execute("VACUUM")

    def export_to_parquet(
        self,
        table_name: str,
        output_path: str,
        hours: Optional[int] = None
    ):
        """
        Exporta uma tabela para formato Parquet.

        Args:
            table_name: Nome da tabela
            output_path: Caminho do arquivo de saída
            hours: Últimas N horas (None = todos os dados)
        """
        conn = self._get_connection()

        if hours:
            cutoff = datetime.now() - timedelta(hours=hours)
            sql = f"COPY (SELECT * FROM {table_name} WHERE collected_at >= ?) TO '{output_path}' (FORMAT PARQUET)"
            conn.execute(sql, [cutoff])
        else:
            sql = f"COPY {table_name} TO '{output_path}' (FORMAT PARQUET)"
            conn.execute(sql)

    def close(self):
        """Fecha conexão do banco (cleanup)."""
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

    def get_recent_query_stats(
        self,
        hours: int = 1,
        table_filter: Optional[str] = None,
        instance_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Busca estatísticas de queries recentes.

        Args:
            hours: Últimas N horas
            table_filter: Filtrar por nome de tabela
            instance_filter: Filtrar por instância

        Returns:
            Lista de dicionários com estatísticas
        """
        conn = self._get_connection()
        cutoff = datetime.now() - timedelta(hours=hours)

        sql = """
            SELECT
                qc.query_hash,
                qc.instance_name,
                qc.database_name,
                qc.table_name,
                qc.query_preview,
                AVG(qm.cpu_time_ms) as avg_cpu_time_ms,
                AVG(qm.duration_ms) as avg_duration_ms,
                AVG(qm.logical_reads) as avg_logical_reads,
                AVG(qm.physical_reads) as avg_physical_reads,
                COUNT(*) as occurrences
            FROM queries_collected qc
            LEFT JOIN query_metrics qm ON qc.query_hash = qm.query_hash
            WHERE qc.collected_at >= ?
        """

        params = [cutoff]

        if table_filter:
            sql += " AND qc.table_name = ?"
            params.append(table_filter)

        if instance_filter:
            sql += " AND qc.instance_name = ?"
            params.append(instance_filter)

        sql += """
            GROUP BY qc.query_hash, qc.instance_name, qc.database_name,
                     qc.table_name, qc.query_preview
            ORDER BY avg_cpu_time_ms DESC
        """

        result = conn.execute(sql, params).fetchall()

        return [
            {
                'query_hash': row[0],
                'instance_name': row[1],
                'database_name': row[2],
                'table_name': row[3],
                'query_preview': row[4],
                'avg_cpu_time_ms': row[5] or 0,
                'avg_duration_ms': row[6] or 0,
                'avg_logical_reads': row[7] or 0,
                'avg_physical_reads': row[8] or 0,
                'occurrences': row[9]
            }
            for row in result
        ]

    def save_execution_result(
        self,
        plan_id: str,
        optimization_id: str,
        status: str,
        duration_seconds: float,
        error_message: Optional[str] = None,
        metrics_before: Optional[Dict[str, Any]] = None,
        metrics_after: Optional[Dict[str, Any]] = None,
        improvement_percent: Optional[float] = None,
        degradation_percent: Optional[float] = None,
        rolled_back: bool = False,
        rollback_reason: Optional[str] = None,
        instance_name: Optional[str] = None,
        executed_by: Optional[str] = None,
        approved_by: Optional[str] = None
    ):
        """
        Salva resultado de execução de otimização.

        Args:
            plan_id: ID do plano
            optimization_id: ID da otimização
            status: Status da execução
            duration_seconds: Duração em segundos
            error_message: Mensagem de erro (se houver)
            metrics_before: Métricas antes da execução
            metrics_after: Métricas depois da execução
            improvement_percent: Percentual de melhoria
            degradation_percent: Percentual de degradação
            rolled_back: Se houve rollback
            rollback_reason: Motivo do rollback
            instance_name: Nome da instância
            executed_by: Quem executou
            approved_by: Quem aprovou
        """
        import json

        conn = self._get_connection()

        with self._lock:
            conn.execute("""
                INSERT INTO optimization_executions (
                    executed_at, plan_id, optimization_id, instance_name,
                    status, duration_seconds, error_message,
                    metrics_before_json, metrics_after_json,
                    improvement_percent, degradation_percent,
                    rolled_back, rollback_reason,
                    executed_by, approved_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                datetime.now(),
                plan_id,
                optimization_id,
                instance_name,
                status,
                duration_seconds,
                error_message,
                json.dumps(metrics_before) if metrics_before else None,
                json.dumps(metrics_after) if metrics_after else None,
                improvement_percent,
                degradation_percent,
                rolled_back,
                rollback_reason,
                executed_by,
                approved_by
            ])

    def get_execution_history(
        self,
        plan_id: Optional[str] = None,
        optimization_id: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Busca histórico de execuções.

        Args:
            plan_id: Filtrar por plano
            optimization_id: Filtrar por otimização
            limit: Limite de resultados

        Returns:
            Lista de execuções
        """
        import json

        conn = self._get_connection()

        sql = """
            SELECT
                id, executed_at, plan_id, optimization_id, instance_name,
                status, duration_seconds, error_message,
                metrics_before_json, metrics_after_json,
                improvement_percent, degradation_percent,
                rolled_back, rollback_reason,
                executed_by, approved_by
            FROM optimization_executions
            WHERE 1=1
        """

        params = []

        if plan_id:
            sql += " AND plan_id = ?"
            params.append(plan_id)

        if optimization_id:
            sql += " AND optimization_id = ?"
            params.append(optimization_id)

        sql += " ORDER BY executed_at DESC LIMIT ?"
        params.append(limit)

        result = conn.execute(sql, params).fetchall()

        return [
            {
                'id': row[0],
                'executed_at': row[1],
                'plan_id': row[2],
                'optimization_id': row[3],
                'instance_name': row[4],
                'status': row[5],
                'duration_seconds': row[6],
                'error_message': row[7],
                'metrics_before': json.loads(row[8]) if row[8] else None,
                'metrics_after': json.loads(row[9]) if row[9] else None,
                'improvement_percent': row[10],
                'degradation_percent': row[11],
                'rolled_back': row[12],
                'rollback_reason': row[13],
                'executed_by': row[14],
                'approved_by': row[15]
            }
            for row in result
        ]
