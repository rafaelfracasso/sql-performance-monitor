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
import json
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

    SCHEMA_SQL = """
    -- Histórico de queries coletadas
    CREATE SEQUENCE IF NOT EXISTS seq_queries_collected;
    CREATE TABLE IF NOT EXISTS queries_collected (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_queries_collected'),
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
        
        -- Rastreabilidade (Novas colunas)
        login_name VARCHAR(100),
        host_name VARCHAR(100),
        program_name VARCHAR(200),
        client_interface_name VARCHAR(100),
        session_id INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_query_hash ON queries_collected(query_hash);
    CREATE INDEX IF NOT EXISTS idx_collected_at ON queries_collected(collected_at);
    CREATE INDEX IF NOT EXISTS idx_instance ON queries_collected(instance_name);
    CREATE INDEX IF NOT EXISTS idx_login_name ON queries_collected(login_name);
    CREATE INDEX IF NOT EXISTS idx_host_name ON queries_collected(host_name);
    CREATE INDEX IF NOT EXISTS idx_program_name ON queries_collected(program_name);

    -- Metricas de performance das queries
    CREATE SEQUENCE IF NOT EXISTS seq_query_metrics;
    CREATE TABLE IF NOT EXISTS query_metrics (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_query_metrics'),
        query_hash VARCHAR(64) NOT NULL,
        collected_at TIMESTAMP NOT NULL,
        instance_name VARCHAR(100) NOT NULL,

        -- Metricas de execucao
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

        -- Metricas avancadas (novas)
        -- Buffer I/O detalhado (PostgreSQL)
        shared_blks_hit BIGINT,
        shared_blks_read BIGINT,
        hit_ratio_pct DOUBLE,

        -- Temp/Spill (todos bancos)
        temp_blks_read BIGINT,
        temp_blks_written BIGINT,
        total_spills BIGINT,

        -- IO Timing (PostgreSQL com track_io_timing)
        io_read_time_ms DOUBLE,
        io_write_time_ms DOUBLE,

        -- Memory grants (SQL Server)
        memory_grant_mb DOUBLE,
        max_memory_grant_mb DOUBLE,

        -- WAL (PostgreSQL)
        wal_bytes BIGINT,

        -- Lock wait (HANA)
        lock_wait_ms DOUBLE
    );
    CREATE INDEX IF NOT EXISTS idx_query_hash_metrics ON query_metrics(query_hash);
    CREATE INDEX IF NOT EXISTS idx_collected_at_metrics ON query_metrics(collected_at);
    CREATE INDEX IF NOT EXISTS idx_cpu_time ON query_metrics(cpu_time_ms);
    CREATE INDEX IF NOT EXISTS idx_duration ON query_metrics(duration_ms);

    -- Análises LLM realizadas
    CREATE SEQUENCE IF NOT EXISTS seq_llm_analyses;
    CREATE TABLE IF NOT EXISTS llm_analyses (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_llm_analyses'),
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
        prompt_tokens INTEGER,
        completion_tokens INTEGER,
        estimated_cost_usd DOUBLE,
        estimated_cost_brl DOUBLE,

        -- TTL e cache
        expires_at TIMESTAMP,
        last_seen TIMESTAMP,
        seen_count INTEGER DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_query_hash_llm ON llm_analyses(query_hash);
    CREATE INDEX IF NOT EXISTS idx_analyzed_at ON llm_analyses(analyzed_at);
    CREATE INDEX IF NOT EXISTS idx_expires_at ON llm_analyses(expires_at);
    CREATE INDEX IF NOT EXISTS idx_severity ON llm_analyses(severity);

    -- Ciclos de monitoramento
    CREATE SEQUENCE IF NOT EXISTS seq_monitoring_cycles;
    CREATE TABLE IF NOT EXISTS monitoring_cycles (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_monitoring_cycles'),
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
        error_message TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_cycle_started ON monitoring_cycles(cycle_started_at);
    CREATE INDEX IF NOT EXISTS idx_instance_cycles ON monitoring_cycles(instance_name);

    -- Alertas de performance (threshold violations)
    CREATE SEQUENCE IF NOT EXISTS seq_performance_alerts;
    CREATE TABLE IF NOT EXISTS performance_alerts (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_performance_alerts'),
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

        -- Informacoes extras (JSON) - usado para blocking_info, etc.
        extra_info VARCHAR,

        -- Notificacao
        teams_notified BOOLEAN DEFAULT FALSE
    );
    CREATE INDEX IF NOT EXISTS idx_alert_time ON performance_alerts(alert_time);
    CREATE INDEX IF NOT EXISTS idx_instance_alerts ON performance_alerts(instance_name);
    CREATE INDEX IF NOT EXISTS idx_severity_alerts ON performance_alerts(severity);

    -- Metadados de tabelas
    CREATE SEQUENCE IF NOT EXISTS seq_table_metadata;
    CREATE TABLE IF NOT EXISTS table_metadata (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_table_metadata'),
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
        data_size_mb DOUBLE
    );
    CREATE INDEX IF NOT EXISTS idx_captured_at_meta ON table_metadata(captured_at);
    CREATE INDEX IF NOT EXISTS idx_table_name_meta ON table_metadata(table_name);

    -- Execuções de otimizações (Weekly Optimizer)
    CREATE SEQUENCE IF NOT EXISTS seq_optimization_executions;
    CREATE TABLE IF NOT EXISTS optimization_executions (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_optimization_executions'),
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
        approved_by VARCHAR(100)
    );
    CREATE INDEX IF NOT EXISTS idx_plan_id_exec ON optimization_executions(plan_id);
    CREATE INDEX IF NOT EXISTS idx_executed_at_exec ON optimization_executions(executed_at);
    CREATE INDEX IF NOT EXISTS idx_optimization_id_exec ON optimization_executions(optimization_id);

    -- Planos de Otimização (Substitui JSON)
    CREATE TABLE IF NOT EXISTS optimization_plans (
        plan_id VARCHAR(50) PRIMARY KEY,
        generated_at TIMESTAMP NOT NULL,
        execution_scheduled_at TIMESTAMP,
        status VARCHAR(20) NOT NULL, -- pending, approved, vetoed, executed, ready_to_execute
        analysis_period_days INTEGER,
        veto_window_expires_at TIMESTAMP,
        metadata_json TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_plan_status ON optimization_plans(status);
    CREATE INDEX IF NOT EXISTS idx_plan_generated ON optimization_plans(generated_at);

    CREATE TABLE IF NOT EXISTS optimization_items (
        id VARCHAR(50) PRIMARY KEY,
        plan_id VARCHAR(50) NOT NULL,
        type VARCHAR(50) NOT NULL,
        priority VARCHAR(20),
        risk_level VARCHAR(20),
        table_name VARCHAR(100),
        description TEXT,
        sql_script TEXT,
        rollback_script TEXT,
        estimated_improvement_percent DOUBLE,
        estimated_duration_minutes INTEGER,
        
        -- Estado do item
        status VARCHAR(20) DEFAULT 'pending', -- pending, approved, vetoed, executed
        veto_reason TEXT,
        vetoed_by VARCHAR(100),
        vetoed_at TIMESTAMP,
        approved_by VARCHAR(100),
        approved_at TIMESTAMP,
        
        metadata_json TEXT,
        FOREIGN KEY (plan_id) REFERENCES optimization_plans(plan_id)
    );
    CREATE INDEX IF NOT EXISTS idx_item_plan_id ON optimization_items(plan_id);

    -- Registros de veto (substitui vetos.json)
    CREATE TABLE IF NOT EXISTS veto_records (
        veto_id VARCHAR PRIMARY KEY,
        plan_id VARCHAR NOT NULL,
        veto_type VARCHAR(20) NOT NULL,
        vetoed_at TIMESTAMP NOT NULL,
        vetoed_by VARCHAR(100) NOT NULL,
        veto_reason TEXT,
        vetoed_items TEXT,
        veto_expires_at TIMESTAMP NOT NULL,
        active BOOLEAN DEFAULT TRUE
    );
    CREATE INDEX IF NOT EXISTS idx_veto_plan_id ON veto_records(plan_id);
    CREATE INDEX IF NOT EXISTS idx_veto_active ON veto_records(active);
    CREATE INDEX IF NOT EXISTS idx_veto_expires ON veto_records(veto_expires_at);

    -- Tabela de thresholds por tipo de banco
    CREATE TABLE IF NOT EXISTS performance_thresholds_by_dbtype (
        db_type VARCHAR(20) PRIMARY KEY,
        execution_time_ms DOUBLE NOT NULL DEFAULT 30000,
        cpu_time_ms DOUBLE NOT NULL DEFAULT 10000,
        logical_reads BIGINT NOT NULL DEFAULT 50000,
        physical_reads BIGINT NOT NULL DEFAULT 10000,
        writes BIGINT NOT NULL DEFAULT 5000,
        wait_time_ms DOUBLE NOT NULL DEFAULT 5000,
        memory_mb DOUBLE NOT NULL DEFAULT 500,
        row_count BIGINT NOT NULL DEFAULT 100000,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR(100) DEFAULT 'system'
    );

    -- Tabela de configurações de coleta
    CREATE TABLE IF NOT EXISTS collection_settings_by_dbtype (
        db_type VARCHAR(20) PRIMARY KEY,
        min_duration_seconds DOUBLE NOT NULL DEFAULT 5.0,
        collect_active_queries BOOLEAN NOT NULL DEFAULT TRUE,
        collect_expensive_queries BOOLEAN NOT NULL DEFAULT TRUE,
        collect_table_scans BOOLEAN NOT NULL DEFAULT TRUE,
        max_queries_per_cycle INTEGER NOT NULL DEFAULT 50,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR(100) DEFAULT 'system'
    );

    -- Tabela de configuração de cache
    CREATE TABLE IF NOT EXISTS metadata_cache_config (
        id INTEGER PRIMARY KEY DEFAULT 1,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        ttl_hours INTEGER NOT NULL DEFAULT 24,
        max_entries INTEGER NOT NULL DEFAULT 1000,
        cache_ddl BOOLEAN NOT NULL DEFAULT TRUE,
        cache_indexes BOOLEAN NOT NULL DEFAULT TRUE,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR(100) DEFAULT 'system'
    );

    -- Log de auditoria de configurações
    CREATE SEQUENCE IF NOT EXISTS seq_config_audit;
    CREATE TABLE IF NOT EXISTS config_audit_log (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_config_audit'),
        changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        changed_by VARCHAR(100),
        config_table VARCHAR(100) NOT NULL,
        config_key VARCHAR(100),
        old_value TEXT,
        new_value TEXT,
        change_reason TEXT
    );

    -- Configurações de LLM
    CREATE TABLE IF NOT EXISTS llm_config (
        id INTEGER PRIMARY KEY DEFAULT 1,
        provider VARCHAR(50) NOT NULL DEFAULT 'gemini',
        model VARCHAR(100) NOT NULL DEFAULT 'gemini-flash-latest',
        temperature DOUBLE NOT NULL DEFAULT 0.1,
        max_tokens INTEGER NOT NULL DEFAULT 8192,
        max_retries INTEGER NOT NULL DEFAULT 3,
        retry_delays VARCHAR(100) DEFAULT '[3, 8, 15]',
        max_requests_per_day INTEGER DEFAULT 1500,
        max_requests_per_minute INTEGER DEFAULT 60,
        max_requests_per_cycle INTEGER DEFAULT 20,
        min_delay_between_requests DOUBLE DEFAULT 2.0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR(100) DEFAULT 'system'
    );

    -- Configurações de Monitor
    CREATE TABLE IF NOT EXISTS monitor_config (
        id INTEGER PRIMARY KEY DEFAULT 1,
        interval_seconds INTEGER NOT NULL DEFAULT 60,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR(100) DEFAULT 'system'
    );

    -- Configurações de Security
    CREATE TABLE IF NOT EXISTS security_config (
        id INTEGER PRIMARY KEY DEFAULT 1,
        sanitize_queries BOOLEAN NOT NULL DEFAULT TRUE,
        placeholder_prefix VARCHAR(10) DEFAULT '@p',
        show_example_values BOOLEAN NOT NULL DEFAULT TRUE,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR(100) DEFAULT 'system'
    );

    -- Configurações de Query Cache
    CREATE TABLE IF NOT EXISTS query_cache_config (
        id INTEGER PRIMARY KEY DEFAULT 1,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        ttl_hours INTEGER NOT NULL DEFAULT 24,
        cache_file VARCHAR(200) DEFAULT 'logs/query_cache.json',
        auto_save_interval INTEGER DEFAULT 300,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR(100) DEFAULT 'system'
    );

    -- Tabela de entradas do cache de queries (substitui JSON)
    CREATE TABLE IF NOT EXISTS query_cache_entries (
        query_hash VARCHAR(64) PRIMARY KEY,
        analyzed_at TIMESTAMP NOT NULL,
        expires_at TIMESTAMP NOT NULL,
        last_seen TIMESTAMP NOT NULL,
        seen_count INTEGER NOT NULL DEFAULT 1,
        database_name VARCHAR(100),
        schema_name VARCHAR(100),
        table_name VARCHAR(100),
        log_file VARCHAR(500),
        query_preview VARCHAR(500),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Índice para limpeza eficiente de expirados
    CREATE INDEX IF NOT EXISTS idx_query_cache_expires
        ON query_cache_entries(expires_at);

    -- Índice para estatísticas
    CREATE INDEX IF NOT EXISTS idx_query_cache_database
        ON query_cache_entries(database_name, schema_name, table_name);

    -- Configurações de Teams Integration
    CREATE TABLE IF NOT EXISTS teams_config (
        id INTEGER PRIMARY KEY DEFAULT 1,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        webhook_url TEXT,
        notify_on_cache_hit BOOLEAN DEFAULT TRUE,
        priority_filter VARCHAR(200) DEFAULT '[]',
        timeout INTEGER DEFAULT 10,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR(100) DEFAULT 'system'
    );

    -- Configurações de Timeouts
    CREATE TABLE IF NOT EXISTS timeouts_config (
        id INTEGER PRIMARY KEY DEFAULT 1,
        database_connect INTEGER NOT NULL DEFAULT 10,
        database_query INTEGER NOT NULL DEFAULT 60,
        llm_analysis INTEGER NOT NULL DEFAULT 30,
        thread_shutdown INTEGER NOT NULL DEFAULT 90,
        circuit_breaker_recovery INTEGER NOT NULL DEFAULT 60,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR(100) DEFAULT 'system'
    );

    -- Configurações de Weekly Optimizer
    CREATE TABLE IF NOT EXISTS weekly_optimizer_config (
        id INTEGER PRIMARY KEY DEFAULT 1,
        enabled BOOLEAN NOT NULL DEFAULT FALSE,
        analysis_day VARCHAR(20) DEFAULT 'thursday',
        analysis_time VARCHAR(10) DEFAULT '18:00',
        execution_day VARCHAR(20) DEFAULT 'sunday',
        execution_time VARCHAR(10) DEFAULT '02:00',
        report_day VARCHAR(20) DEFAULT 'monday',
        report_time VARCHAR(10) DEFAULT '08:00',
        veto_window_hours INTEGER DEFAULT 72,
        check_before_execution BOOLEAN DEFAULT TRUE,
        table_size_gb_medium INTEGER DEFAULT 100,
        table_size_gb_high INTEGER DEFAULT 500,
        table_size_gb_critical INTEGER DEFAULT 1000,
        index_fragmentation_percent INTEGER DEFAULT 50,
        max_execution_time_minutes INTEGER DEFAULT 240,
        auto_rollback_enabled BOOLEAN DEFAULT TRUE,
        degradation_threshold_percent INTEGER DEFAULT 20,
        wait_after_execution_minutes INTEGER DEFAULT 10,
        api_enabled BOOLEAN DEFAULT TRUE,
        api_host VARCHAR(50) DEFAULT '0.0.0.0',
        api_port INTEGER DEFAULT 8080,
        cors_enabled BOOLEAN DEFAULT TRUE,
        analysis_days INTEGER DEFAULT 7,
        min_occurrences INTEGER DEFAULT 10,
        min_avg_duration_ms INTEGER DEFAULT 1000,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR(100) DEFAULT 'system'
    );

    -- Configurações de Logging
    CREATE TABLE IF NOT EXISTS logging_config (
        id INTEGER PRIMARY KEY DEFAULT 1,
        level VARCHAR(20) NOT NULL DEFAULT 'INFO',
        format VARCHAR(20) NOT NULL DEFAULT 'colored',
        log_file VARCHAR(200) DEFAULT 'logs/monitor.log',
        enable_console BOOLEAN NOT NULL DEFAULT TRUE,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR(100) DEFAULT 'system'
    );

    -- Configurações de Metrics Store
    CREATE TABLE IF NOT EXISTS metrics_store_config (
        id INTEGER PRIMARY KEY DEFAULT 1,
        db_path VARCHAR(200) NOT NULL DEFAULT 'logs/metrics.duckdb',
        enable_compression BOOLEAN NOT NULL DEFAULT TRUE,
        retention_days INTEGER NOT NULL DEFAULT 30,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR(100) DEFAULT 'system'
    );

    -- Tabela de prompts LLM
    CREATE SEQUENCE IF NOT EXISTS seq_llm_prompts;
    CREATE TABLE IF NOT EXISTS llm_prompts (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_llm_prompts'),
        db_type VARCHAR(20) NOT NULL,
        prompt_type VARCHAR(50) NOT NULL,
        name VARCHAR(100) NOT NULL,
        content TEXT NOT NULL,
        version INTEGER DEFAULT 1,
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR(100) DEFAULT 'system',
        UNIQUE(db_type, prompt_type, version)
    );
    CREATE INDEX IF NOT EXISTS idx_llm_prompts_db_type ON llm_prompts(db_type);
    CREATE INDEX IF NOT EXISTS idx_llm_prompts_active ON llm_prompts(is_active);

    -- Histórico de mudanças de prompts
    CREATE SEQUENCE IF NOT EXISTS seq_llm_prompt_history;
    CREATE TABLE IF NOT EXISTS llm_prompt_history (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_llm_prompt_history'),
        prompt_id INTEGER NOT NULL,
        old_content TEXT,
        new_content TEXT,
        changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        changed_by VARCHAR(100),
        change_reason TEXT,
        -- prompt_id referencia llm_prompts(id) - sem FK pois DuckDB nao suporta UPDATE em tabelas com FK
    );
    CREATE INDEX IF NOT EXISTS idx_prompt_history_prompt_id ON llm_prompt_history(prompt_id);
    CREATE INDEX IF NOT EXISTS idx_prompt_history_changed_at ON llm_prompt_history(changed_at);

    -- Wait Stats Snapshots (deltas calculados)
    CREATE SEQUENCE IF NOT EXISTS seq_wait_stats_snapshots;
    CREATE TABLE IF NOT EXISTS wait_stats_snapshots (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_wait_stats_snapshots'),
        collected_at TIMESTAMP NOT NULL,
        instance_name VARCHAR(100) NOT NULL,
        db_type VARCHAR(20) NOT NULL,

        -- Identificacao do wait
        wait_type VARCHAR(200) NOT NULL,
        wait_category VARCHAR(50),  -- IO, Lock, CPU, Network, Memory, Parallelism, Other

        -- Valores cumulativos (raw do servidor)
        cumulative_wait_ms DOUBLE,
        cumulative_signal_wait_ms DOUBLE,
        waiting_tasks_count BIGINT,

        -- Deltas calculados vs snapshot anterior
        delta_wait_ms DOUBLE,
        delta_signal_wait_ms DOUBLE,
        delta_tasks_count BIGINT,
        delta_avg_wait_ms DOUBLE
    );
    CREATE INDEX IF NOT EXISTS idx_wait_stats_collected ON wait_stats_snapshots(collected_at);
    CREATE INDEX IF NOT EXISTS idx_wait_stats_instance ON wait_stats_snapshots(instance_name);
    CREATE INDEX IF NOT EXISTS idx_wait_stats_category ON wait_stats_snapshots(wait_category);
    CREATE INDEX IF NOT EXISTS idx_wait_stats_type ON wait_stats_snapshots(wait_type);
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
        self._connections: list = []
        self._conn_list_lock = threading.Lock()

        # Garantir que diretório existe
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)

        # Inicializar schema
        self._initialize_schema()

    def save_optimization_plan(self, plan_data: Dict[str, Any]) -> bool:
        """
        Salva um plano de otimização completo no banco.
        
        Args:
            plan_data: Dicionário contendo dados do plano e lista 'optimizations'
            
        Returns:
            True se sucesso
        """
        conn = self._get_connection()
        
        with self._lock:
            try:
                # Iniciar transação implícita
                conn.execute("BEGIN TRANSACTION")

                # Upsert do Plano
                conn.execute("""
                    INSERT INTO optimization_plans (
                        plan_id, generated_at, execution_scheduled_at, status,
                        analysis_period_days, veto_window_expires_at, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (plan_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        execution_scheduled_at = EXCLUDED.execution_scheduled_at,
                        veto_window_expires_at = EXCLUDED.veto_window_expires_at,
                        metadata_json = EXCLUDED.metadata_json
                """, [
                    plan_data['plan_id'],
                    plan_data.get('generated_at') or datetime.now(),
                    plan_data.get('execution_scheduled_at'),
                    plan_data.get('status', 'pending'),
                    plan_data.get('analysis_period_days', 7),
                    plan_data.get('veto_window_expires_at'),
                    json.dumps(plan_data.get('metadata', {}), ensure_ascii=False)
                ])
                
                # Inserir ou Atualizar Itens
                for item in plan_data.get('optimizations', []):
                    conn.execute("""
                        INSERT INTO optimization_items (
                            id, plan_id, type, priority, risk_level, table_name,
                            description, sql_script, rollback_script,
                            estimated_improvement_percent, estimated_duration_minutes,
                            status, veto_reason, vetoed_by, vetoed_at,
                            approved_by, approved_at, metadata_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (id) DO UPDATE SET
                            status = EXCLUDED.status,
                            veto_reason = EXCLUDED.veto_reason,
                            vetoed_by = EXCLUDED.vetoed_by,
                            vetoed_at = EXCLUDED.vetoed_at,
                            approved_by = EXCLUDED.approved_by,
                            approved_at = EXCLUDED.approved_at,
                            metadata_json = EXCLUDED.metadata_json
                    """, [
                        item['id'],
                        plan_data['plan_id'],
                        item['type'],
                        item.get('priority', 'medium'),
                        item.get('risk_level', 'medium'),
                        item.get('table', 'unknown'),
                        item['description'],
                        item.get('sql_script'),
                        item.get('rollback_script'),
                        item.get('estimated_improvement_percent', 0),
                        item.get('estimated_duration_minutes', 0),
                        item.get('status', 'pending'),
                        item.get('veto_reason'),
                        item.get('vetoed_by'),
                        item.get('vetoed_at'),
                        item.get('approved_by'),
                        item.get('approved_at'),
                        json.dumps(item.get('metadata', {}), ensure_ascii=False)
                    ])
                
                conn.execute("COMMIT")
                return True

            except Exception as e:
                conn.execute("ROLLBACK")
                print(f"Erro ao salvar plano: {e}")
                return False

    def get_optimization_plan(self, plan_id: str) -> Optional[Dict[str, Any]]:
        """
        Carrega um plano completo do banco.
        """
        conn = self._get_connection()
        
        # Carregar Header
        plan_row = conn.execute("""
            SELECT plan_id, generated_at, execution_scheduled_at, status,
                   analysis_period_days, veto_window_expires_at, metadata_json
            FROM optimization_plans
            WHERE plan_id = ?
        """, [plan_id]).fetchone()
        
        if not plan_row:
            return None
            
        # Carregar Itens
        items_rows = conn.execute("""
            SELECT id, type, priority, risk_level, table_name, description,
                   sql_script, rollback_script, estimated_improvement_percent,
                   estimated_duration_minutes, status, veto_reason, vetoed_by,
                   vetoed_at, approved_by, approved_at, metadata_json
            FROM optimization_items
            WHERE plan_id = ?
        """, [plan_id]).fetchall()
        
        items = []
        for row in items_rows:
            items.append({
                'id': row[0],
                'type': row[1],
                'priority': row[2],
                'risk_level': row[3],
                'table': row[4],
                'description': row[5],
                'sql_script': row[6],
                'rollback_script': row[7],
                'estimated_improvement_percent': row[8],
                'estimated_duration_minutes': row[9],
                'status': row[10],
                'veto_reason': row[11],
                'vetoed_by': row[12],
                'vetoed_at': row[13],
                'approved_by': row[14],
                'approved_at': row[15],
                'metadata': json.loads(row[16]) if row[16] else {},
                'vetoed': row[10] == 'vetoed',
                'approved': row[10] == 'approved',
                'auto_approved': False
            })
            
        return {
            'plan_id': plan_row[0],
            'generated_at': plan_row[1],
            'execution_scheduled_at': plan_row[2],
            'status': plan_row[3],
            'analysis_period_days': plan_row[4],
            'veto_window_expires_at': plan_row[5],
            'metadata': json.loads(plan_row[6]) if plan_row[6] else {},
            'optimizations': items,
            'total_optimizations': len(items),
            'auto_approved_count': sum(1 for i in items if i.get('auto_approved')),
            'blocked_count': sum(1 for i in items if i['status'] == 'vetoed'),
            'requires_review_count': sum(1 for i in items if i['status'] == 'pending')
        }

    def delete_optimization_plan(self, plan_id: str) -> bool:
        """
        Remove um plano e todos os registros relacionados do banco.
        DuckDB nao suporta ON DELETE CASCADE, entao deletamos dependencias primeiro.

        Args:
            plan_id: ID do plano a remover

        Returns:
            True se sucesso
        """
        conn = self._get_connection()

        try:
            with self._lock:
                # DuckDB valida FKs contra o estado commitado, nao o pendente da transacao.
                # Cada DELETE em autocommit garante que os filhos ja estejam persistidos
                # antes de deletar o pai (optimization_plans).
                conn.execute("DELETE FROM veto_records WHERE plan_id = ?", [plan_id])
                conn.execute("DELETE FROM optimization_executions WHERE plan_id = ?", [plan_id])
                conn.execute("DELETE FROM optimization_items WHERE plan_id = ?", [plan_id])
                conn.execute("DELETE FROM optimization_plans WHERE plan_id = ?", [plan_id])
                return True

        except Exception as e:
            print(f"Erro ao deletar plano {plan_id}: {e}")
            return False

    def list_optimization_plans(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Lista planos recentes (apenas headers)."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT plan_id, generated_at, status, total_optimizations_count
            FROM optimization_plans p
            LEFT JOIN (
                SELECT plan_id as pid, COUNT(*) as total_optimizations_count 
                FROM optimization_items GROUP BY pid
            ) i ON p.plan_id = i.pid
            ORDER BY generated_at DESC
            LIMIT ?
        """, [limit]).fetchall()
        
        return [
            {
                'plan_id': r[0],
                'generated_at': r[1],
                'status': r[2],
                'total_optimizations': r[3] or 0
            }
            for r in rows
        ]

    def init_config_defaults(self):
        """Inicializa configurações padrão se não existirem."""
        conn = self._get_connection()

        with self._lock:
            # Defaults inteligentes por dbtype
            defaults_thresholds = [
                # dbtype, exec_ms, cpu_ms, logical_reads, physical_reads, writes, wait_ms, memory_mb, row_count
                ('hana', 5000, -1, -1, 1000, 500, 1000, 500, 50000),
                ('sqlserver', 30000, 10000, 50000, 10000, 5000, 5000, 500, 100000),
                ('postgresql', 10000, 5000, 30000, 5000, 3000, 3000, 300, 80000)
            ]

            # Defaults de coleta
            defaults_collection = [
                # dbtype, min_duration_s, collect_active, collect_expensive, collect_scans, max_queries
                ('hana', 0.001, False, True, True, 30),  # 1ms, sem active queries
                ('sqlserver', 5.0, True, True, True, 50),
                ('postgresql', 3.0, True, True, True, 50)
            ]

            # Inserir thresholds apenas se não existir
            for dbtype, exec_ms, cpu_ms, lr, pr, w, wt, mem, rc in defaults_thresholds:
                conn.execute("""
                    INSERT INTO performance_thresholds_by_dbtype
                    (db_type, execution_time_ms, cpu_time_ms, logical_reads,
                     physical_reads, writes, wait_time_ms, memory_mb, row_count)
                    SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM performance_thresholds_by_dbtype WHERE db_type = ?
                    )
                """, (dbtype, exec_ms, cpu_ms, lr, pr, w, wt, mem, rc, dbtype))

            # Inserir collection settings apenas se não existir
            for dbtype, min_dur, active, expensive, scans, max_q in defaults_collection:
                conn.execute("""
                    INSERT INTO collection_settings_by_dbtype
                    (db_type, min_duration_seconds, collect_active_queries,
                     collect_expensive_queries, collect_table_scans, max_queries_per_cycle)
                    SELECT ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM collection_settings_by_dbtype WHERE db_type = ?
                    )
                """, (dbtype, min_dur, active, expensive, scans, max_q, dbtype))

            # Inserir config de cache apenas se não existir
            conn.execute("""
                INSERT INTO metadata_cache_config (id, enabled, ttl_hours, max_entries, cache_ddl, cache_indexes)
                SELECT 1, true, 24, 1000, true, true
                WHERE NOT EXISTS (
                    SELECT 1 FROM metadata_cache_config WHERE id = 1
                )
            """)

            # Inserir configurações de LLM
            conn.execute("""
                INSERT INTO llm_config (id, provider, model, temperature, max_tokens, max_retries)
                SELECT 1, 'gemini', 'gemini-flash-latest', 0.1, 8192, 3
                WHERE NOT EXISTS (SELECT 1 FROM llm_config WHERE id = 1)
            """)

            # Inserir configurações de Monitor
            conn.execute("""
                INSERT INTO monitor_config (id, interval_seconds)
                SELECT 1, 60
                WHERE NOT EXISTS (SELECT 1 FROM monitor_config WHERE id = 1)
            """)

            # Inserir configurações de Security
            conn.execute("""
                INSERT INTO security_config (id, sanitize_queries, placeholder_prefix, show_example_values)
                SELECT 1, true, '@p', true
                WHERE NOT EXISTS (SELECT 1 FROM security_config WHERE id = 1)
            """)

            # Inserir configurações de Query Cache
            conn.execute("""
                INSERT INTO query_cache_config (id, enabled, ttl_hours, cache_file, auto_save_interval)
                SELECT 1, true, 24, 'logs/query_cache.json', 300
                WHERE NOT EXISTS (SELECT 1 FROM query_cache_config WHERE id = 1)
            """)

            # Inserir configurações de Teams
            conn.execute("""
                INSERT INTO teams_config (id, enabled, notify_on_cache_hit, timeout)
                SELECT 1, true, true, 10
                WHERE NOT EXISTS (SELECT 1 FROM teams_config WHERE id = 1)
            """)

            # Inserir configurações de Timeouts
            conn.execute("""
                INSERT INTO timeouts_config (id, database_connect, database_query, llm_analysis, thread_shutdown, circuit_breaker_recovery)
                SELECT 1, 10, 60, 30, 90, 60
                WHERE NOT EXISTS (SELECT 1 FROM timeouts_config WHERE id = 1)
            """)

            # Inserir configurações de Weekly Optimizer (com todos os defaults, incluindo API)
            conn.execute("""
                INSERT INTO weekly_optimizer_config (
                    id, enabled,
                    analysis_day, analysis_time, execution_day, execution_time, report_day, report_time,
                    veto_window_hours, check_before_execution,
                    table_size_gb_medium, table_size_gb_high, table_size_gb_critical,
                    index_fragmentation_percent, max_execution_time_minutes,
                    auto_rollback_enabled, degradation_threshold_percent, wait_after_execution_minutes,
                    api_enabled, api_host, api_port, cors_enabled,
                    analysis_days, min_occurrences, min_avg_duration_ms
                )
                SELECT
                    1, false,
                    'thursday', '18:00', 'sunday', '02:00', 'monday', '08:00',
                    72, true,
                    100, 500, 1000,
                    50, 240,
                    true, 20, 10,
                    true, '0.0.0.0', 8080, true,
                    7, 10, 1000
                WHERE NOT EXISTS (SELECT 1 FROM weekly_optimizer_config WHERE id = 1)
            """)

            # Inserir configurações de Logging
            conn.execute("""
                INSERT INTO logging_config (id, level, format, log_file, enable_console)
                SELECT 1, 'INFO', 'colored', 'logs/monitor.log', true
                WHERE NOT EXISTS (SELECT 1 FROM logging_config WHERE id = 1)
            """)

            # Inserir configurações de Metrics Store
            conn.execute("""
                INSERT INTO metrics_store_config (id, db_path, enable_compression, retention_days)
                SELECT 1, 'logs/metrics.duckdb', true, 30
                WHERE NOT EXISTS (SELECT 1 FROM metrics_store_config WHERE id = 1)
            """)

    def execute(self, query: str, params: tuple = None):
        """Executa query com ou sem parâmetros."""
        conn = self._get_connection()
        with self._lock:
            if params:
                return conn.execute(query, params)
            else:
                return conn.execute(query)

    def execute_query(self, query: str, params: tuple = None):
        """Executa query e retorna resultados."""
        result = self.execute(query, params)
        return result.fetchall() if result else []

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Retorna conexão thread-local ao DuckDB.

        Returns:
            Conexão DuckDB específica da thread atual
        """
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            conn = duckdb.connect(self.db_path)
            self._local.conn = conn
            with self._conn_list_lock:
                self._connections.append(conn)
        return self._local.conn

    def _initialize_schema(self):
        """Cria tabelas se não existirem."""
        conn = self._get_connection()
        with self._lock:
            # Executar statement por statement
            for statement in self.SCHEMA_SQL.split(';'):
                if statement.strip():
                    try:
                        conn.execute(statement)
                    except Exception as e:
                        print(f"Erro ao executar statement SQL: {e}")
                        print(f"Statement: {statement[:100]}...")
                        raise
        

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
        metrics: Optional[Dict[str, Any]] = None,
        login_name: Optional[str] = None,
        host_name: Optional[str] = None,
        program_name: Optional[str] = None,
        client_interface_name: Optional[str] = None,
        session_id: Optional[int] = None
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
            login_name: Usuário que executou a query
            host_name: Host de origem da conexão
            program_name: Aplicação que executou a query
            client_interface_name: Interface de conexão
            session_id: ID da sessão

        Returns:
            ID da query inserida
        """
        conn = self._get_connection()
        now = datetime.now()

        with self._lock:
            # Preview da query (primeiros 200 chars)
            query_preview = query_text[:200] if query_text else ""

            # Inserir query coletada
            result = conn.execute("""
                INSERT INTO queries_collected (
                    query_hash, collected_at, instance_name, db_type,
                    database_name, schema_name, table_name,
                    query_text, sanitized_query, query_preview, query_type,
                    login_name, host_name, program_name, client_interface_name, session_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
            """, [
                query_hash,
                now,
                instance_name,
                db_type,
                database_name,
                schema_name,
                table_name,
                query_text,
                sanitized_query,
                query_preview,
                query_type,
                login_name,
                host_name,
                program_name,
                client_interface_name,
                session_id
            ]).fetchone()

            query_id = result[0]

            # Se há métricas, inserir na tabela de métricas
            if metrics:
                self._add_query_metrics(query_hash, instance_name, metrics, collected_at=now)

            return query_id

    def _add_query_metrics(
        self,
        query_hash: str,
        instance_name: str,
        metrics: Dict[str, Any],
        collected_at: Optional[datetime] = None
    ):
        """
        Adiciona métricas de performance de uma query.

        Args:
            query_hash: Hash da query
            instance_name: Nome da instância
            metrics: Dicionário com métricas
            collected_at: Timestamp da coleta (opcional, usa now() se None)
        """
        conn = self._get_connection()
        if collected_at is None:
            collected_at = datetime.now()

        conn.execute("""
            INSERT INTO query_metrics (
                query_hash, collected_at, instance_name,
                cpu_time_ms, duration_ms, logical_reads, physical_reads, writes,
                row_count, memory_mb, wait_time_ms, blocking_session_id,
                status, wait_type, execution_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            query_hash,
            collected_at,
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
        analysis_duration_ms: Optional[float] = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        estimated_cost_usd: float = 0.0,
        estimated_cost_brl: float = 0.0
    ) -> bool:
        """
        Adiciona resultado de análise LLM.

        Args:
            # ... (args anteriores mantidos)
            estimated_cost_usd: Custo estimado em USD
            estimated_cost_brl: Custo estimado em BRL (PTAX)

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
                        expires_at, last_seen, seen_count,
                        prompt_tokens, completion_tokens, estimated_cost_usd, estimated_cost_brl
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (query_hash) DO UPDATE SET
                        last_seen = EXCLUDED.last_seen,
                        seen_count = llm_analyses.seen_count + 1,
                        analysis_text = EXCLUDED.analysis_text,
                        recommendations = EXCLUDED.recommendations,
                        severity = EXCLUDED.severity,
                        tokens_used = EXCLUDED.tokens_used,
                        prompt_tokens = EXCLUDED.prompt_tokens,
                        completion_tokens = EXCLUDED.completion_tokens,
                        estimated_cost_usd = EXCLUDED.estimated_cost_usd,
                        estimated_cost_brl = EXCLUDED.estimated_cost_brl,
                        analyzed_at = EXCLUDED.analyzed_at,
                        expires_at = EXCLUDED.expires_at
                """, [
                    query_hash, now, instance_name,
                    database_name, schema_name, table_name,
                    analysis_text, recommendations, severity,
                    model_used, tokens_used, analysis_duration_ms,
                    expires_at, now, 1,
                    prompt_tokens, completion_tokens, estimated_cost_usd, estimated_cost_brl
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
        extra_info: Optional[str] = None,
        teams_notified: bool = False
    ) -> int:
        """
        Registra um alerta de performance.

        Args:
            instance_name: Nome da instancia
            query_hash: Hash da query
            alert_type: Tipo de alerta (cpu_threshold, duration_threshold, etc.)
            severity: Severidade (low, medium, high, critical)
            threshold_value: Valor do threshold configurado
            actual_value: Valor real medido
            database_name: Nome do database
            table_name: Nome da tabela
            query_preview: Preview da query
            extra_info: Informacoes extras em JSON (ex: blocking_info)
            teams_notified: Se notificacao Teams foi enviada

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
                    extra_info, teams_notified
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
            """, [
                datetime.now(), instance_name, query_hash,
                alert_type, severity,
                threshold_value, actual_value,
                database_name, table_name, query_preview,
                extra_info, teams_notified
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

    # ========== VETO RECORDS ==========

    def save_veto(self, veto_data: Dict[str, Any]) -> bool:
        """
        Salva ou atualiza um registro de veto.

        Args:
            veto_data: Dicionario com campos do veto (veto_id, plan_id, etc.)

        Returns:
            True se sucesso
        """
        import json as _json
        conn = self._get_connection()

        vetoed_items = veto_data.get('vetoed_items', [])
        if isinstance(vetoed_items, list):
            vetoed_items = _json.dumps(vetoed_items)

        with self._lock:
            try:
                conn.execute("""
                    INSERT INTO veto_records (
                        veto_id, plan_id, veto_type, vetoed_at, vetoed_by,
                        veto_reason, vetoed_items, veto_expires_at, active
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (veto_id) DO UPDATE SET
                        veto_reason = EXCLUDED.veto_reason,
                        vetoed_items = EXCLUDED.vetoed_items,
                        veto_expires_at = EXCLUDED.veto_expires_at,
                        active = EXCLUDED.active
                """, [
                    veto_data['veto_id'],
                    veto_data['plan_id'],
                    veto_data['veto_type'],
                    veto_data.get('vetoed_at') or datetime.now(),
                    veto_data['vetoed_by'],
                    veto_data.get('veto_reason'),
                    vetoed_items,
                    veto_data['veto_expires_at'],
                    veto_data.get('active', True)
                ])
                return True
            except Exception as e:
                print(f"Erro ao salvar veto: {e}")
                return False

    def delete_veto(self, veto_id: str) -> bool:
        """Remove um registro de veto."""
        conn = self._get_connection()
        with self._lock:
            try:
                conn.execute("DELETE FROM veto_records WHERE veto_id = ?", [veto_id])
                return True
            except Exception as e:
                print(f"Erro ao deletar veto {veto_id}: {e}")
                return False

    def get_vetos_for_plan(self, plan_id: str) -> List[Dict[str, Any]]:
        """Retorna vetos ativos para um plano."""
        import json as _json
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT veto_id, plan_id, veto_type, vetoed_at, vetoed_by,
                   veto_reason, vetoed_items, veto_expires_at, active
            FROM veto_records
            WHERE plan_id = ? AND active = TRUE AND veto_expires_at > ?
        """, [plan_id, datetime.now()]).fetchall()

        return [self._veto_row_to_dict(r) for r in rows]

    def get_all_active_vetos(self) -> List[Dict[str, Any]]:
        """Retorna todos os vetos ativos e nao expirados."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT veto_id, plan_id, veto_type, vetoed_at, vetoed_by,
                   veto_reason, vetoed_items, veto_expires_at, active
            FROM veto_records
            WHERE active = TRUE AND veto_expires_at > ?
        """, [datetime.now()]).fetchall()

        return [self._veto_row_to_dict(r) for r in rows]

    def cleanup_expired_vetos(self) -> int:
        """Desativa vetos expirados. Retorna quantidade desativada."""
        conn = self._get_connection()
        with self._lock:
            result = conn.execute("""
                UPDATE veto_records SET active = FALSE
                WHERE active = TRUE AND veto_expires_at < ?
            """, [datetime.now()])
            # DuckDB retorna rowcount via changes
            try:
                return result.fetchone()[0] if result else 0
            except Exception:
                return 0

    def update_veto_items(self, veto_id: str, vetoed_items: list) -> bool:
        """Atualiza lista de itens vetados."""
        import json as _json
        conn = self._get_connection()
        with self._lock:
            try:
                conn.execute("""
                    UPDATE veto_records SET vetoed_items = ?
                    WHERE veto_id = ?
                """, [_json.dumps(vetoed_items), veto_id])
                return True
            except Exception as e:
                print(f"Erro ao atualizar veto items: {e}")
                return False

    @staticmethod
    def _veto_row_to_dict(row) -> Dict[str, Any]:
        """Converte uma row de veto_records para dicionario."""
        import json as _json
        vetoed_items = row[6]
        if isinstance(vetoed_items, str):
            try:
                vetoed_items = _json.loads(vetoed_items)
            except (ValueError, TypeError):
                vetoed_items = []
        elif vetoed_items is None:
            vetoed_items = []

        return {
            'veto_id': row[0],
            'plan_id': row[1],
            'veto_type': row[2],
            'vetoed_at': row[3],
            'vetoed_by': row[4],
            'veto_reason': row[5],
            'vetoed_items': vetoed_items,
            'veto_expires_at': row[7],
            'active': row[8]
        }

    # ========== QUERIES ANALITICAS ==========

    def get_thresholds(self, db_type: str) -> dict:
        """
        Retorna thresholds de performance do DuckDB para o db_type informado.
        Sempre le do banco para refletir alteracoes feitas via UI sem restart.
        """
        conn = self._get_connection()
        result = conn.execute("""
            SELECT execution_time_ms, cpu_time_ms, logical_reads, physical_reads,
                   writes, wait_time_ms, memory_mb, row_count
            FROM performance_thresholds_by_dbtype
            WHERE db_type = ?
        """, [db_type.lower()]).fetchone()

        if not result:
            return {}

        return {
            'execution_time_ms': result[0],
            'cpu_time_ms': result[1],
            'logical_reads': result[2],
            'physical_reads': result[3],
            'writes': result[4],
            'wait_time_ms': result[5],
            'memory_mb': result[6],
            'row_count': result[7],
        }

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
        database_name: Optional[str] = None,
        table_name: Optional[str] = None,
        hours: int = 24,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Retorna alertas recentes.

        Args:
            instance_name: Filtrar por instancia
            severity: Filtrar por severidade
            database_name: Filtrar por database
            table_name: Filtrar por tabela
            hours: Ultimas N horas
            limit: Numero maximo de alertas

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

        if database_name:
            sql += " AND database_name = ?"
            params.append(database_name)

        if table_name:
            sql += " AND table_name = ?"
            params.append(table_name)

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

    ALLOWED_EXPORT_TABLES = {
        'query_metrics', 'queries_collected', 'performance_alerts',
        'llm_analyses', 'optimization_plans', 'optimization_items',
        'wait_stats_snapshots', 'config_audit_log', 'llm_prompts',
        'monitoring_cycles',
    }

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
        if table_name not in self.ALLOWED_EXPORT_TABLES:
            raise ValueError(f"Tabela '{table_name}' nao permitida para exportacao. Permitidas: {sorted(self.ALLOWED_EXPORT_TABLES)}")

        safe_output = str(output_path).replace("'", "")

        conn = self._get_connection()

        if hours:
            cutoff = datetime.now() - timedelta(hours=hours)
            sql = f"COPY (SELECT * FROM {table_name} WHERE collected_at >= ?) TO '{safe_output}' (FORMAT PARQUET)"
            conn.execute(sql, [cutoff])
        else:
            sql = f"COPY {table_name} TO '{safe_output}' (FORMAT PARQUET)"
            conn.execute(sql)

    def close(self):
        """Fecha todas as conexoes DuckDB de todas as threads (cleanup)."""
        with self._conn_list_lock:
            for conn in self._connections:
                try:
                    conn.close()
                except Exception:
                    pass
            self._connections.clear()
        if hasattr(self._local, 'conn'):
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
                COUNT(DISTINCT qc.id) as occurrences
            FROM queries_collected qc
            LEFT JOIN query_metrics qm ON qc.query_hash = qm.query_hash AND qc.collected_at = qm.collected_at
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
            ORDER BY avg_cpu_time_ms DESC, occurrences DESC, qc.query_hash
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

    def get_duckdb_stats(self) -> Dict[str, Any]:
        """
        Retorna estatísticas internas do DuckDB.

        Returns:
            Dict com informações de armazenamento e tabelas.
        """
        conn = self._get_connection()
        
        stats = {
            'storage': {},
            'tables': []
        }

        try:
            # Tamanho do banco
            # Em versoes recentes do DuckDB, pragma_database_size retorna colunas diferentes
            # Vamos tentar usar call pragma_database_size()
            db_size = conn.execute("CALL pragma_database_size()").fetchone()
            # Retorna: database_name, database_size, block_size, total_blocks, used_blocks, free_blocks, wal_size, memory_usage, memory_limit
            # Adaptar conforme a versao instalada. 
            # Assumindo estrutura comum:
            if db_size:
                stats['storage'] = {
                    'database_size': db_size[1] if len(db_size) > 1 else 'N/A',
                    'block_size': db_size[2] if len(db_size) > 2 else 'N/A',
                    'total_blocks': db_size[3] if len(db_size) > 3 else 0,
                    'used_blocks': db_size[4] if len(db_size) > 4 else 0,
                    'free_blocks': db_size[5] if len(db_size) > 5 else 0,
                    'wal_size': db_size[6] if len(db_size) > 6 else 'N/A',
                    'memory_usage': db_size[7] if len(db_size) > 7 else 'N/A'
                }

            # Listar todas as tabelas do banco dinamicamente
            all_tables = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' ORDER BY table_name"
            ).fetchall()

            for (table,) in all_tables:
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    stats['tables'].append({
                        'name': table,
                        'row_count': count,
                    })
                except Exception:
                    pass

        except Exception as e:
            print(f"Erro ao coletar stats do DuckDB: {e}")
            
        return stats

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

    # ========== GERENCIAMENTO DE PROMPTS LLM ==========

    def get_llm_prompts(self, db_type: str) -> Dict[str, str]:
        """
        Retorna prompts ativos para um tipo de banco.

        Args:
            db_type: Tipo do banco (sqlserver, hana, postgresql)

        Returns:
            Dicionário com prompt_type: content
        """
        conn = self._get_connection()

        result = conn.execute("""
            SELECT prompt_type, content
            FROM llm_prompts
            WHERE db_type = ? AND is_active = TRUE
            ORDER BY prompt_type
        """, [db_type]).fetchall()

        return {row[0]: row[1] for row in result}

    def save_llm_prompt(
        self,
        db_type: str,
        prompt_type: str,
        name: str,
        content: str,
        updated_by: str = 'system',
        change_reason: Optional[str] = None
    ) -> bool:
        """
        Salva ou atualiza um prompt LLM.

        Args:
            db_type: Tipo do banco
            prompt_type: Tipo do prompt (base_template, task_instructions, features)
            name: Nome descritivo do prompt
            content: Conteúdo do prompt
            updated_by: Usuário que fez a mudança
            change_reason: Motivo da mudança

        Returns:
            True se sucesso
        """
        conn = self._get_connection()

        with self._lock:
            try:
                conn.execute("BEGIN TRANSACTION")

                # Buscar prompt existente
                existing = conn.execute("""
                    SELECT id, content, version
                    FROM llm_prompts
                    WHERE db_type = ? AND prompt_type = ? AND is_active = TRUE
                """, [db_type, prompt_type]).fetchone()

                if existing:
                    prompt_id, old_content, old_version = existing

                    # Desativar versão antiga
                    conn.execute("""
                        UPDATE llm_prompts
                        SET is_active = FALSE
                        WHERE id = ?
                    """, [prompt_id])

                    # Criar nova versão
                    result = conn.execute("""
                        INSERT INTO llm_prompts
                        (db_type, prompt_type, name, content, version, is_active, updated_by)
                        VALUES (?, ?, ?, ?, ?, TRUE, ?)
                        RETURNING id
                    """, [db_type, prompt_type, name, content, old_version + 1, updated_by]).fetchone()

                    new_prompt_id = result[0]

                    # Registrar no histórico
                    conn.execute("""
                        INSERT INTO llm_prompt_history
                        (prompt_id, old_content, new_content, changed_by, change_reason)
                        VALUES (?, ?, ?, ?, ?)
                    """, [new_prompt_id, old_content, content, updated_by, change_reason])

                else:
                    # Criar primeiro prompt
                    conn.execute("""
                        INSERT INTO llm_prompts
                        (db_type, prompt_type, name, content, version, is_active, updated_by)
                        VALUES (?, ?, ?, ?, 1, TRUE, ?)
                    """, [db_type, prompt_type, name, content, updated_by])

                conn.execute("COMMIT")
                return True

            except Exception as e:
                conn.execute("ROLLBACK")
                print(f"Erro ao salvar prompt: {e}")
                return False

    def get_prompt_history(self, db_type: str, prompt_type: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Retorna histórico de mudanças de um prompt.

        Args:
            db_type: Tipo do banco
            prompt_type: Tipo do prompt
            limit: Número máximo de versões

        Returns:
            Lista de versões ordenadas por data (mais recente primeiro)
        """
        conn = self._get_connection()

        result = conn.execute("""
            SELECT
                p.id,
                p.version,
                p.content,
                p.updated_at,
                p.updated_by,
                p.is_active,
                h.change_reason
            FROM llm_prompts p
            LEFT JOIN llm_prompt_history h ON p.id = h.prompt_id
            WHERE p.db_type = ? AND p.prompt_type = ?
            ORDER BY p.version DESC
            LIMIT ?
        """, [db_type, prompt_type, limit]).fetchall()

        return [
            {
                'id': row[0],
                'version': row[1],
                'content': row[2],
                'updated_at': row[3],
                'updated_by': row[4],
                'is_active': row[5],
                'change_reason': row[6]
            }
            for row in result
        ]

    def restore_prompt_version(
        self,
        prompt_id: int,
        restored_by: str = 'system',
        change_reason: Optional[str] = None
    ) -> bool:
        """
        Restaura uma versão antiga de um prompt.

        Args:
            prompt_id: ID do prompt a restaurar
            restored_by: Usuário que fez o restore
            change_reason: Motivo do restore

        Returns:
            True se sucesso
        """
        conn = self._get_connection()

        with self._lock:
            try:
                conn.execute("BEGIN TRANSACTION")

                # Buscar prompt a restaurar
                old_prompt = conn.execute("""
                    SELECT db_type, prompt_type, name, content, version
                    FROM llm_prompts
                    WHERE id = ?
                """, [prompt_id]).fetchone()

                if not old_prompt:
                    conn.execute("ROLLBACK")
                    return False

                db_type, prompt_type, name, content, old_version = old_prompt

                # Desativar versão atual
                conn.execute("""
                    UPDATE llm_prompts
                    SET is_active = FALSE
                    WHERE db_type = ? AND prompt_type = ? AND is_active = TRUE
                """, [db_type, prompt_type])

                # Buscar versão máxima
                max_version = conn.execute("""
                    SELECT MAX(version) FROM llm_prompts
                    WHERE db_type = ? AND prompt_type = ?
                """, [db_type, prompt_type]).fetchone()[0]

                # Criar nova versão com conteúdo antigo
                result = conn.execute("""
                    INSERT INTO llm_prompts
                    (db_type, prompt_type, name, content, version, is_active, updated_by)
                    VALUES (?, ?, ?, ?, ?, TRUE, ?)
                    RETURNING id
                """, [db_type, prompt_type, name, content, max_version + 1, restored_by]).fetchone()

                new_prompt_id = result[0]

                # Registrar no histórico
                reason = f"Restaurado da versão {old_version}"
                if change_reason:
                    reason += f": {change_reason}"

                conn.execute("""
                    INSERT INTO llm_prompt_history
                    (prompt_id, old_content, new_content, changed_by, change_reason)
                    VALUES (?, NULL, ?, ?, ?)
                """, [new_prompt_id, content, restored_by, reason])

                conn.execute("COMMIT")
                return True

            except Exception as e:
                conn.execute("ROLLBACK")
                print(f"Erro ao restaurar prompt: {e}")
                return False

    def restore_prompt_by_version(
        self,
        db_type: str,
        prompt_type: str,
        version: int,
        restored_by: str = 'system',
        change_reason: Optional[str] = None
    ) -> Optional[bool]:
        """
        Restaura uma versão específica de um prompt, buscando por db_type/prompt_type/version.
        Faz tudo numa única transação para evitar race conditions.

        Args:
            db_type: Tipo do banco (sqlserver, hana, postgresql)
            prompt_type: Tipo do prompt (base_template, task_instructions, features, index_syntax)
            version: Número da versão a restaurar
            restored_by: Usuário que fez o restore
            change_reason: Motivo do restore

        Returns:
            True se sucesso, False se erro, None se versão não encontrada
        """
        conn = self._get_connection()

        with self._lock:
            try:
                conn.execute("BEGIN TRANSACTION")

                # Buscar prompt da versão especificada
                old_prompt = conn.execute("""
                    SELECT id, name, content, version
                    FROM llm_prompts
                    WHERE db_type = ? AND prompt_type = ? AND version = ?
                """, [db_type, prompt_type, version]).fetchone()

                if not old_prompt:
                    conn.execute("ROLLBACK")
                    return None

                old_id, name, content, old_version = old_prompt

                # Desativar versão atual
                conn.execute("""
                    UPDATE llm_prompts
                    SET is_active = FALSE
                    WHERE db_type = ? AND prompt_type = ? AND is_active = TRUE
                """, [db_type, prompt_type])

                # Buscar versão máxima
                max_version = conn.execute("""
                    SELECT MAX(version) FROM llm_prompts
                    WHERE db_type = ? AND prompt_type = ?
                """, [db_type, prompt_type]).fetchone()[0]

                # Criar nova versão com conteúdo antigo
                result = conn.execute("""
                    INSERT INTO llm_prompts
                    (db_type, prompt_type, name, content, version, is_active, updated_by)
                    VALUES (?, ?, ?, ?, ?, TRUE, ?)
                    RETURNING id
                """, [db_type, prompt_type, name, content, max_version + 1, restored_by]).fetchone()

                new_prompt_id = result[0]

                # Registrar no histórico
                reason = f"Restaurado da versão {old_version}"
                if change_reason:
                    reason += f": {change_reason}"

                conn.execute("""
                    INSERT INTO llm_prompt_history
                    (prompt_id, old_content, new_content, changed_by, change_reason)
                    VALUES (?, NULL, ?, ?, ?)
                """, [new_prompt_id, content, restored_by, reason])

                conn.execute("COMMIT")
                return True

            except Exception as e:
                conn.execute("ROLLBACK")
                print(f"Erro ao restaurar prompt por versão: {e}")
                return False

    def load_config_from_db(self) -> Dict[str, Any]:
        """
        Carrega todas as configurações do DuckDB e retorna no formato config.json.

        Returns:
            Dict com todas as configurações
        """

        config = {}

        try:
            # Monitor config
            result = self.execute_query("SELECT interval_seconds FROM monitor_config WHERE id = 1")
            if result:
                config['monitor'] = {
                    'interval_seconds': result[0][0],
                    'comment': 'Intervalo entre verificações'
                }

            # LLM config
            result = self.execute_query("""
                SELECT provider, model, temperature, max_tokens, max_retries, retry_delays,
                       max_requests_per_day, max_requests_per_minute, max_requests_per_cycle,
                       min_delay_between_requests
                FROM llm_config WHERE id = 1
            """)
            if result:
                r = result[0]
                retry_delays = json.loads(r[5]) if r[5] else [3, 8, 15]
                config['llm'] = {
                    'provider': r[0],
                    'model': r[1],
                    'temperature': r[2],
                    'max_tokens': r[3],
                    'max_retries': r[4],
                    'retry_delays': retry_delays,
                    'rate_limit': {
                        'max_requests_per_day': r[6],
                        'max_requests_per_minute': r[7],
                        'max_requests_per_cycle': r[8],
                        'min_delay_between_requests': r[9]
                    },
                    'comment': ''
                }

            # Security config
            result = self.execute_query("""
                SELECT sanitize_queries, placeholder_prefix, show_example_values
                FROM security_config WHERE id = 1
            """)
            if result:
                r = result[0]
                config['security'] = {
                    'sanitize_queries': r[0],
                    'placeholder_prefix': r[1],
                    'show_example_values': r[2],
                    'comment': 'Segurança: valores sensíveis são substituídos por placeholders tipados'
                }

            # Query cache config
            result = self.execute_query("""
                SELECT enabled, ttl_hours, cache_file, auto_save_interval
                FROM query_cache_config WHERE id = 1
            """)
            if result:
                r = result[0]
                config['query_cache'] = {
                    'enabled': r[0],
                    'ttl_hours': r[1],
                    'cache_file': r[2],
                    'auto_save_interval': r[3],
                    'comment': 'Cache de queries analisadas: TTL deslizantes, evita análises duplicadas pelo LLM'
                }

            # Teams config
            result = self.execute_query("""
                SELECT enabled, webhook_url, notify_on_cache_hit, priority_filter, timeout
                FROM teams_config WHERE id = 1
            """)
            if result:
                r = result[0]
                priority_filter = json.loads(r[3]) if r[3] else []
                config['teams'] = {
                    'enabled': r[0],
                    'webhook_url': r[1],
                    'notify_on_cache_hit': r[2],
                    'priority_filter': priority_filter,
                    'timeout': r[4],
                    'comment': 'Integração com Microsoft Teams via Power Automate'
                }

            # Timeouts config
            result = self.execute_query("""
                SELECT database_connect, database_query, llm_analysis, thread_shutdown, circuit_breaker_recovery
                FROM timeouts_config WHERE id = 1
            """)
            if result:
                r = result[0]
                config['timeouts'] = {
                    'database_connect': r[0],
                    'database_query': r[1],
                    'llm_analysis': r[2],
                    'thread_shutdown': r[3],
                    'circuit_breaker_recovery': r[4],
                    'comment': 'Timeouts em segundos'
                }

            # Logging config
            result = self.execute_query("""
                SELECT level, format, log_file, enable_console
                FROM logging_config WHERE id = 1
            """)
            if result:
                r = result[0]
                config['logging'] = {
                    'level': r[0],
                    'format': r[1],
                    'log_file': r[2],
                    'enable_console': r[3],
                    'comment': 'Níveis: DEBUG, INFO, WARNING, ERROR, CRITICAL | Formatos: colored, json, simple'
                }

            # Metrics store config
            result = self.execute_query("""
                SELECT db_path, enable_compression, retention_days
                FROM metrics_store_config WHERE id = 1
            """)
            if result:
                r = result[0]
                config['metrics_store'] = {
                    'db_path': r[0],
                    'enable_compression': r[1],
                    'retention_days': r[2],
                    'comment': 'DuckDB para métricas e observabilidade'
                }

            # Weekly optimizer config
            result = self.execute_query("""
                SELECT enabled, analysis_day, analysis_time, execution_day, execution_time,
                       report_day, report_time, veto_window_hours, check_before_execution,
                       table_size_gb_medium, table_size_gb_high, table_size_gb_critical,
                       index_fragmentation_percent, max_execution_time_minutes,
                       auto_rollback_enabled, degradation_threshold_percent, wait_after_execution_minutes,
                       api_enabled, api_host, api_port, cors_enabled,
                       analysis_days, min_occurrences, min_avg_duration_ms
                FROM weekly_optimizer_config WHERE id = 1
            """)
            if result:
                r = result[0]
                config['weekly_optimizer'] = {
                    'enabled': r[0],
                    'schedule': {
                        'analysis_day': r[1],
                        'analysis_time': r[2],
                        'execution_day': r[3],
                        'execution_time': r[4],
                        'report_day': r[5],
                        'report_time': r[6],
                        'comment': 'Quinta 18h: gera plano | Domingo 02h: executa | Segunda 08h: relatório'
                    },
                    'veto_window': {
                        'hours': r[7],
                        'check_before_execution': r[8],
                        'comment': '72 horas de quinta a domingo para DBA vetar via web interface'
                    },
                    'risk_thresholds': {
                        'table_size_gb_medium': r[9],
                        'table_size_gb_high': r[10],
                        'table_size_gb_critical': r[11],
                        'index_fragmentation_percent': r[12],
                        'max_execution_time_minutes': r[13],
                        'comment': 'Thresholds para classificação automática de risco'
                    },
                    'auto_rollback': {
                        'enabled': r[14],
                        'degradation_threshold_percent': r[15],
                        'wait_after_execution_minutes': r[16],
                        'comment': 'Rollback automático se degradação > threshold após espera'
                    },
                    'api': {
                        'enabled': r[17],
                        'host': r[18],
                        'port': r[19],
                        'cors_enabled': r[20],
                        'comment': 'API REST e Dashboard Web para visualização e gerenciamento'
                    },
                    'analysis': {
                        'days': r[21],
                        'min_occurrences': r[22],
                        'min_avg_duration_ms': r[23],
                        'comment': 'Analisa últimos N dias, queries com min ocorrências e duração'
                    },
                    'comment': 'Sistema de otimização semanal automático com veto via web'
                }

            return config

        except Exception as e:
            print(f"Erro ao carregar configuração do DuckDB: {e}")
            return {}

    # ========== WAIT STATS ==========

    WAIT_CATEGORY_MAP = {
        # SQL Server
        'PAGEIOLATCH_SH': 'IO', 'PAGEIOLATCH_EX': 'IO', 'PAGEIOLATCH_UP': 'IO',
        'WRITELOG': 'IO', 'IO_COMPLETION': 'IO', 'ASYNC_IO_COMPLETION': 'IO',
        'ASYNC_NETWORK_IO': 'Network', 'PREEMPTIVE_OS_WRITEFILEGATHER': 'IO',
        'LCK_M_S': 'Lock', 'LCK_M_X': 'Lock', 'LCK_M_U': 'Lock',
        'LCK_M_IS': 'Lock', 'LCK_M_IX': 'Lock', 'LCK_M_SCH_S': 'Lock',
        'LCK_M_SCH_M': 'Lock', 'LCK_M_BU': 'Lock',
        'SOS_SCHEDULER_YIELD': 'CPU', 'THREADPOOL': 'CPU',
        'RESOURCE_SEMAPHORE': 'Memory', 'RESOURCE_SEMAPHORE_QUERY_COMPILE': 'Memory',
        'CXPACKET': 'Parallelism', 'CXCONSUMER': 'Parallelism', 'EXCHANGE': 'Parallelism',
        'LATCH_EX': 'Latch', 'LATCH_SH': 'Latch',
        # PostgreSQL wait_event_type
        'LWLock': 'Lock', 'Lock': 'Lock', 'BufferPin': 'Lock',
        'IO': 'IO', 'IPC': 'Network', 'Client': 'Network',
        'Activity': 'CPU', 'Extension': 'Other',
        # HANA
        'Thread': 'CPU', 'I/O': 'IO', 'Mutex': 'Lock',
        'Semaphore': 'Lock', 'Barrier': 'Parallelism',
    }

    @staticmethod
    def _categorize_wait_type(wait_type: str, db_type: str) -> str:
        """Mapeia wait type para categoria."""
        if not wait_type:
            return 'Other'

        # Tentar match exato
        category = MetricsStore.WAIT_CATEGORY_MAP.get(wait_type)
        if category:
            return category

        # Match por prefixo
        wt_upper = wait_type.upper()
        if any(wt_upper.startswith(p) for p in ('PAGEIOLATCH', 'IO_', 'ASYNC_IO', 'WRITELOG')):
            return 'IO'
        if any(wt_upper.startswith(p) for p in ('LCK_', 'LOCK')):
            return 'Lock'
        if any(wt_upper.startswith(p) for p in ('CX', 'EXCHANGE')):
            return 'Parallelism'
        if any(wt_upper.startswith(p) for p in ('LATCH_',)):
            return 'Latch'
        if any(wt_upper.startswith(p) for p in ('RESOURCE_SEMAPHORE',)):
            return 'Memory'
        if any(wt_upper.startswith(p) for p in ('ASYNC_NETWORK', 'NETWORK')):
            return 'Network'

        return 'Other'

    def add_wait_stats_snapshot(
        self,
        instance_name: str,
        db_type: str,
        wait_stats: List[Dict]
    ) -> int:
        """
        Armazena snapshot de wait stats e calcula deltas vs ultimo snapshot.

        Args:
            instance_name: Nome da instancia
            db_type: Tipo do banco (sqlserver, postgresql, hana)
            wait_stats: Lista de wait stats do collector

        Returns:
            Numero de registros inseridos
        """
        conn = self._get_connection()
        now = datetime.now()
        inserted = 0

        # Buscar ultimo snapshot desta instancia para calcular deltas
        prev_snapshot = {}
        try:
            prev_rows = conn.execute("""
                SELECT wait_type, cumulative_wait_ms, cumulative_signal_wait_ms, waiting_tasks_count
                FROM wait_stats_snapshots
                WHERE instance_name = ? AND db_type = ?
                    AND collected_at = (
                        SELECT MAX(collected_at) FROM wait_stats_snapshots
                        WHERE instance_name = ? AND db_type = ?
                    )
            """, [instance_name, db_type, instance_name, db_type]).fetchall()

            for row in prev_rows:
                prev_snapshot[row[0]] = {
                    'cumulative_wait_ms': row[1] or 0,
                    'cumulative_signal_wait_ms': row[2] or 0,
                    'waiting_tasks_count': row[3] or 0
                }
        except Exception:
            pass

        for ws in wait_stats:
            # Normalizar formato entre collectors
            # Todos os valores numericos convertidos para float para evitar
            # erro de operacao entre decimal.Decimal e float
            if db_type == 'sqlserver':
                wait_type = ws.get('wait_type', '')
                # SQL Server retorna wait_time_seconds, converter para ms
                cumulative_wait_ms = float(ws.get('wait_time_seconds', 0) or 0) * 1000
                cumulative_signal_ms = float(ws.get('signal_wait_time_seconds', 0) or 0) * 1000
                tasks_count = int(ws.get('waiting_tasks_count', 0) or 0)
            elif db_type == 'postgresql':
                wait_type = f"{ws.get('wait_event_type', '')}.{ws.get('wait_event', '')}"
                # PostgreSQL: point-in-time snapshot, usar waiting_count como proxy
                cumulative_wait_ms = float(ws.get('waiting_count', 0) or 0)
                cumulative_signal_ms = 0.0
                tasks_count = int(ws.get('waiting_count', 0) or 0)
            elif db_type == 'hana':
                wait_type = ws.get('thread_state', '') or ws.get('wait_type', '')
                cumulative_wait_ms = float(ws.get('total_wait_ms', 0) or 0)
                cumulative_signal_ms = 0.0
                tasks_count = int(ws.get('thread_count', 0) or ws.get('waiting_tasks_count', 0) or 0)
            else:
                wait_type = ws.get('wait_type', 'unknown')
                cumulative_wait_ms = float(ws.get('wait_time_ms', 0) or 0)
                cumulative_signal_ms = 0.0
                tasks_count = 0

            if not wait_type:
                continue

            category = self._categorize_wait_type(wait_type, db_type)

            # Calcular deltas
            prev = prev_snapshot.get(wait_type)
            if prev:
                delta_wait_ms = max(0, cumulative_wait_ms - prev['cumulative_wait_ms'])
                delta_signal_ms = max(0, cumulative_signal_ms - prev['cumulative_signal_wait_ms'])
                delta_tasks = max(0, tasks_count - prev['waiting_tasks_count'])
                delta_avg = delta_wait_ms / delta_tasks if delta_tasks > 0 else 0
            else:
                delta_wait_ms = None
                delta_signal_ms = None
                delta_tasks = None
                delta_avg = None

            try:
                conn.execute("""
                    INSERT INTO wait_stats_snapshots (
                        collected_at, instance_name, db_type, wait_type, wait_category,
                        cumulative_wait_ms, cumulative_signal_wait_ms, waiting_tasks_count,
                        delta_wait_ms, delta_signal_wait_ms, delta_tasks_count, delta_avg_wait_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    now, instance_name, db_type, wait_type, category,
                    cumulative_wait_ms, cumulative_signal_ms, tasks_count,
                    delta_wait_ms, delta_signal_ms, delta_tasks, delta_avg
                ])
                inserted += 1
            except Exception as e:
                print(f"Erro ao inserir wait stat {wait_type}: {e}")

        return inserted

    def get_wait_stats_delta(
        self,
        instance_name: str,
        hours: int = 24,
        limit: int = 20
    ) -> List[Dict]:
        """
        Retorna top wait types por delta acumulado no periodo.

        Args:
            instance_name: Nome da instancia
            hours: Periodo em horas
            limit: Numero maximo de resultados

        Returns:
            Lista de wait types com deltas agregados
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self._get_connection()

        results = conn.execute("""
            SELECT
                wait_type,
                wait_category,
                SUM(delta_wait_ms) as total_delta_wait_ms,
                SUM(delta_signal_wait_ms) as total_delta_signal_ms,
                SUM(delta_tasks_count) as total_delta_tasks,
                AVG(delta_avg_wait_ms) as avg_wait_per_task_ms,
                COUNT(*) as snapshot_count,
                MAX(collected_at) as last_seen
            FROM wait_stats_snapshots
            WHERE instance_name = ?
                AND collected_at >= ?
                AND delta_wait_ms IS NOT NULL
            GROUP BY wait_type, wait_category
            HAVING total_delta_wait_ms > 0
            ORDER BY total_delta_wait_ms DESC
            LIMIT ?
        """, [instance_name, cutoff, limit]).fetchall()

        return [
            {
                'wait_type': r[0],
                'wait_category': r[1],
                'total_delta_wait_ms': round(r[2] or 0, 2),
                'total_delta_signal_ms': round(r[3] or 0, 2),
                'total_delta_tasks': r[4] or 0,
                'avg_wait_per_task_ms': round(r[5] or 0, 2),
                'snapshot_count': r[6],
                'last_seen': r[7].isoformat() if r[7] else None,
            }
            for r in results
        ]

    def get_wait_stats_timeline(
        self,
        instance_name: str,
        hours: int = 24
    ) -> List[Dict]:
        """
        Retorna timeline de wait stats por categoria para graficos.

        Args:
            instance_name: Nome da instancia
            hours: Periodo em horas

        Returns:
            Lista de pontos temporais com waits por categoria
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self._get_connection()

        if hours <= 24:
            date_format = '%Y-%m-%d %H:00:00'
        elif hours <= 168:
            date_format = '%Y-%m-%d %H:00:00'
        else:
            date_format = '%Y-%m-%d'

        results = conn.execute(f"""
            SELECT
                strftime(collected_at, '{date_format}') as time_bucket,
                wait_category,
                SUM(delta_wait_ms) as total_wait_ms
            FROM wait_stats_snapshots
            WHERE instance_name = ?
                AND collected_at >= ?
                AND delta_wait_ms IS NOT NULL
            GROUP BY time_bucket, wait_category
            ORDER BY time_bucket ASC
        """, [instance_name, cutoff]).fetchall()

        # Reorganizar por time_bucket
        timeline = {}
        categories = set()
        for r in results:
            bucket = r[0]
            cat = r[1] or 'Other'
            categories.add(cat)
            if bucket not in timeline:
                timeline[bucket] = {'time': bucket}
            timeline[bucket][cat] = round(r[2] or 0, 2)

        # Preencher categorias faltantes com 0
        for bucket_data in timeline.values():
            for cat in categories:
                if cat not in bucket_data:
                    bucket_data[cat] = 0

        return {
            'timeline': list(timeline.values()),
            'categories': sorted(categories)
        }

    def cleanup_old_data(self, retention_days: int = 90) -> Dict[str, int]:
        """
        Remove dados mais antigos que retention_days.

        Args:
            retention_days: Dias de retencao

        Returns:
            Dicionario com contagem de registros removidos por tabela
        """
        conn = self._get_connection()
        cutoff = datetime.now() - timedelta(days=retention_days)
        deleted = {}

        tables = [
            ('query_metrics', 'collected_at'),
            ('queries_collected', 'collected_at'),
            ('wait_stats_snapshots', 'collected_at'),
            ('performance_alerts', 'alert_time'),
            ('monitoring_cycles', 'cycle_started_at'),
        ]

        for table_name, date_col in tables:
            try:
                result = conn.execute(
                    f"DELETE FROM {table_name} WHERE {date_col} < ? RETURNING 1",
                    [cutoff]
                ).fetchall()
                deleted[table_name] = len(result)
            except Exception as e:
                print(f"Erro ao limpar {table_name}: {e}")
                deleted[table_name] = 0

        total = sum(deleted.values())
        if total > 0:
            print(f"Cleanup: {total} registros removidos (retencao: {retention_days} dias)")

        return deleted
