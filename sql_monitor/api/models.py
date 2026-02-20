"""
Schemas Pydantic para validação de requests/responses da API.
"""
from datetime import datetime
from typing import Optional, List, Dict, Any, Literal
from pydantic import BaseModel, Field


# Dashboard Models

class DashboardSummary(BaseModel):
    """Resumo de métricas do dashboard."""
    period_hours: int
    unique_queries: int
    total_occurrences: int
    analyses_performed: int
    total_alerts: int
    cache_hit_rate_percent: float
    active_instances: int


class QueryInfo(BaseModel):
    """Informações de uma query problemática."""
    query_hash: str
    instance_name: str
    database_name: Optional[str]
    table_name: Optional[str]
    query_preview: str
    avg_cpu_ms: float
    avg_duration_ms: float
    avg_logical_reads: float
    occurrences: int
    severity: Optional[str]
    has_analysis: bool


class AlertInfo(BaseModel):
    """Informações de um alerta."""
    alert_time: datetime
    instance_name: str
    alert_type: str
    severity: str
    threshold_value: float
    actual_value: float
    database_name: Optional[str]
    table_name: Optional[str]
    query_preview: Optional[str]


class InstanceStatus(BaseModel):
    """Status de uma instância monitorada."""
    instance_name: str
    db_type: str
    status: str  # online, offline, error
    last_cycle: Optional[datetime]
    queries_found: int
    cache_hits: int
    errors: int
    health_score: float  # 0-100


# Optimization Plan Models

class OptimizationItem(BaseModel):
    """Item de otimização no plano."""
    id: str
    type: str  # create_index, update_statistics, etc.
    priority: str  # low, medium, high, critical
    risk_level: str  # low, medium, high, critical
    table: str
    description: str
    estimated_improvement_percent: float
    estimated_duration_minutes: int
    sql_script: str
    rollback_script: Optional[str]
    auto_approved: bool
    approved: Optional[bool]
    approved_by: Optional[str]
    approved_at: Optional[datetime]
    vetoed: bool = False
    vetoed_by: Optional[str] = None
    vetoed_at: Optional[datetime] = None
    veto_reason: Optional[str] = None
    execution_status: str = "pending"  # pending, executing, success, failed, rolled_back


class OptimizationPlan(BaseModel):
    """Plano completo de otimização."""
    plan_id: str
    generated_at: datetime
    execution_scheduled_at: datetime
    analysis_period_days: int
    status: str  # pending, approved, vetoed, executing, completed
    veto_window_expires_at: datetime
    total_optimizations: int
    auto_approved_count: int
    requires_review_count: int
    blocked_count: int
    optimizations: List[OptimizationItem]


# API Request/Response Models

class VetoRequest(BaseModel):
    """Request para vetar plano ou item."""
    reason: str = Field(..., min_length=10, description="Motivo do veto (mínimo 10 caracteres)")
    vetoed_by: Optional[str] = Field(None, description="Email/nome de quem vetou")


class ApproveRequest(BaseModel):
    """Request para aprovar plano."""
    approved_by: Optional[str] = Field(None, description="Email/nome de quem aprovou")
    execute_now: bool = Field(False, description="Executar imediatamente")


class PlanStatusResponse(BaseModel):
    """Response com status do plano."""
    plan_id: str
    status: str
    veto_window_active: bool
    veto_window_expires_at: datetime
    hours_until_execution: float
    total_optimizations: int
    vetoed_count: int
    approved_count: int
    pending_count: int


# Trends/Charts Models

class TrendDataPoint(BaseModel):
    """Ponto de dado para gráficos de tendência."""
    time_bucket: str  # Data/hora formatada
    unique_queries: int
    total_queries: int
    avg_cpu_ms: float
    avg_duration_ms: float
    max_cpu_ms: float
    max_duration_ms: float


class ChartData(BaseModel):
    """Dados formatados para Chart.js."""
    labels: List[str]
    datasets: List[Dict[str, Any]]


# Tipos válidos para prompts
ValidDbType = Literal["sqlserver", "hana", "postgresql"]
ValidPromptType = Literal["base_template", "task_instructions", "features", "index_syntax"]


class SavePromptRequest(BaseModel):
    """Request para salvar/atualizar um prompt LLM."""
    name: str = Field(..., min_length=1, max_length=200, description="Nome do prompt")
    content: str = Field(..., min_length=1, max_length=100_000, description="Conteudo do prompt (max 100KB)")
    change_reason: Optional[str] = Field(None, max_length=500, description="Motivo da mudanca")
    updated_by: str = Field("api_user", max_length=100, description="Usuario que fez a alteracao")


class RollbackPromptRequest(BaseModel):
    """Request para rollback de versao de prompt."""
    restored_by: str = Field("api_user", max_length=100, description="Usuario que fez o rollback")
    change_reason: Optional[str] = Field(None, max_length=500, description="Motivo do rollback")


# Settings Request Models

class ThresholdsRequest(BaseModel):
    """Thresholds de performance por tipo de banco."""
    execution_time_ms: Optional[int] = Field(None, ge=-1)
    cpu_time_ms: Optional[int] = Field(None, ge=-1)
    logical_reads: Optional[int] = Field(None, ge=-1)
    physical_reads: Optional[int] = Field(None, ge=-1)
    writes: Optional[int] = Field(None, ge=-1)
    wait_time_ms: Optional[int] = Field(None, ge=-1)
    memory_mb: Optional[int] = Field(None, ge=-1)
    row_count: Optional[int] = Field(None, ge=-1)


class CollectionSettingsRequest(BaseModel):
    """Settings de coleta por tipo de banco."""
    min_duration_seconds: Optional[int] = Field(None, ge=0)
    collect_active_queries: Optional[bool] = None
    collect_expensive_queries: Optional[bool] = None
    collect_table_scans: Optional[bool] = None
    max_queries_per_cycle: Optional[int] = Field(None, ge=1, le=10000)


class CacheConfigRequest(BaseModel):
    """Configuracao de cache de metadata."""
    enabled: Optional[bool] = None
    ttl_hours: Optional[int] = Field(None, ge=1, le=8760)
    max_entries: Optional[int] = Field(None, ge=1, le=100000)
    cache_ddl: Optional[bool] = None
    cache_indexes: Optional[bool] = None


class LLMConfigRequest(BaseModel):
    """Configuracao do LLM."""
    provider: Optional[str] = Field(None, max_length=50)
    model: Optional[str] = Field(None, max_length=100)
    temperature: Optional[float] = Field(None, ge=0.0, le=2.0)
    max_tokens: Optional[int] = Field(None, ge=1, le=1000000)
    max_retries: Optional[int] = Field(None, ge=0, le=10)
    retry_delays: Optional[List[int]] = None
    max_requests_per_day: Optional[int] = Field(None, ge=1)
    max_requests_per_minute: Optional[int] = Field(None, ge=1)
    max_requests_per_cycle: Optional[int] = Field(None, ge=1)
    min_delay_between_requests: Optional[int] = Field(None, ge=0)


class MonitorConfigRequest(BaseModel):
    """Configuracao do monitor."""
    interval_seconds: Optional[int] = Field(None, ge=10, le=3600)


class TeamsConfigRequest(BaseModel):
    """Configuracao de notificacao Teams."""
    enabled: Optional[bool] = None
    webhook_url: Optional[str] = Field(None, max_length=2048)
    notify_on_cache_hit: Optional[bool] = None
    priority_filter: Optional[List[str]] = None
    timeout: Optional[int] = Field(None, ge=1, le=300)


class TimeoutsConfigRequest(BaseModel):
    """Configuracao de timeouts."""
    database_connect: Optional[int] = Field(None, ge=1, le=300)
    database_query: Optional[int] = Field(None, ge=1, le=3600)
    llm_analysis: Optional[int] = Field(None, ge=1, le=600)
    thread_shutdown: Optional[int] = Field(None, ge=1, le=600)
    circuit_breaker_recovery: Optional[int] = Field(None, ge=1, le=3600)


class SecurityConfigRequest(BaseModel):
    """Configuracao de seguranca."""
    sanitize_queries: Optional[bool] = None
    placeholder_prefix: Optional[str] = Field(None, max_length=20)
    show_example_values: Optional[bool] = None


class QueryCacheConfigRequest(BaseModel):
    """Configuracao de query cache."""
    enabled: Optional[bool] = None
    ttl_hours: Optional[int] = Field(None, ge=1, le=8760)
    cache_file: Optional[str] = Field(None, max_length=500)
    auto_save_interval: Optional[int] = Field(None, ge=30, le=86400)


class LoggingConfigRequest(BaseModel):
    """Configuracao de logging."""
    level: Optional[str] = Field(None, pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    format: Optional[str] = Field(None, pattern="^(colored|json)$")
    log_file: Optional[str] = Field(None, max_length=500)
    enable_console: Optional[bool] = None


class MetricsStoreConfigRequest(BaseModel):
    """Configuracao do metrics store."""
    db_path: Optional[str] = Field(None, max_length=500)
    enable_compression: Optional[bool] = None
    retention_days: Optional[int] = Field(None, ge=1, le=3650)
