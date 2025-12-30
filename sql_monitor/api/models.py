"""
Schemas Pydantic para validação de requests/responses da API.
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
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
