"""
Rotas da API REST e páginas HTML do dashboard.
"""
from fastapi import APIRouter, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from typing import Optional
from datetime import datetime, timedelta

from .app import templates
from .models import (
    DashboardSummary, QueryInfo, AlertInfo, InstanceStatus,
    VetoRequest, ApproveRequest, PlanStatusResponse
)
from ..utils.metrics_store import MetricsStore
from ..utils.query_analytics import QueryAnalytics

# Router principal
router = APIRouter()

# Instâncias globais (serão injetadas ao iniciar a API)
metrics_store: Optional[MetricsStore] = None
analytics: Optional[QueryAnalytics] = None
plan_state_manager = None
veto_system = None


def init_dependencies(store: MetricsStore):
    """
    Inicializa dependências globais.

    Args:
        store: Instância do MetricsStore
    """
    global metrics_store, analytics, plan_state_manager, veto_system
    metrics_store = store
    analytics = QueryAnalytics(store)

    # Inicializar sistema de planos e vetos
    from ..optimization.veto_system import VetoSystem
    from ..optimization.plan_state import PlanStateManager

    veto_system = VetoSystem()
    plan_state_manager = PlanStateManager(veto_system=veto_system)


# ========== PÁGINAS HTML (Dashboard) ==========

@router.get("/", response_class=HTMLResponse)
async def dashboard_home(request: Request):
    """Dashboard principal."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    # Buscar dados para dashboard
    summary = analytics.get_executive_summary(hours=24)
    health = analytics.get_monitoring_health(hours=24)
    worst_queries = analytics.get_worst_performers(metric='cpu_time_ms', hours=24, limit=5)
    recent_alerts = analytics.get_recent_alerts(hours=24, limit=10)

    return templates.TemplateResponse("dashboard_home.html", {
        "request": request,
        "summary": summary,
        "health": health,
        "worst_queries": worst_queries,
        "recent_alerts": recent_alerts,
        "page_title": "Dashboard"
    })


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_redirect(request: Request):
    """Redirect para dashboard principal."""
    return await dashboard_home(request)


@router.get("/dashboard/queries", response_class=HTMLResponse)
async def dashboard_queries(
    request: Request,
    period: str = Query("24h", description="Período: 24h, 7d, 30d"),
    metric: str = Query("cpu_time_ms", description="Métrica: cpu_time_ms, duration_ms, logical_reads"),
    instance: Optional[str] = Query(None, description="Filtrar por instância")
):
    """Página de top queries problemáticas."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    # Converter período para horas
    period_hours = {"24h": 24, "7d": 168, "30d": 720}.get(period, 24)

    # Buscar queries
    queries = analytics.get_worst_performers(
        metric=metric,
        hours=period_hours,
        limit=50,
        instance_name=instance
    )

    return templates.TemplateResponse("dashboard_queries.html", {
        "request": request,
        "queries": queries,
        "period": period,
        "metric": metric,
        "instance_filter": instance,
        "page_title": "Top Queries"
    })


@router.get("/dashboard/alerts", response_class=HTMLResponse)
async def dashboard_alerts(
    request: Request,
    period: str = Query("24h", description="Período: 24h, 7d, 30d"),
    severity: Optional[str] = Query(None, description="Filtrar por severidade"),
    instance: Optional[str] = Query(None, description="Filtrar por instância")
):
    """Página de alertas."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    # Converter período para horas
    period_hours = {"24h": 24, "7d": 168, "30d": 720}.get(period, 24)

    # Buscar alertas
    alerts = analytics.get_recent_alerts(
        instance_name=instance,
        severity=severity,
        hours=period_hours,
        limit=100
    )

    # Buscar hotspots
    hotspots = analytics.get_alert_hotspots(hours=period_hours, min_alerts=3)

    return templates.TemplateResponse("dashboard_alerts.html", {
        "request": request,
        "alerts": alerts,
        "hotspots": hotspots,
        "period": period,
        "severity_filter": severity,
        "instance_filter": instance,
        "page_title": "Alertas"
    })


@router.get("/dashboard/instances", response_class=HTMLResponse)
async def dashboard_instances(request: Request):
    """Página de status das instâncias."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    # Buscar health de monitoramento
    health = analytics.get_monitoring_health(hours=24)

    return templates.TemplateResponse("dashboard_instances.html", {
        "request": request,
        "health": health,
        "page_title": "Instâncias"
    })


@router.get("/dashboard/trends", response_class=HTMLResponse)
async def dashboard_trends(
    request: Request,
    days: int = Query(7, description="Dias de histórico: 7, 14, 30")
):
    """Página de gráficos de tendências."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    # Buscar tendências
    trends = analytics.get_performance_trends(days=days, granularity='day')

    return templates.TemplateResponse("dashboard_trends.html", {
        "request": request,
        "trends": trends,
        "days": days,
        "page_title": "Tendências"
    })


# ========== API REST (JSON Endpoints) ==========

@router.get("/api/dashboard/summary")
async def api_dashboard_summary(hours: int = Query(24, description="Horas de histórico")):
    """API: Resumo de métricas principais."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    summary = analytics.get_executive_summary(hours=hours)
    return summary


@router.get("/api/dashboard/queries")
async def api_dashboard_queries(
    period: str = Query("24h", description="Período: 24h, 7d, 30d"),
    metric: str = Query("cpu_time_ms", description="Métrica para ordenação"),
    instance: Optional[str] = Query(None, description="Filtrar por instância"),
    limit: int = Query(20, description="Número de resultados")
):
    """API: Top queries problemáticas."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    period_hours = {"24h": 24, "7d": 168, "30d": 720}.get(period, 24)

    queries = analytics.get_worst_performers(
        metric=metric,
        hours=period_hours,
        limit=limit,
        instance_name=instance
    )

    return queries


@router.get("/api/dashboard/alerts")
async def api_dashboard_alerts(
    period: str = Query("24h", description="Período"),
    severity: Optional[str] = Query(None, description="Filtrar por severidade"),
    instance: Optional[str] = Query(None, description="Filtrar por instância")
):
    """API: Alertas recentes."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    period_hours = {"24h": 24, "7d": 168, "30d": 720}.get(period, 24)

    alerts = analytics.get_recent_alerts(
        instance_name=instance,
        severity=severity,
        hours=period_hours,
        limit=100
    )

    return alerts


@router.get("/api/dashboard/instances")
async def api_dashboard_instances():
    """API: Status das instâncias monitoradas."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    health = analytics.get_monitoring_health(hours=24)
    return health


@router.get("/api/dashboard/trends")
async def api_dashboard_trends(
    days: int = Query(7, description="Dias de histórico"),
    granularity: str = Query("day", description="Granularidade: hour, day, week")
):
    """API: Dados para gráficos de tendências."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    trends = analytics.get_performance_trends(days=days, granularity=granularity)

    # Formatar para Chart.js
    labels = [t['time_bucket'] for t in trends]

    chart_data = {
        "labels": labels,
        "datasets": [
            {
                "label": "CPU Médio (ms)",
                "data": [t['avg_cpu_ms'] for t in trends],
                "borderColor": "rgb(255, 99, 132)",
                "backgroundColor": "rgba(255, 99, 132, 0.1)"
            },
            {
                "label": "Duration Média (ms)",
                "data": [t['avg_duration_ms'] for t in trends],
                "borderColor": "rgb(54, 162, 235)",
                "backgroundColor": "rgba(54, 162, 235, 0.1)"
            },
            {
                "label": "Queries Únicas",
                "data": [t['unique_queries'] for t in trends],
                "borderColor": "rgb(75, 192, 192)",
                "backgroundColor": "rgba(75, 192, 192, 0.1)",
                "yAxisID": "y1"
            }
        ]
    }

    return chart_data


@router.get("/api/dashboard/cache-efficiency")
async def api_cache_efficiency(hours: int = Query(24, description="Horas de histórico")):
    """API: Eficiência do cache e ROI."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    efficiency = analytics.get_cache_efficiency(hours=hours)
    return efficiency


# ========== Planos de Otimização ==========

@router.get("/plans", response_class=HTMLResponse)
async def plans_list(request: Request):
    """Lista de planos de otimização."""
    if not plan_state_manager:
        raise HTTPException(status_code=503, detail="Plan manager não inicializado")

    plans = plan_state_manager.list_plans(limit=50)

    return templates.TemplateResponse("plan_list.html", {
        "request": request,
        "plans": plans,
        "page_title": "Planos de Otimização"
    })


@router.get("/plans/{plan_id}", response_class=HTMLResponse)
async def plan_detail(request: Request, plan_id: str):
    """Detalhes de um plano específico."""
    if not plan_state_manager:
        raise HTTPException(status_code=503, detail="Plan manager não inicializado")

    plan = plan_state_manager.get_plan(plan_id, sync_vetos=True)

    return templates.TemplateResponse("plan_detail.html", {
        "request": request,
        "plan": plan,
        "plan_id": plan_id,
        "page_title": f"Plano {plan_id}"
    })


@router.get("/api/plans")
async def api_plans_list(
    limit: int = Query(50, description="Número de planos"),
    status: Optional[str] = Query(None, description="Filtrar por status")
):
    """API: Lista de planos."""
    if not plan_state_manager:
        raise HTTPException(status_code=503, detail="Plan manager não inicializado")

    plans = plan_state_manager.list_plans(limit=limit, status_filter=status)

    return {
        "plans": [p.to_dict() for p in plans],
        "count": len(plans)
    }


@router.get("/api/plans/{plan_id}")
async def api_plan_detail(plan_id: str):
    """API: Detalhes de um plano."""
    if not plan_state_manager:
        raise HTTPException(status_code=503, detail="Plan manager não inicializado")

    plan = plan_state_manager.get_plan(plan_id, sync_vetos=True)

    if not plan:
        raise HTTPException(status_code=404, detail="Plano não encontrado")

    return plan.to_dict()


@router.get("/api/plans/{plan_id}/status")
async def api_plan_status(plan_id: str):
    """API: Status do plano."""
    if not plan_state_manager:
        raise HTTPException(status_code=503, detail="Plan manager não inicializado")

    summary = plan_state_manager.get_plan_summary(plan_id)

    if not summary:
        raise HTTPException(status_code=404, detail="Plano não encontrado")

    return summary


@router.post("/api/plans/{plan_id}/veto")
async def api_veto_plan(plan_id: str, request: VetoRequest):
    """API: Veta plano completo."""
    if not plan_state_manager or not veto_system:
        raise HTTPException(status_code=503, detail="Sistema não inicializado")

    plan = plan_state_manager.get_plan(plan_id)

    if not plan:
        raise HTTPException(status_code=404, detail="Plano não encontrado")

    # Aplicar veto
    veto = veto_system.veto_plan(
        plan_id=plan_id,
        vetoed_by=request.vetoed_by or "unknown",
        reason=request.reason,
        veto_expires_at=plan.veto_window_expires_at
    )

    # Atualizar status do plano
    plan_state_manager.update_plan_status(plan_id, 'vetoed')

    return {
        "success": True,
        "message": "Plano vetado com sucesso",
        "veto": veto.to_dict()
    }


@router.post("/api/plans/{plan_id}/approve")
async def api_approve_plan(plan_id: str, request: ApproveRequest):
    """API: Aprova plano."""
    if not plan_state_manager:
        raise HTTPException(status_code=503, detail="Plan manager não inicializado")

    success = plan_state_manager.approve_plan(
        plan_id=plan_id,
        approved_by=request.approved_by or "unknown",
        execute_now=request.execute_now
    )

    if not success:
        raise HTTPException(status_code=404, detail="Plano não encontrado")

    return {
        "success": True,
        "message": "Plano aprovado com sucesso",
        "execute_now": request.execute_now
    }


@router.post("/api/plans/{plan_id}/items/{item_id}/veto")
async def api_veto_item(plan_id: str, item_id: str, request: VetoRequest):
    """API: Veta item específico."""
    if not plan_state_manager or not veto_system:
        raise HTTPException(status_code=503, detail="Sistema não inicializado")

    plan = plan_state_manager.get_plan(plan_id)

    if not plan:
        raise HTTPException(status_code=404, detail="Plano não encontrado")

    # Verificar se item existe
    item_exists = any(opt.id == item_id for opt in plan.optimizations)

    if not item_exists:
        raise HTTPException(status_code=404, detail="Item não encontrado no plano")

    # Aplicar veto
    veto = veto_system.veto_item(
        plan_id=plan_id,
        item_id=item_id,
        vetoed_by=request.vetoed_by or "unknown",
        reason=request.reason,
        veto_expires_at=plan.veto_window_expires_at
    )

    # Sincronizar plano
    plan_state_manager.get_plan(plan_id, sync_vetos=True)

    return {
        "success": True,
        "message": "Item vetado com sucesso",
        "veto": veto.to_dict()
    }


@router.delete("/api/plans/{plan_id}/items/{item_id}/veto")
async def api_unveto_item(plan_id: str, item_id: str):
    """API: Remove veto de item específico."""
    if not veto_system:
        raise HTTPException(status_code=503, detail="Sistema de veto não inicializado")

    success = veto_system.remove_item_veto(plan_id, item_id)

    if not success:
        raise HTTPException(status_code=404, detail="Veto não encontrado")

    return {
        "success": True,
        "message": "Veto removido com sucesso"
    }
