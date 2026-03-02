"""
Rotas da API REST e páginas HTML do dashboard.
"""
import json
import asyncio
from fastapi import APIRouter, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timedelta

from .app import templates
from .models import (
    DashboardSummary, QueryInfo, AlertInfo, InstanceStatus,
    VetoRequest, ApproveRequest, PlanStatusResponse,
    SavePromptRequest, RollbackPromptRequest, ValidDbType, ValidPromptType,
    ThresholdsRequest, CollectionSettingsRequest, CacheConfigRequest,
    LLMConfigRequest, MonitorConfigRequest, TeamsConfigRequest,
    TimeoutsConfigRequest, SecurityConfigRequest, QueryCacheConfigRequest,
    LoggingConfigRequest, MetricsStoreConfigRequest,
)
from ..utils.metrics_store import MetricsStore
from ..utils.query_analytics import QueryAnalytics

# Router principal
router = APIRouter()

PERIOD_MAP = {"1h": 1, "6h": 6, "12h": 12, "24h": 24, "7d": 168, "30d": 720}

from ..utils.llm_analyzer import LLMAnalyzer
from ..utils.currency import CurrencyConverter

# Instâncias globais (serão injetadas ao iniciar a API)
metrics_store: Optional[MetricsStore] = None
analytics: Optional[QueryAnalytics] = None
plan_state_manager = None
veto_system = None
db_config_global: Optional[dict] = None
llm_analyzer: Optional[LLMAnalyzer] = None
global_config: Optional[dict] = None


def init_dependencies(store: MetricsStore, db_config: Optional[dict] = None, config: Optional[dict] = None) -> None:
    """
    Inicializa dependências globais da API.

    Args:
        store: Instância do MetricsStore para acesso aos dados
        db_config: Configuração de databases (databases.json)
        config: Configuração completa (config.json)
    """
    global metrics_store, analytics, plan_state_manager, veto_system, db_config_global, llm_analyzer, global_config

    metrics_store = store
    analytics = QueryAnalytics(store)
    db_config_global = db_config
    global_config = config

    if config:
        try:
            llm_analyzer = LLMAnalyzer(config, metrics_store=store)
        except Exception as e:
            print(f"Erro ao inicializar LLMAnalyzer na API: {e}")

    from ..optimization.veto_system import VetoSystem
    from ..optimization.plan_state import PlanStateManager

    veto_system = VetoSystem(metrics_store=store)
    plan_state_manager = PlanStateManager(metrics_store=store, veto_system=veto_system)


def _filter_health_data(health: dict) -> dict:
    """
    Combina dados de health com a configuração para mostrar status de todas as instâncias.
    - Instâncias com dados: status baseado na saúde (ou 'disabled' se desabilitada na config)
    - Instâncias sem dados: 'offline' (se habilitada) ou 'disabled'
    """
    if not db_config_global or 'databases' not in db_config_global:
        return health
        
    # Mapa da config: nome -> dados da config
    config_map = {db['name']: db for db in db_config_global['databases']}
    
    # Mapa dos dados atuais: nome -> dados de health
    data_map = {inst['name']: inst for inst in health.get('active_instances', [])}
    
    final_instances = []
    
    # Iterar sobre TODAS as instâncias da configuração para garantir que todas apareçam
    for name, config in config_map.items():
        is_enabled = config.get('enabled', True)
        instance_data = data_map.get(name)
        
        if instance_data:
            # Instância tem dados históricos
            if not is_enabled:
                instance_data['status'] = 'disabled'
            elif 'status' not in instance_data:
                # Se não tem status definido, assume online (já que tem dados)
                instance_data['status'] = 'online'
            
            instance_data['enabled'] = is_enabled
            final_instances.append(instance_data)
        else:
            # Instância NÃO tem dados (pode ser nova, offline ou desabilitada)
            status = 'disabled' if not is_enabled else 'offline'
            
            final_instances.append({
                'name': name,
                'type': config.get('type', 'unknown'),
                'queries_found': 0,
                'queries_analyzed': 0,
                'cache_hits': 0,
                'errors': 0,
                'total_cycles': 0,
                'avg_cycle_duration_ms': 0.0,
                'last_cycle': None,
                'health_score': 0.0,
                'status': status,
                'enabled': is_enabled
            })
            
    health['active_instances'] = final_instances
    return health


@router.get("/", response_class=HTMLResponse)
async def dashboard_home(
    request: Request,
    period: str = Query("24h", description="Período: 24h, 7d, 30d"),
    instance: Optional[str] = Query(None, description="Filtrar por instância")
):
    """Dashboard principal com visão geral do sistema."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    period_hours = PERIOD_MAP.get(period, 24)

    summary = analytics.get_executive_summary(hours=period_hours)
    health = analytics.get_monitoring_health(hours=period_hours)

    # Filtrar instâncias desabilitadas
    health = _filter_health_data(health)

    # Lista de instâncias disponíveis para o filtro
    available_instances = [inst['name'] for inst in health['active_instances']]

    worst_result = analytics.get_worst_performers(
        metric='cpu_time_ms', hours=period_hours, limit=5, instance_name=instance
    )
    worst_queries = worst_result['queries']
    recent_alerts = analytics.get_recent_alerts(hours=period_hours, limit=10, instance_name=instance)

    # Dados para graficos (com filtros aplicados)
    timeline = analytics.get_queries_timeline(hours=period_hours, instance_name=instance)
    distribution = analytics.get_queries_distribution(hours=period_hours, instance_name=instance)

    # Wait stats para widget
    wait_stats_data = []
    wait_stats_instance = instance
    if metrics_store:
        try:
            if not wait_stats_instance and available_instances:
                wait_stats_instance = available_instances[0]
            if wait_stats_instance:
                wait_stats_data = metrics_store.get_wait_stats_delta(
                    instance_name=wait_stats_instance, hours=period_hours, limit=10
                )
        except Exception:
            pass

    return templates.TemplateResponse("dashboard_home.html", {
        "request": request,
        "summary": summary,
        "health": health,
        "worst_queries": worst_queries,
        "recent_alerts": recent_alerts,
        "timeline": timeline,
        "distribution": distribution,
        "wait_stats": wait_stats_data,
        "wait_stats_instance": wait_stats_instance,
        "period": period,
        "instance": instance,
        "instance_filter": available_instances,
        "page_title": "Dashboard"
    })


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_redirect(request: Request):
    """Redirect para dashboard principal."""
    return await dashboard_home(request)


@router.get("/dashboard/queries", response_class=HTMLResponse)
async def dashboard_queries(
    request: Request,
    period: str = Query("24h", description="Periodo: 24h, 7d, 30d"),
    metric: str = Query("severity", description="Metrica para ordenacao"),
    view_mode: str = Query("top", description="Modo de visualizacao: top ou chronological"),
    instance: Optional[str] = Query(None, description="Filtrar por instancia"),
    database: Optional[str] = Query(None, description="Filtrar por database"),
    login_name: Optional[str] = Query(None, description="Filtrar por usuario"),
    host_name: Optional[str] = Query(None, description="Filtrar por host"),
    program_name: Optional[str] = Query(None, description="Filtrar por aplicacao"),
    severity: Optional[str] = Query(None, description="Filtrar por severidade"),
    search: Optional[str] = Query(None, description="Busca por texto na query"),
    page: int = Query(1, ge=1, description="Pagina atual")
):
    """Pagina de queries - Top Queries ou Cronologico."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics nao inicializado")

    period_hours = PERIOD_MAP.get(period, 24)
    page_size = 50
    offset = (page - 1) * page_size

    try:
        if view_mode == "chronological":
            # Modo cronologico: capturas individuais ordenadas por data
            result = analytics.get_chronological_queries(
                hours=period_hours,
                limit=page_size,
                offset=offset,
                instance_name=instance,
                database_name=database,
                login_name=login_name,
                host_name=host_name,
                program_name=program_name,
                search_text=search,
                severity=severity
            )
        else:
            # Modo top: queries agregadas por hash
            result = analytics.get_worst_performers(
                metric=metric,
                hours=period_hours,
                limit=page_size,
                offset=offset,
                instance_name=instance,
                database_name=database,
                login_name=login_name,
                host_name=host_name,
                program_name=program_name,
                severity=severity,
                search_text=search
            )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Buscar lista de instancias disponiveis para o filtro
    health = analytics.get_monitoring_health(hours=period_hours)
    health = _filter_health_data(health)
    available_instances = [inst['name'] for inst in health['active_instances']]

    # Buscar opcoes de filtros (logins, hosts, apps, databases)
    filter_options = analytics.get_filter_options(hours=period_hours)

    # Buscar timeline de capturas
    timeline = analytics.get_queries_timeline(hours=period_hours, instance_name=instance)

    # Buscar distribuicao para graficos
    distribution = analytics.get_queries_distribution(hours=period_hours, instance_name=instance)

    # Calcular paginacao
    total_pages = (result['total'] + page_size - 1) // page_size if result['total'] > 0 else 1

    return templates.TemplateResponse("dashboard_queries.html", {
        "request": request,
        "queries": result['queries'],
        "total_queries": result['total'],
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "has_more": result['has_more'],
        "period": period,
        "metric": metric,
        "view_mode": view_mode,
        "instance": instance,
        "database": database,
        "login_name": login_name,
        "host_name": host_name,
        "program_name": program_name,
        "severity": severity,
        "search": search,
        "instance_filter": available_instances,
        "database_filter": filter_options['databases'],
        "login_filter": filter_options['logins'],
        "host_filter": filter_options['hosts'],
        "program_filter": filter_options['programs'],
        "timeline": timeline,
        "distribution": distribution,
        "page_title": "Top Queries" if view_mode == "top" else "Cronologico"
    })


@router.get("/dashboard/queries/{query_hash}", response_class=HTMLResponse)
async def query_detail(request: Request, query_hash: str):
    """Página de detalhes de uma query específica."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    conn = metrics_store._get_connection()

    # Buscar informações da query coletada
    query_info = conn.execute("""
        SELECT
            qc.query_hash, qc.instance_name, qc.database_name,
            qc.schema_name, qc.table_name, qc.query_preview,
            qc.query_type, qc.collected_at, qc.sanitized_query, qc.query_text,
            qc.login_name, qc.host_name, qc.program_name,
            qc.client_interface_name, qc.session_id, qc.db_type
        FROM queries_collected qc
        WHERE qc.query_hash = ?
        LIMIT 1
    """, [query_hash]).fetchone()

    if not query_info:
        raise HTTPException(status_code=404, detail="Query não encontrada")

    # Buscar metricas agregadas
    metrics = conn.execute("""
        SELECT
            AVG(qm.cpu_time_ms) as avg_cpu_time_ms,
            MAX(qm.cpu_time_ms) as max_cpu_time_ms,
            MIN(qm.cpu_time_ms) as min_cpu_time_ms,
            AVG(qm.duration_ms) as avg_duration_ms,
            MAX(qm.duration_ms) as max_duration_ms,
            MIN(qm.duration_ms) as min_duration_ms,
            AVG(qm.logical_reads) as avg_logical_reads,
            MAX(qm.logical_reads) as max_logical_reads,
            AVG(qm.physical_reads) as avg_physical_reads,
            AVG(qm.writes) as avg_writes,
            AVG(qm.wait_time_ms) as avg_wait_time_ms,
            AVG(qm.memory_mb) as avg_memory_mb,
            AVG(qm.row_count) as avg_row_count,
            COUNT(*) as total_executions,
            FIRST(qm.wait_type ORDER BY qm.collected_at DESC) as last_wait_type,
            FIRST(qm.blocking_session_id ORDER BY qm.collected_at DESC) as last_blocking_session_id,
            MAX(qm.execution_count) as max_execution_count,
            SUM(qm.cpu_time_ms) as total_cpu_impact,
            SUM(qm.duration_ms) as total_duration_impact
        FROM query_metrics qm
        WHERE qm.query_hash = ?
    """, [query_hash]).fetchone()

    # Buscar análise LLM
    llm_analysis = conn.execute("""
        SELECT
            la.analysis_text, la.recommendations, la.severity,
            la.analyzed_at, la.analysis_duration_ms, la.tokens_used,
            la.prompt_tokens, la.completion_tokens,
            la.estimated_cost_usd, la.estimated_cost_brl,
            la.model_used, la.seen_count, la.last_seen
        FROM llm_analyses la
        WHERE la.query_hash = ?
        ORDER BY la.analyzed_at DESC
        LIMIT 1
    """, [query_hash]).fetchone()

    # Buscar histórico de execuções (últimas 50)
    executions = conn.execute("""
        SELECT
            qm.cpu_time_ms, qm.duration_ms, qm.logical_reads,
            qm.physical_reads, qm.writes, qm.collected_at,
            qm.wait_time_ms, qm.wait_type, qm.memory_mb, qm.row_count
        FROM query_metrics qm
        WHERE qm.query_hash = ?
        ORDER BY qm.collected_at DESC
        LIMIT 50
    """, [query_hash]).fetchall()

    # Buscar alertas relacionados
    alerts = conn.execute("""
        SELECT
            pa.id,
            pa.alert_type,
            pa.severity,
            pa.threshold_value,
            pa.actual_value,
            pa.alert_time
        FROM performance_alerts pa
        WHERE pa.query_hash = ?
        ORDER BY pa.alert_time DESC
        LIMIT 10
    """, [query_hash]).fetchall()

    # Buscar sumário de alertas (Agrupados por tipo)
    alert_groups = conn.execute("""
        SELECT
            alert_type,
            COUNT(*) as count,
            MAX(alert_time) as last_alert,
            MAX(severity) as max_severity, -- Aproximação, idealmente usaria CASE
            AVG(actual_value) as avg_value
        FROM performance_alerts
        WHERE query_hash = ?
        GROUP BY alert_type
        ORDER BY count DESC
    """, [query_hash]).fetchall()

    # Buscar detalhes do último bloqueio (se houver)
    blocking_info = None
    last_blocking_alert = conn.execute("""
        SELECT extra_info
        FROM performance_alerts
        WHERE query_hash = ? 
          AND alert_type = 'blocking'
          AND extra_info IS NOT NULL
        ORDER BY alert_time DESC
        LIMIT 1
    """, [query_hash]).fetchone()

    if last_blocking_alert and last_blocking_alert[0]:
        try:
            blocking_info = json.loads(last_blocking_alert[0])
        except (json.JSONDecodeError, TypeError):
            pass

    return templates.TemplateResponse("query_detail.html", {
        "request": request,
        "query_hash": query_hash,
        "blocking_info": blocking_info,
        "query_info": {
            "instance_name": query_info[1],
            "database_name": query_info[2],
            "schema_name": query_info[3],
            "table_name": query_info[4],
            "query_preview": query_info[5],
            "query_type": query_info[6],
            "collected_at": query_info[7],
            "sanitized_query": query_info[8],
            "query_text": query_info[9],
            "login_name": query_info[10],
            "host_name": query_info[11],
            "program_name": query_info[12],
            "client_interface_name": query_info[13],
            "session_id": query_info[14],
            "db_type": query_info[15]
        },
        "metrics": {
            "avg_cpu_time_ms": metrics[0] or 0,
            "max_cpu_time_ms": metrics[1] or 0,
            "min_cpu_time_ms": metrics[2] or 0,
            "avg_duration_ms": metrics[3] or 0,
            "max_duration_ms": metrics[4] or 0,
            "min_duration_ms": metrics[5] or 0,
            "avg_logical_reads": metrics[6] or 0,
            "max_logical_reads": metrics[7] or 0,
            "avg_physical_reads": metrics[8] or 0,
            "avg_writes": metrics[9] or 0,
            "avg_wait_time_ms": metrics[10] or 0,
            "avg_memory_mb": metrics[11] or 0,
            "avg_row_count": metrics[12] or 0,
            "total_executions": metrics[13] or 0,
            "last_wait_type": metrics[14],
            "last_blocking_session_id": metrics[15],
            "max_execution_count": metrics[16] or 0,
            "total_cpu_impact": metrics[17] or 0,
            "total_duration_impact": metrics[18] or 0
        },
        "llm_analysis": {
            "analysis_text": llm_analysis[0] if llm_analysis else None,
            "recommendations": llm_analysis[1] if llm_analysis else None,
            "severity": llm_analysis[2] if llm_analysis else None,
            "analyzed_at": llm_analysis[3] if llm_analysis else None,
            "analysis_duration_ms": llm_analysis[4] if llm_analysis else None,
            "tokens_used": llm_analysis[5] if llm_analysis else None,
            "prompt_tokens": llm_analysis[6] if llm_analysis else 0,
            "completion_tokens": llm_analysis[7] if llm_analysis else 0,
            "estimated_cost_usd": llm_analysis[8] if llm_analysis else 0,
            "estimated_cost_brl": llm_analysis[9] if llm_analysis else 0,
            "model_used": llm_analysis[10] if llm_analysis else None,
            "seen_count": llm_analysis[11] if llm_analysis else 0,
            "last_seen": llm_analysis[12] if llm_analysis else None
        } if llm_analysis else None,
        "executions": [
            {
                "cpu_time_ms": exec[0],
                "duration_ms": exec[1],
                "logical_reads": exec[2],
                "physical_reads": exec[3],
                "writes": exec[4],
                "collected_at": exec[5],
                "wait_time_ms": exec[6],
                "wait_type": exec[7],
                "memory_mb": exec[8],
                "row_count": exec[9]
            }
            for exec in executions
        ],
        "alerts": [
            {
                "id": alert[0],
                "alert_type": alert[1],
                "severity": alert[2],
                "threshold_value": alert[3],
                "actual_value": alert[4],
                "alert_time": alert[5]
            }
            for alert in alerts
        ],
        "alert_groups": [
            {
                "type": g[0],
                "count": g[1],
                "last_alert": g[2],
                "severity": g[3],
                "avg_value": g[4]
            }
            for g in alert_groups
        ],
        "page_title": f"Query {query_hash[:8]}..."
    })


@router.get("/dashboard/alerts/{alert_id}", response_class=HTMLResponse)
async def alert_detail(request: Request, alert_id: int):
    """Página de detalhes de um alerta específico."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    conn = analytics.store._get_connection()

    # Buscar informacoes do alerta (incluindo extra_info para blocking)
    alert_query = """
        SELECT
            id, alert_time, instance_name, query_hash, alert_type,
            severity, threshold_value, actual_value, database_name,
            table_name, query_preview, teams_notified, extra_info
        FROM performance_alerts
        WHERE id = ?
    """
    alert = conn.execute(alert_query, [alert_id]).fetchone()

    if not alert:
        raise HTTPException(status_code=404, detail="Alerta nao encontrado")

    # Parse blocking_info se existir
    blocking_info = None
    if alert[12]:  # extra_info
        try:
            blocking_info = json.loads(alert[12])
        except (json.JSONDecodeError, TypeError):
            pass

    # Buscar informações da query
    query_info = None
    if alert[3]:  # query_hash
        query_query = """
            SELECT
                query_hash, instance_name, database_name, schema_name,
                table_name, query_type, query_preview, collected_at
            FROM queries_collected
            WHERE query_hash = ?
            ORDER BY collected_at DESC
            LIMIT 1
        """
        query_result = conn.execute(query_query, [alert[3]]).fetchone()

        # Calcular contagem de ocorrências separadamente
        count_query = "SELECT COUNT(*) FROM queries_collected WHERE query_hash = ?"
        count_result = conn.execute(count_query, [alert[3]]).fetchone()
        occurrence_count = count_result[0] if count_result else 0

        if query_result:
            query_info = {
                'query_hash': query_result[0],
                'instance_name': query_result[1],
                'database_name': query_result[2],
                'schema_name': query_result[3],
                'table_name': query_result[4],
                'query_type': query_result[5],
                'query_preview': query_result[6],
                'collected_at': query_result[7],
                'occurrence_count': occurrence_count
            }

    # Buscar análise LLM
    llm_analysis = None
    if alert[3]:  # query_hash
        llm_query = """
            SELECT
                analysis_text, recommendations, severity,
                analyzed_at, analysis_duration_ms, tokens_used
            FROM llm_analyses
            WHERE query_hash = ?
            AND expires_at > CURRENT_TIMESTAMP
            ORDER BY analyzed_at DESC
            LIMIT 1
        """
        llm_result = conn.execute(llm_query, [alert[3]]).fetchone()
        if llm_result:
            llm_analysis = {
                'analysis_text': llm_result[0],
                'recommendations': llm_result[1],
                'severity': llm_result[2],
                'analyzed_at': llm_result[3],
                'analysis_duration_ms': llm_result[4],
                'tokens_used': llm_result[5]
            }

    # Buscar métricas agregadas
    metrics = None
    if alert[3]:  # query_hash
        metrics_query = """
            SELECT
                AVG(cpu_time_ms) as avg_cpu_time_ms,
                MIN(cpu_time_ms) as min_cpu_time_ms,
                MAX(cpu_time_ms) as max_cpu_time_ms,
                AVG(duration_ms) as avg_duration_ms,
                MIN(duration_ms) as min_duration_ms,
                MAX(duration_ms) as max_duration_ms,
                AVG(logical_reads) as avg_logical_reads,
                MAX(logical_reads) as max_logical_reads,
                COUNT(*) as execution_count
            FROM query_metrics
            WHERE query_hash = ?
        """
        metrics_result = conn.execute(metrics_query, [alert[3]]).fetchone()
        if metrics_result:
            metrics = {
                'avg_cpu_time_ms': metrics_result[0],
                'min_cpu_time_ms': metrics_result[1],
                'max_cpu_time_ms': metrics_result[2],
                'avg_duration_ms': metrics_result[3],
                'min_duration_ms': metrics_result[4],
                'max_duration_ms': metrics_result[5],
                'avg_logical_reads': metrics_result[6],
                'max_logical_reads': metrics_result[7],
                'execution_count': metrics_result[8]
            }

    # Buscar alertas relacionados (mesma query)
    related_alerts = []
    if alert[3]:  # query_hash
        related_query = """
            SELECT
                id, alert_time, alert_type, severity,
                threshold_value, actual_value
            FROM performance_alerts
            WHERE query_hash = ?
            AND id != ?
            ORDER BY alert_time DESC
            LIMIT 10
        """
        related_results = conn.execute(related_query, [alert[3], alert_id]).fetchall()
        related_alerts = [
            {
                'id': r[0],
                'alert_time': r[1],
                'alert_type': r[2],
                'severity': r[3],
                'threshold_value': r[4],
                'actual_value': r[5]
            }
            for r in related_results
        ]

    return templates.TemplateResponse("alert_detail.html", {
        "request": request,
        "alert": {
            'id': alert[0],
            'alert_time': alert[1],
            'instance_name': alert[2],
            'query_hash': alert[3],
            'alert_type': alert[4],
            'severity': alert[5],
            'threshold_value': alert[6],
            'actual_value': alert[7],
            'database_name': alert[8],
            'table_name': alert[9],
            'query_preview': alert[10],
            'teams_notified': alert[11]
        },
        "blocking_info": blocking_info,
        "query_info": query_info,
        "llm_analysis": llm_analysis,
        "metrics": metrics,
        "related_alerts": related_alerts,
        "page_title": f"Alerta #{alert_id}"
    })


@router.get("/dashboard/alerts", response_class=HTMLResponse)
async def dashboard_alerts(
    request: Request,
    period: str = Query("24h", description="Periodo: 24h, 7d, 30d"),
    severity: Optional[str] = Query(None, description="Filtrar por severidade"),
    instance: Optional[str] = Query(None, description="Filtrar por instancia"),
    database: Optional[str] = Query(None, description="Filtrar por database"),
    table: Optional[str] = Query(None, description="Filtrar por tabela")
):
    """Pagina de alertas."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics nao inicializado")

    period_hours = PERIOD_MAP.get(period, 24)

    # Sanitizar params que podem chegar como string "None" via URL
    if table in ("None", "null", ""):
        table = None
    if database in ("None", "null", ""):
        database = None
    if instance in ("None", "null", ""):
        instance = None

    # Buscar alertas filtrados para a lista
    alerts = analytics.get_recent_alerts(
        instance_name=instance,
        severity=severity,
        database_name=database,
        table_name=table,
        hours=period_hours,
        limit=100
    )

    # Buscar totais por severidade SEM filtro de severidade (para KPIs)
    all_alerts_for_totals = analytics.get_recent_alerts(
        instance_name=instance,
        database_name=database,
        table_name=table,
        hours=period_hours,
        limit=1000
    )

    # Calcular totais por severidade
    alert_totals = {
        'critical': sum(1 for a in all_alerts_for_totals if a.get('severity') == 'critical'),
        'high': sum(1 for a in all_alerts_for_totals if a.get('severity') == 'high'),
        'medium': sum(1 for a in all_alerts_for_totals if a.get('severity') == 'medium'),
        'low': sum(1 for a in all_alerts_for_totals if a.get('severity') == 'low'),
        'total': len(all_alerts_for_totals)
    }

    hotspots = analytics.get_alert_hotspots(
        hours=period_hours,
        min_alerts=3,
        instance_name=instance,
        database_name=database,
        severity=severity
    )

    # Buscar lista de instancias disponiveis para o filtro
    health = analytics.get_monitoring_health(hours=period_hours)
    health = _filter_health_data(health)
    available_instances = [inst['name'] for inst in health['active_instances']]

    # Gerar titulo dinamico baseado nos filtros
    title_parts = []
    if table:
        title_parts.append(f"Tabela: {table}")
    if database:
        title_parts.append(f"Database: {database}")
    if instance:
        title_parts.append(f"Instancia: {instance}")
    page_title = "Alertas" + (f" - {', '.join(title_parts)}" if title_parts else "")

    return templates.TemplateResponse("dashboard_alerts.html", {
        "request": request,
        "alerts": alerts,
        "alert_totals": alert_totals,
        "hotspots": hotspots,
        "period": period,
        "severity_filter": severity,
        "instance": instance,
        "database": database,
        "table": table,
        "instance_filter": available_instances,
        "page_title": page_title
    })


@router.get("/dashboard/instances", response_class=HTMLResponse)
async def dashboard_instances(request: Request):
    """Página de status das instâncias."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    # Buscar health de monitoramento
    health = analytics.get_monitoring_health(hours=24)
    health = _filter_health_data(health)

    return templates.TemplateResponse("dashboard_instances.html", {
        "request": request,
        "health": health,
        "page_title": "Instâncias"
    })


@router.get("/dashboard/trends", response_class=HTMLResponse)
async def dashboard_trends(
    request: Request,
    days: int = Query(7, description="Dias de histórico: 7, 14, 30"),
    instance: Optional[str] = Query(None, description="Filtrar por instância")
):
    """Página de gráficos de tendências."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    # Buscar tendências
    trends = analytics.get_performance_trends(
        instance_name=instance,
        days=days,
        granularity='day'
    )

    # Buscar lista de instâncias para o filtro
    health = analytics.get_monitoring_health(hours=24)
    health = _filter_health_data(health)
    available_instances = [inst['name'] for inst in health['active_instances']]

    return templates.TemplateResponse("dashboard_trends.html", {
        "request": request,
        "trends": trends,
        "days": days,
        "instance": instance,
        "instance_filter": available_instances,
        "page_title": "Tendências"
    })


@router.get("/dashboard/applications", response_class=HTMLResponse)
async def dashboard_applications(request: Request, period: str = "24h", instance: Optional[str] = None):
    """Dashboard de Top Aplicações."""
    if not analytics:
        raise HTTPException(status_code=503)

    period_hours = PERIOD_MAP.get(period, 24)
    applications = analytics.get_top_problematic_applications(hours=period_hours, limit=50, instance_name=instance)

    # Buscar lista de instâncias disponíveis
    health = analytics.get_monitoring_health(hours=period_hours)
    health = _filter_health_data(health)
    available_instances = [inst['name'] for inst in health['active_instances']]

    return templates.TemplateResponse("dashboard_applications.html", {
        "request": request,
        "applications": applications,
        "period": period,
        "instance": instance,
        "instance_filter": available_instances,
        "page_title": "Top Aplicações"
    })


@router.get("/dashboard/users", response_class=HTMLResponse)
async def dashboard_users(request: Request, period: str = "24h", instance: Optional[str] = None):
    """Dashboard de Top Usuários."""
    if not analytics:
        raise HTTPException(status_code=503)

    period_hours = PERIOD_MAP.get(period, 24)
    users = analytics.get_top_problematic_users(hours=period_hours, limit=50, instance_name=instance)

    # Buscar lista de instâncias disponíveis
    health = analytics.get_monitoring_health(hours=period_hours)
    health = _filter_health_data(health)
    available_instances = [inst['name'] for inst in health['active_instances']]

    return templates.TemplateResponse("dashboard_users.html", {
        "request": request,
        "users": users,
        "period": period,
        "instance": instance,
        "instance_filter": available_instances,
        "page_title": "Top Usuários"
    })


@router.get("/dashboard/hosts", response_class=HTMLResponse)
async def dashboard_hosts(request: Request, period: str = "24h", instance: Optional[str] = None):
    """Dashboard de Top Hosts."""
    if not analytics:
        raise HTTPException(status_code=503)

    period_hours = PERIOD_MAP.get(period, 24)
    hosts = analytics.get_top_problematic_hosts(hours=period_hours, limit=50, instance_name=instance)

    # Buscar lista de instâncias disponíveis
    health = analytics.get_monitoring_health(hours=period_hours)
    health = _filter_health_data(health)
    available_instances = [inst['name'] for inst in health['active_instances']]

    return templates.TemplateResponse("dashboard_hosts.html", {
        "request": request,
        "hosts": hosts,
        "period": period,
        "instance": instance,
        "instance_filter": available_instances,
        "page_title": "Top Hosts"
    })


@router.get("/dashboard/llm", response_class=HTMLResponse)
async def dashboard_llm(request: Request, days: int = 30):
    """Dashboard de uso de LLM."""
    if not analytics:
        raise HTTPException(status_code=503)

    stats = analytics.get_llm_usage_stats(days=days)

    return templates.TemplateResponse("dashboard_llm.html", {
        "request": request,
        "stats": stats,
        "days": days,
        "page_title": "Métricas LLM"
    })


@router.get("/dashboard/duckdb", response_class=HTMLResponse)
async def dashboard_duckdb(request: Request):
    """Dashboard de estatísticas do DuckDB."""
    if not metrics_store:
        raise HTTPException(status_code=503)

    stats = metrics_store.get_duckdb_stats()

    return templates.TemplateResponse("dashboard_duckdb.html", {
        "request": request,
        "stats": stats,
        "page_title": "Status DuckDB"
    })


@router.post("/api/admin/clear-data")
async def clear_all_data():
    """
    Trunca todas as tabelas de dados operacionais do DuckDB.
    Mantém apenas tabelas de configuração (llm_config, thresholds, settings, etc.).
    """
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    DATA_TABLES = [
        'queries_collected',
        'query_metrics',
        'llm_analyses',
        'performance_alerts',
        'table_metadata',
        'wait_stats_snapshots',
        'optimization_items',
        'optimization_plans',
        'optimization_executions',
        'veto_records',
    ]

    results = {}
    errors = []
    for table in DATA_TABLES:
        try:
            count = metrics_store.execute_query(f"SELECT COUNT(*) FROM {table}")
            before = count[0][0] if count and count[0] else 0
            metrics_store.execute(f"DELETE FROM {table}")
            results[table] = before
        except Exception as e:
            errors.append(f"{table}: {str(e)}")

    # monitoring_cycles: apaga o histórico mas mantém o ciclo mais recente por instância
    # para que o status das instâncias continue aparecendo como "online" no dashboard
    try:
        count = metrics_store.execute_query("SELECT COUNT(*) FROM monitoring_cycles")
        before = count[0][0] if count and count[0] else 0
        metrics_store.execute("""
            DELETE FROM monitoring_cycles
            WHERE id NOT IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (PARTITION BY instance_name ORDER BY cycle_started_at DESC) AS rn
                    FROM monitoring_cycles
                ) sub WHERE rn = 1
            )
        """)
        kept = metrics_store.execute_query("SELECT COUNT(*) FROM monitoring_cycles")
        kept_count = kept[0][0] if kept and kept[0] else 0
        results['monitoring_cycles'] = before - kept_count
    except Exception as e:
        errors.append(f"monitoring_cycles: {str(e)}")

    try:
        metrics_store.execute("VACUUM")
    except Exception:
        pass

    return {
        "status": "ok",
        "deleted": results,
        "errors": errors
    }


@router.get("/settings", response_class=HTMLResponse)
async def dashboard_settings(request: Request):
    """Dashboard de configurações de performance."""
    return templates.TemplateResponse("dashboard_settings.html", {
        "request": request,
        "page_title": "Configurações"
    })


@router.get("/api/dashboard/instances")
async def api_dashboard_instances():
    """API: Status das instâncias monitoradas."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    health = analytics.get_monitoring_health(hours=24)
    health = _filter_health_data(health)
    return health


# ========== API REST (JSON Endpoints) ==========

@router.get("/api/dashboard/summary")
async def api_dashboard_summary(hours: int = Query(24, description="Horas de histórico")):
    """API: Resumo de métricas principais."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    summary = analytics.get_executive_summary(hours=hours)
    return summary


@router.get("/api/wait-stats")
async def api_wait_stats(
    instance: str = Query(..., description="Nome da instancia"),
    period: str = Query("24h", description="Periodo: 1h, 6h, 12h, 24h, 7d, 30d")
):
    """API: Top wait types por delta no periodo."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore nao inicializado")

    period_hours = PERIOD_MAP.get(period, 24)

    try:
        result = metrics_store.get_wait_stats_delta(
            instance_name=instance,
            hours=period_hours,
            limit=20
        )
        return {"wait_stats": result, "instance": instance, "period": period}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/wait-stats/timeline")
async def api_wait_stats_timeline(
    instance: str = Query(..., description="Nome da instancia"),
    period: str = Query("24h", description="Periodo: 1h, 6h, 12h, 24h, 7d, 30d")
):
    """API: Timeline de wait stats por categoria para Chart.js."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore nao inicializado")

    period_hours = PERIOD_MAP.get(period, 24)

    try:
        result = metrics_store.get_wait_stats_timeline(
            instance_name=instance,
            hours=period_hours
        )
        return {"timeline": result.get('timeline', []), "categories": result.get('categories', []), "instance": instance, "period": period}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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

    period_hours = PERIOD_MAP.get(period, 24)

    try:
        result = analytics.get_worst_performers(
            metric=metric,
            hours=period_hours,
            limit=limit,
            instance_name=instance
        )
        queries = result['queries']
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Filtrar instâncias desabilitadas se filtro de instância não for fornecido
    # (se fornecido, o filtro na query já resolve, mas podemos validar)
    health = analytics.get_monitoring_health(hours=period_hours)
    health = _filter_health_data(health)

    # Se instância específica solicitada está desabilitada, retornar vazio ou erro?
    # Melhor retornar vazio
    if instance:
        enabled_instances = {inst['name'] for inst in health['active_instances']}
        if instance not in enabled_instances:
            return []

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

    period_hours = PERIOD_MAP.get(period, 24)

    alerts = analytics.get_recent_alerts(
        instance_name=instance,
        severity=severity,
        hours=period_hours,
        limit=100
    )
    
    # Filtrar alertas de instâncias desabilitadas
    health = analytics.get_monitoring_health(hours=period_hours)
    health = _filter_health_data(health)
    enabled_instances = {inst['name'] for inst in health['active_instances']}
    
    alerts = [a for a in alerts if a['instance_name'] in enabled_instances]

    return alerts


@router.get("/api/dashboard/trends")
async def api_dashboard_trends(
    days: int = Query(7, description="Dias de histórico"),
    granularity: str = Query("day", description="Granularidade: hour, day, week"),
    instance: Optional[str] = Query(None, description="Filtrar por instância")
):
    """API: Dados para gráficos de tendências."""
    if not analytics:
        raise HTTPException(status_code=503, detail="Analytics não inicializado")

    trends = analytics.get_performance_trends(
        instance_name=instance,
        days=days,
        granularity=granularity
    )

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


import sqlparse

@router.get("/api/queries/{query_hash}/full-text")
async def get_query_full_text(query_hash: str):
    """API: Retorna query completa para modal."""
    if not metrics_store:
        raise HTTPException(status_code=503)

    conn = metrics_store._get_connection()
    result = conn.execute("""
        SELECT query_text, sanitized_query
        FROM queries_collected
        WHERE query_hash = ?
        ORDER BY collected_at DESC
        LIMIT 1
    """, [query_hash]).fetchone()

    if not result:
        raise HTTPException(status_code=404)

    raw_query = result[0] or result[1] or "N/A"
    formatted_query = sqlparse.format(raw_query, reindent=True, keyword_case='upper')

    return {
        "query_hash": query_hash,
        "query_text": raw_query,
        "formatted_query": formatted_query
    }





@router.get("/api/queries/timeline")
async def get_queries_timeline(
    period: str = Query("24h", description="Período"),
    instance: Optional[str] = Query(None)
):
    """API: Timeline de capturas de queries."""
    if not analytics:
        raise HTTPException(status_code=503)

    period_hours = PERIOD_MAP.get(period, 24)
    timeline = analytics.get_queries_timeline(hours=period_hours, instance_name=instance)

    return {
        "timeline": timeline,
        "period": period
    }


@router.get("/api/queries/distribution")
async def get_queries_distribution(
    period: str = Query("24h", description="Período"),
    instance: Optional[str] = Query(None)
):
    """API: Distribuição de queries para gráficos."""
    if not analytics:
        raise HTTPException(status_code=503)

    period_hours = PERIOD_MAP.get(period, 24)
    distribution = analytics.get_queries_distribution(hours=period_hours, instance_name=instance)

    return distribution


class BulkAnalysisRequest(BaseModel):
    query_hashes: List[str]

def _run_bulk_analysis(query_hashes: List[str]) -> List[dict]:
    """Executa analise bulk em thread separada (nao bloqueia o event loop)."""
    results = []
    conn = metrics_store._get_connection()

    input_price = 0.075
    output_price = 0.30
    if global_config and 'llm' in global_config:
        input_price = global_config['llm'].get('input_price_per_million', 0.075)
        output_price = global_config['llm'].get('output_price_per_million', 0.30)

    for query_hash in query_hashes:
        try:
            query_data = conn.execute("""
                SELECT sanitized_query, query_text, instance_name, database_name, schema_name, table_name, db_type
                FROM queries_collected
                WHERE query_hash = ?
                LIMIT 1
            """, [query_hash]).fetchone()

            if not query_data:
                results.append({"hash": query_hash, "status": "not_found"})
                continue

            sanitized_query = query_data[0] or query_data[1]
            instance_name = query_data[2]
            database_name = query_data[3]
            schema_name = query_data[4]
            table_name = query_data[5]
            db_type = query_data[6] or 'sqlserver'

            metrics_data = conn.execute("""
                SELECT
                    AVG(cpu_time_ms) as cpu_time_ms,
                    AVG(duration_ms) as duration_ms,
                    AVG(logical_reads) as logical_reads,
                    AVG(physical_reads) as physical_reads,
                    AVG(writes) as writes
                FROM query_metrics
                WHERE query_hash = ?
            """, [query_hash]).fetchone()

            metrics_dict = {
                "cpu_time_ms": metrics_data[0] or 0,
                "duration_ms": metrics_data[1] or 0,
                "logical_reads": metrics_data[2] or 0,
                "physical_reads": metrics_data[3] or 0,
                "writes": metrics_data[4] or 0,
            }

            analysis = llm_analyzer.analyze_query_performance(
                sanitized_query=sanitized_query,
                placeholder_map="N/A (Bulk Analysis)",
                table_ddl="N/A (Bulk Analysis)",
                existing_indexes="N/A (Bulk Analysis)",
                metrics=metrics_dict,
                db_type=db_type,
            )

            if isinstance(analysis, dict):
                prompt_tokens = analysis.get('prompt_tokens', 0)
                completion_tokens = analysis.get('completion_tokens', 0)
                estimated_cost_usd = (
                    (prompt_tokens / 1_000_000 * input_price) +
                    (completion_tokens / 1_000_000 * output_price)
                )
                ptax_rate = CurrencyConverter.get_usd_brl_rate()
                estimated_cost_brl = estimated_cost_usd * ptax_rate

                metrics_store.add_llm_analysis(
                    query_hash=query_hash,
                    instance_name=instance_name,
                    database_name=database_name,
                    schema_name=schema_name,
                    table_name=table_name,
                    analysis_text=analysis.get('explanation', 'Erro'),
                    recommendations=analysis.get('suggestions', ''),
                    severity=analysis.get('priority', 'medium').lower(),
                    model_used=llm_analyzer.model_name,
                    tokens_used=analysis.get('tokens_used', 0),
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    estimated_cost_usd=estimated_cost_usd,
                    estimated_cost_brl=estimated_cost_brl,
                )
                results.append({"hash": query_hash, "status": "analyzed"})
            else:
                results.append({"hash": query_hash, "status": "error", "message": "Invalid response format"})

        except Exception as e:
            print(f"Erro na analise bulk de {query_hash}: {e}")
            results.append({"hash": query_hash, "status": "error", "message": str(e)})

    return results


@router.post("/api/analyze/bulk")
async def api_analyze_bulk(request: BulkAnalysisRequest):
    """API: Analisa multiplas queries via LLM em thread separada."""
    if not llm_analyzer:
        raise HTTPException(status_code=503, detail="LLMAnalyzer nao disponivel")
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore nao inicializado")

    results = await asyncio.to_thread(_run_bulk_analysis, request.query_hashes)
    return {"results": results}


# ========== Planos de Otimização ==========

@router.get("/plans", response_class=HTMLResponse)
async def plans_list(request: Request):
    """Lista de planos de otimização."""
    if not plan_state_manager:
        raise HTTPException(status_code=503, detail="Plan manager não inicializado")

    plans = plan_state_manager.list_plans(limit=50)

    # Pre-calcular contagem de risco por plano para o template
    plans_with_risk = []
    for plan in plans:
        risk_counts = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        for opt in plan.optimizations:
            rl = getattr(opt, 'risk_level', 'medium')
            if rl in risk_counts:
                risk_counts[rl] += 1
        plans_with_risk.append({
            'plan': plan,
            'risk_counts': risk_counts
        })

    return templates.TemplateResponse("plan_list.html", {
        "request": request,
        "plans_with_risk": plans_with_risk,
        "plans": plans,
        "page_title": "Planos de Otimizacao"
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


@router.get("/api/plans/{plan_id}/status")
async def api_plan_status(plan_id: str):
    """API: Status do plano."""
    if not plan_state_manager:
        raise HTTPException(status_code=503, detail="Plan manager não inicializado")

    summary = plan_state_manager.get_plan_summary(plan_id)

    if not summary:
        raise HTTPException(status_code=404, detail="Plano não encontrado")

    return summary


@router.get("/api/plans/{plan_id}")
async def api_plan_detail(plan_id: str):
    """API: Detalhes de um plano."""
    if not plan_state_manager:
        raise HTTPException(status_code=503, detail="Plan manager não inicializado")

    plan = plan_state_manager.get_plan(plan_id, sync_vetos=False)

    if not plan:
        raise HTTPException(status_code=404, detail="Plano não encontrado")

    return plan.to_dict()


@router.post("/api/plans/generate")
async def api_generate_plan():
    """API: Força a geração de um plano de otimização."""
    if not metrics_store or not global_config:
        raise HTTPException(status_code=503, detail="Sistema não inicializado")

    from ..optimization.weekly_planner import WeeklyOptimizationPlanner
    
    try:
        planner = WeeklyOptimizationPlanner(metrics_store, global_config)
        plan = planner.generate_weekly_plan()
        
        return {
            "success": True,
            "message": "Plano gerado com sucesso",
            "plan_id": plan['plan_id']
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/api/plans/{plan_id}")
async def api_delete_plan(plan_id: str):
    """API: Deleta um plano de otimização."""
    if not plan_state_manager:
        raise HTTPException(status_code=503, detail="Plan manager não inicializado")

    success = plan_state_manager.delete_plan(plan_id)

    if not success:
        raise HTTPException(status_code=404, detail="Plano não encontrado ou erro ao deletar")

    return {
        "success": True,
        "message": "Plano removido com sucesso"
    }


# ========== CONFIGURAÇÕES (Settings) ==========

@router.get("/api/settings/thresholds")
async def get_thresholds():
    """Retorna thresholds configurados para todos os dbtypes."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    query = """
        SELECT db_type, execution_time_ms, cpu_time_ms, logical_reads,
               physical_reads, writes, wait_time_ms, memory_mb, row_count,
               updated_at, updated_by
        FROM performance_thresholds_by_dbtype
        ORDER BY db_type
    """
    results = metrics_store.execute_query(query)

    return [
        {
            "db_type": r[0],
            "execution_time_ms": r[1],
            "cpu_time_ms": r[2],
            "logical_reads": r[3],
            "physical_reads": r[4],
            "writes": r[5],
            "wait_time_ms": r[6],
            "memory_mb": r[7],
            "row_count": r[8],
            "updated_at": r[9],
            "updated_by": r[10]
        }
        for r in results
    ]


@router.post("/api/settings/thresholds/{db_type}")
async def update_thresholds(db_type: str, thresholds: ThresholdsRequest):
    """Atualiza thresholds para um dbtype específico."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    # Validar db_type
    valid_types = ['hana', 'sqlserver', 'postgresql']
    if db_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"db_type inválido. Use: {', '.join(valid_types)}")

    # Buscar valores antigos para auditoria
    old_values = metrics_store.execute_query(
        "SELECT * FROM performance_thresholds_by_dbtype WHERE db_type = ?",
        (db_type,)
    )

    t = thresholds
    # Atualizar
    metrics_store.execute("""
        UPDATE performance_thresholds_by_dbtype
        SET execution_time_ms = ?, cpu_time_ms = ?, logical_reads = ?,
            physical_reads = ?, writes = ?, wait_time_ms = ?, memory_mb = ?,
            row_count = ?, updated_at = CURRENT_TIMESTAMP, updated_by = 'api_user'
        WHERE db_type = ?
    """, (
        t.execution_time_ms, t.cpu_time_ms, t.logical_reads,
        t.physical_reads, t.writes, t.wait_time_ms, t.memory_mb,
        t.row_count, db_type
    ))

    # Registrar auditoria
    metrics_store.execute("""
        INSERT INTO config_audit_log (changed_by, config_table, config_key, old_value, new_value)
        VALUES ('api_user', 'thresholds', ?, ?, ?)
    """, (db_type, str(old_values), json.dumps(t.model_dump(exclude_none=True))))

    return {"status": "updated", "db_type": db_type}


@router.get("/api/settings/collection")
async def get_collection_settings():
    """Retorna settings de coleta para todos os dbtypes."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    query = """
        SELECT db_type, min_duration_seconds, collect_active_queries,
               collect_expensive_queries, collect_table_scans, max_queries_per_cycle,
               updated_at, updated_by
        FROM collection_settings_by_dbtype
        ORDER BY db_type
    """
    results = metrics_store.execute_query(query)

    return [
        {
            "db_type": r[0],
            "min_duration_seconds": r[1],
            "collect_active_queries": r[2],
            "collect_expensive_queries": r[3],
            "collect_table_scans": r[4],
            "max_queries_per_cycle": r[5],
            "updated_at": r[6],
            "updated_by": r[7]
        }
        for r in results
    ]


@router.post("/api/settings/collection/{db_type}")
async def update_collection_settings(db_type: str, settings: CollectionSettingsRequest):
    """Atualiza settings de coleta para um dbtype específico."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    # Validar db_type
    valid_types = ['hana', 'sqlserver', 'postgresql']
    if db_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"db_type inválido. Use: {', '.join(valid_types)}")

    # Buscar valores antigos
    old_values = metrics_store.execute_query(
        "SELECT * FROM collection_settings_by_dbtype WHERE db_type = ?",
        (db_type,)
    )

    # Atualizar
    metrics_store.execute("""
        UPDATE collection_settings_by_dbtype
        SET min_duration_seconds = ?, collect_active_queries = ?,
            collect_expensive_queries = ?, collect_table_scans = ?,
            max_queries_per_cycle = ?, updated_at = CURRENT_TIMESTAMP,
            updated_by = 'api_user'
        WHERE db_type = ?
    """, (
        settings.min_duration_seconds,
        settings.collect_active_queries,
        settings.collect_expensive_queries,
        settings.collect_table_scans,
        settings.max_queries_per_cycle,
        db_type
    ))

    # Registrar auditoria
    metrics_store.execute("""
        INSERT INTO config_audit_log (changed_by, config_table, config_key, old_value, new_value)
        VALUES ('api_user', 'collection', ?, ?, ?)
    """, (db_type, str(old_values), json.dumps(settings.model_dump(exclude_none=True))))

    return {"status": "updated", "db_type": db_type}


@router.get("/api/settings/cache")
async def get_cache_config():
    """Retorna configuração de cache."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    query = """
        SELECT enabled, ttl_hours, max_entries, cache_ddl, cache_indexes,
               updated_at, updated_by
        FROM metadata_cache_config
        WHERE id = 1
    """
    result = metrics_store.execute_query(query)

    if not result or len(result) == 0:
        return {"enabled": True, "ttl_hours": 24, "max_entries": 1000}

    r = result[0]
    return {
        "enabled": r[0],
        "ttl_hours": r[1],
        "max_entries": r[2],
        "cache_ddl": r[3],
        "cache_indexes": r[4],
        "updated_at": r[5],
        "updated_by": r[6]
    }


@router.post("/api/settings/cache")
async def update_cache_config(config: CacheConfigRequest):
    """Atualiza configuração de cache."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    # Buscar valores antigos
    old_values = metrics_store.execute_query(
        "SELECT * FROM metadata_cache_config WHERE id = 1"
    )

    # Atualizar
    metrics_store.execute("""
        UPDATE metadata_cache_config
        SET enabled = ?, ttl_hours = ?, max_entries = ?,
            cache_ddl = ?, cache_indexes = ?,
            updated_at = CURRENT_TIMESTAMP, updated_by = 'api_user'
        WHERE id = 1
    """, (
        config.enabled,
        config.ttl_hours,
        config.max_entries,
        config.cache_ddl,
        config.cache_indexes
    ))

    # Registrar auditoria
    metrics_store.execute("""
        INSERT INTO config_audit_log (changed_by, config_table, config_key, old_value, new_value)
        VALUES ('api_user', 'cache', 'global', ?, ?)
    """, (str(old_values), json.dumps(config.model_dump(exclude_none=True))))

    return {"status": "updated"}


@router.post("/api/settings/cache/clear")
async def clear_metadata_cache():
    """Limpa cache de metadados de todas as instâncias ativas."""
    # Nota: precisaríamos acesso aos monitors ativos para limpar o cache
    # Por ora, retornamos sucesso (cache será limpo no próximo restart)
    return {
        "status": "cleared",
        "message": "Cache será limpo no próximo ciclo de monitoramento"
    }


@router.post("/api/settings/reset/{db_type}")
async def reset_to_defaults(db_type: str):
    """Reseta configurações para defaults do dbtype."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    # Validar db_type
    valid_types = ['hana', 'sqlserver', 'postgresql']
    if db_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"db_type inválido. Use: {', '.join(valid_types)}")

    # Deletar configuração existente
    metrics_store.execute(
        "DELETE FROM performance_thresholds_by_dbtype WHERE db_type = ?",
        (db_type,)
    )
    metrics_store.execute(
        "DELETE FROM collection_settings_by_dbtype WHERE db_type = ?",
        (db_type,)
    )

    # Reinicializar com defaults
    metrics_store.init_config_defaults()

    return {"status": "reset", "db_type": db_type}


@router.get("/api/settings/audit")
async def get_audit_log(limit: int = Query(100, description="Número de registros")):
    """Retorna histórico de alterações de configuração."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    query = """
        SELECT id, changed_at, changed_by, config_table, config_key,
               old_value, new_value, change_reason
        FROM config_audit_log
        ORDER BY changed_at DESC
        LIMIT ?
    """
    results = metrics_store.execute_query(query, (limit,))

    return [
        {
            "id": r[0],
            "changed_at": r[1],
            "changed_by": r[2],
            "config_table": r[3],
            "config_key": r[4],
            "old_value": r[5],
            "new_value": r[6],
            "change_reason": r[7]
        }
        for r in results
    ]


# ========== CONFIGURAÇÕES GERAIS (System Settings) ==========

@router.get("/api/settings/llm")
async def get_llm_config():
    """Retorna configuração de LLM."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    query = """
        SELECT provider, model, temperature, max_tokens, max_retries, retry_delays,
               max_requests_per_day, max_requests_per_minute, max_requests_per_cycle,
               min_delay_between_requests, updated_at, updated_by
        FROM llm_config WHERE id = 1
    """
    result = metrics_store.execute_query(query)

    if not result:
        return {}

    r = result[0]
    return {
        "provider": r[0],
        "model": r[1],
        "temperature": r[2],
        "max_tokens": r[3],
        "max_retries": r[4],
        "retry_delays": json.loads(r[5]) if r[5] else [3, 8, 15],
        "max_requests_per_day": r[6],
        "max_requests_per_minute": r[7],
        "max_requests_per_cycle": r[8],
        "min_delay_between_requests": r[9],
        "updated_at": r[10],
        "updated_by": r[11]
    }


@router.post("/api/settings/llm")
async def update_llm_config(config: LLMConfigRequest):
    """Atualiza configuração de LLM."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")


    # Buscar valores antigos
    old_values = metrics_store.execute_query("SELECT * FROM llm_config WHERE id = 1")

    # Atualizar
    metrics_store.execute("""
        UPDATE llm_config SET
            provider = ?, model = ?, temperature = ?, max_tokens = ?,
            max_retries = ?, retry_delays = ?,
            max_requests_per_day = ?, max_requests_per_minute = ?,
            max_requests_per_cycle = ?, min_delay_between_requests = ?,
            updated_at = CURRENT_TIMESTAMP, updated_by = 'api_user'
        WHERE id = 1
    """, (
        config.provider,
        config.model,
        config.temperature,
        config.max_tokens,
        config.max_retries,
        json.dumps(config.retry_delays or []),
        config.max_requests_per_day,
        config.max_requests_per_minute,
        config.max_requests_per_cycle,
        config.min_delay_between_requests
    ))

    # Registrar auditoria
    metrics_store.execute("""
        INSERT INTO config_audit_log (changed_by, config_table, config_key, old_value, new_value)
        VALUES ('api_user', 'llm', 'global', ?, ?)
    """, (str(old_values), json.dumps(config.model_dump(exclude_none=True))))

    return {"status": "updated"}


@router.post("/api/settings/llm/reset")
async def reset_llm_config():
    """Reseta configuração de LLM para os valores padrão."""
    from ..utils.llm_providers import get_default_model
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    default_model = get_default_model('groq')
    metrics_store.execute(f"""
        UPDATE llm_config SET
            provider = 'groq', model = '{default_model}',
            temperature = 0.1, max_tokens = 8192, max_retries = 3,
            retry_delays = '[3, 8, 15]', max_requests_per_day = 1500,
            max_requests_per_minute = 60, max_requests_per_cycle = 20,
            min_delay_between_requests = 2.0,
            updated_at = CURRENT_TIMESTAMP, updated_by = 'reset'
        WHERE id = 1
    """)
    return {"status": "reset"}


@router.get("/api/settings/llm/models")
async def get_llm_models(provider: str = Query(default=None)):
    """Lista modelos disponíveis para o provider LLM."""
    import os
    from ..utils.llm_providers import get_api_key_env, list_models

    # Se provider não foi passado, usa o salvo no DB
    if not provider:
        provider = 'groq'
        if metrics_store:
            try:
                result = metrics_store.execute_query("SELECT provider FROM llm_config WHERE id = 1")
                if result and result[0]:
                    provider = result[0][0] or 'groq'
            except Exception:
                pass

    try:
        env_var = get_api_key_env(provider)
    except ValueError as e:
        return {"models": [], "error": str(e)}

    api_key = os.getenv(env_var)
    if not api_key:
        return {"models": [], "error": f"{env_var} nao configurada"}
    try:
        model_ids = list_models(provider, api_key)
        return {"models": model_ids}
    except Exception as e:
        return {"models": [], "error": str(e)}


@router.get("/api/settings/monitor")
async def get_monitor_config():
    """Retorna configuração de monitor."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    result = metrics_store.execute_query("SELECT interval_seconds, updated_at, updated_by FROM monitor_config WHERE id = 1")

    if not result:
        return {}

    r = result[0]
    return {
        "interval_seconds": r[0],
        "updated_at": r[1],
        "updated_by": r[2]
    }


@router.get("/api/status/collector")
async def get_collector_status():
    """Retorna status do coletor: último ciclo e quando ocorrerá o próximo."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    interval_result = metrics_store.execute_query(
        "SELECT interval_seconds FROM monitor_config WHERE id = 1"
    )
    interval_seconds = interval_result[0][0] if interval_result else 60

    last_cycle_result = metrics_store.execute_query(
        "SELECT MAX(cycle_started_at) FROM monitoring_cycles"
    )
    last_cycle_at = last_cycle_result[0][0] if last_cycle_result and last_cycle_result[0][0] else None

    if last_cycle_at:
        import datetime as _dt
        # Comparar sempre com datetime do mesmo tipo: se naive, usa now(); se aware, usa now(utc)
        if hasattr(last_cycle_at, 'tzinfo') and last_cycle_at.tzinfo is not None:
            now = _dt.datetime.now(last_cycle_at.tzinfo)
        else:
            now = _dt.datetime.now()
        elapsed = (now - last_cycle_at).total_seconds()
        seconds_until_next = max(0, interval_seconds - elapsed)
    else:
        seconds_until_next = interval_seconds

    return {
        "interval_seconds": interval_seconds,
        "last_cycle_at": last_cycle_at.isoformat() if last_cycle_at else None,
        "seconds_until_next": int(seconds_until_next),
    }


@router.post("/api/settings/monitor")
async def update_monitor_config(config: MonitorConfigRequest):
    """Atualiza configuração de monitor."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")


    old_values = metrics_store.execute_query("SELECT * FROM monitor_config WHERE id = 1")

    metrics_store.execute("""
        UPDATE monitor_config SET
            interval_seconds = ?,
            updated_at = CURRENT_TIMESTAMP,
            updated_by = 'api_user'
        WHERE id = 1
    """, (config.interval_seconds,))

    metrics_store.execute("""
        INSERT INTO config_audit_log (changed_by, config_table, config_key, old_value, new_value)
        VALUES ('api_user', 'monitor', 'global', ?, ?)
    """, (str(old_values), json.dumps(config.model_dump(exclude_none=True))))

    return {"status": "updated"}


@router.get("/api/settings/teams")
async def get_teams_config():
    """Retorna configuração de Teams integration."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    result = metrics_store.execute_query("""
        SELECT enabled, webhook_url, notify_on_cache_hit, priority_filter, timeout, updated_at, updated_by
        FROM teams_config WHERE id = 1
    """)

    if not result:
        return {}

    r = result[0]
    raw_url = r[1] or ""
    if raw_url:
        # Mascarar webhook: manter dominio + "***"
        try:
            from urllib.parse import urlparse
            parsed = urlparse(raw_url)
            masked_url = f"{parsed.scheme}://{parsed.netloc}/***"
        except Exception:
            masked_url = "***"
    else:
        masked_url = ""

    return {
        "enabled": r[0],
        "webhook_url": masked_url,
        "webhook_configured": bool(raw_url),
        "notify_on_cache_hit": r[2],
        "priority_filter": json.loads(r[3]) if r[3] else [],
        "timeout": r[4],
        "updated_at": r[5],
        "updated_by": r[6]
    }


@router.post("/api/settings/teams")
async def update_teams_config(config: TeamsConfigRequest):
    """Atualiza configuração de Teams."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")


    old_values = metrics_store.execute_query("SELECT * FROM teams_config WHERE id = 1")

    metrics_store.execute("""
        UPDATE teams_config SET
            enabled = ?,
            webhook_url = ?,
            notify_on_cache_hit = ?,
            priority_filter = ?,
            timeout = ?,
            updated_at = CURRENT_TIMESTAMP,
            updated_by = 'api_user'
        WHERE id = 1
    """, (
        config.enabled,
        config.webhook_url,
        config.notify_on_cache_hit,
        json.dumps(config.priority_filter or []),
        config.timeout
    ))

    metrics_store.execute("""
        INSERT INTO config_audit_log (changed_by, config_table, config_key, old_value, new_value)
        VALUES ('api_user', 'teams', 'global', ?, ?)
    """, (str(old_values), json.dumps(config.model_dump(exclude={'webhook_url'}))))

    return {"status": "updated"}


@router.get("/api/settings/timeouts")
async def get_timeouts_config():
    """Retorna configuração de timeouts."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    result = metrics_store.execute_query("""
        SELECT database_connect, database_query, llm_analysis, thread_shutdown, circuit_breaker_recovery,
               updated_at, updated_by
        FROM timeouts_config WHERE id = 1
    """)

    if not result:
        return {}

    r = result[0]
    return {
        "database_connect": r[0],
        "database_query": r[1],
        "llm_analysis": r[2],
        "thread_shutdown": r[3],
        "circuit_breaker_recovery": r[4],
        "updated_at": r[5],
        "updated_by": r[6]
    }


@router.post("/api/settings/timeouts")
async def update_timeouts_config(config: TimeoutsConfigRequest):
    """Atualiza configuração de timeouts."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")


    old_values = metrics_store.execute_query("SELECT * FROM timeouts_config WHERE id = 1")

    metrics_store.execute("""
        UPDATE timeouts_config SET
            database_connect = ?,
            database_query = ?,
            llm_analysis = ?,
            thread_shutdown = ?,
            circuit_breaker_recovery = ?,
            updated_at = CURRENT_TIMESTAMP,
            updated_by = 'api_user'
        WHERE id = 1
    """, (
        config.database_connect,
        config.database_query,
        config.llm_analysis,
        config.thread_shutdown,
        config.circuit_breaker_recovery
    ))

    metrics_store.execute("""
        INSERT INTO config_audit_log (changed_by, config_table, config_key, old_value, new_value)
        VALUES ('api_user', 'timeouts', 'global', ?, ?)
    """, (str(old_values), json.dumps(config.model_dump(exclude_none=True))))

    return {"status": "updated"}


@router.get("/api/settings/security")
async def get_security_config():
    """Retorna configuração de security."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    result = metrics_store.execute_query("""
        SELECT sanitize_queries, placeholder_prefix, show_example_values, updated_at, updated_by
        FROM security_config WHERE id = 1
    """)

    if not result:
        return {}

    r = result[0]
    return {
        "sanitize_queries": r[0],
        "placeholder_prefix": r[1],
        "show_example_values": r[2],
        "updated_at": r[3],
        "updated_by": r[4]
    }


@router.post("/api/settings/security")
async def update_security_config(config: SecurityConfigRequest):
    """Atualiza configuração de security."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")


    old_values = metrics_store.execute_query("SELECT * FROM security_config WHERE id = 1")

    metrics_store.execute("""
        UPDATE security_config SET
            sanitize_queries = ?,
            placeholder_prefix = ?,
            show_example_values = ?,
            updated_at = CURRENT_TIMESTAMP,
            updated_by = 'api_user'
        WHERE id = 1
    """, (
        config.sanitize_queries,
        config.placeholder_prefix,
        config.show_example_values
    ))

    metrics_store.execute("""
        INSERT INTO config_audit_log (changed_by, config_table, config_key, old_value, new_value)
        VALUES ('api_user', 'security', 'global', ?, ?)
    """, (str(old_values), json.dumps(config.model_dump(exclude_none=True))))

    return {"status": "updated"}


@router.get("/api/settings/query_cache")
async def get_query_cache_config():
    """Retorna configuração de query cache."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    result = metrics_store.execute_query("""
        SELECT enabled, ttl_hours, updated_at, updated_by
        FROM query_cache_config WHERE id = 1
    """)

    if not result:
        return {}

    r = result[0]
    return {
        "enabled": r[0],
        "ttl_hours": r[1],
        "updated_at": r[2],
        "updated_by": r[3]
    }


@router.post("/api/settings/query_cache")
async def update_query_cache_config(config: QueryCacheConfigRequest):
    """Atualiza configuração de query cache."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")


    old_values = metrics_store.execute_query("SELECT * FROM query_cache_config WHERE id = 1")

    metrics_store.execute("""
        UPDATE query_cache_config SET
            enabled = ?,
            ttl_hours = ?,
            updated_at = CURRENT_TIMESTAMP,
            updated_by = 'api_user'
        WHERE id = 1
    """, (
        config.enabled,
        config.ttl_hours
    ))

    metrics_store.execute("""
        INSERT INTO config_audit_log (changed_by, config_table, config_key, old_value, new_value)
        VALUES ('api_user', 'query_cache', 'global', ?, ?)
    """, (str(old_values), json.dumps(config.model_dump(exclude_none=True))))

    return {"status": "updated"}


@router.get("/api/settings/logging")
async def get_logging_config():
    """Retorna configuração de logging."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    result = metrics_store.execute_query("""
        SELECT level, format, log_file, enable_console, updated_at, updated_by
        FROM logging_config WHERE id = 1
    """)

    if not result:
        return {}

    r = result[0]
    return {
        "level": r[0],
        "format": r[1],
        "log_file": r[2],
        "enable_console": r[3],
        "updated_at": r[4],
        "updated_by": r[5]
    }


@router.post("/api/settings/logging")
async def update_logging_config(config: LoggingConfigRequest):
    """Atualiza configuração de logging."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")


    old_values = metrics_store.execute_query("SELECT * FROM logging_config WHERE id = 1")

    metrics_store.execute("""
        UPDATE logging_config SET
            level = ?,
            format = ?,
            log_file = ?,
            enable_console = ?,
            updated_at = CURRENT_TIMESTAMP,
            updated_by = 'api_user'
        WHERE id = 1
    """, (
        config.level,
        config.format,
        config.log_file,
        config.enable_console
    ))

    metrics_store.execute("""
        INSERT INTO config_audit_log (changed_by, config_table, config_key, old_value, new_value)
        VALUES ('api_user', 'logging', 'global', ?, ?)
    """, (str(old_values), json.dumps(config.model_dump(exclude_none=True))))

    return {"status": "updated"}


@router.get("/api/settings/metrics_store")
async def get_metrics_store_config():
    """Retorna configuração de metrics store."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    result = metrics_store.execute_query("""
        SELECT db_path, enable_compression, retention_days, updated_at, updated_by
        FROM metrics_store_config WHERE id = 1
    """)

    if not result:
        return {}

    r = result[0]
    return {
        "db_path": r[0],
        "enable_compression": r[1],
        "retention_days": r[2],
        "updated_at": r[3],
        "updated_by": r[4]
    }


@router.post("/api/settings/metrics_store")
async def update_metrics_store_config(config: MetricsStoreConfigRequest):
    """Atualiza configuração de metrics store."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")


    old_values = metrics_store.execute_query("SELECT * FROM metrics_store_config WHERE id = 1")

    metrics_store.execute("""
        UPDATE metrics_store_config SET
            db_path = ?,
            enable_compression = ?,
            retention_days = ?,
            updated_at = CURRENT_TIMESTAMP,
            updated_by = 'api_user'
        WHERE id = 1
    """, (
        config.db_path,
        config.enable_compression,
        config.retention_days
    ))

    metrics_store.execute("""
        INSERT INTO config_audit_log (changed_by, config_table, config_key, old_value, new_value)
        VALUES ('api_user', 'metrics_store', 'global', ?, ?)
    """, (str(old_values), json.dumps(config.model_dump(exclude_none=True))))

    return {"status": "updated"}


@router.get("/api/settings/weekly_optimizer")
async def get_weekly_optimizer_config():
    """Retorna configuração completa de weekly optimizer."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    result = metrics_store.execute_query("""
        SELECT enabled, analysis_day, analysis_time, execution_day, execution_time,
               report_day, report_time, veto_window_hours, check_before_execution,
               table_size_gb_medium, table_size_gb_high, table_size_gb_critical,
               index_fragmentation_percent, max_execution_time_minutes,
               auto_rollback_enabled, degradation_threshold_percent, wait_after_execution_minutes,
               api_enabled, api_host, api_port, cors_enabled,
               analysis_days, min_occurrences, min_avg_duration_ms,
               updated_at, updated_by
        FROM weekly_optimizer_config WHERE id = 1
    """)

    if not result:
        return {}

    r = result[0]
    return {
        "enabled": r[0],
        "schedule": {
            "analysis_day": r[1],
            "analysis_time": r[2],
            "execution_day": r[3],
            "execution_time": r[4],
            "report_day": r[5],
            "report_time": r[6]
        },
        "veto_window": {
            "hours": r[7],
            "check_before_execution": r[8]
        },
        "risk_thresholds": {
            "table_size_gb_medium": r[9],
            "table_size_gb_high": r[10],
            "table_size_gb_critical": r[11],
            "index_fragmentation_percent": r[12],
            "max_execution_time_minutes": r[13]
        },
        "auto_rollback": {
            "enabled": r[14],
            "degradation_threshold_percent": r[15],
            "wait_after_execution_minutes": r[16]
        },
        "api": {
            "enabled": r[17],
            "host": r[18],
            "port": r[19],
            "cors_enabled": r[20]
        },
        "analysis": {
            "days": r[21],
            "min_occurrences": r[22],
            "min_avg_duration_ms": r[23]
        },
        "updated_at": r[24],
        "updated_by": r[25]
    }


@router.post("/api/settings/weekly_optimizer")
async def update_weekly_optimizer_config(config: dict):
    """Atualiza configuração completa de weekly optimizer."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")


    old_values = metrics_store.execute_query("SELECT * FROM weekly_optimizer_config WHERE id = 1")

    # Extrair valores das estruturas aninhadas
    schedule = config.get('schedule', {})
    veto_window = config.get('veto_window', {})
    risk_thresholds = config.get('risk_thresholds', {})
    auto_rollback = config.get('auto_rollback', {})
    api = config.get('api', {})
    analysis = config.get('analysis', {})

    metrics_store.execute("""
        UPDATE weekly_optimizer_config SET
            enabled = ?,
            analysis_day = ?,
            analysis_time = ?,
            execution_day = ?,
            execution_time = ?,
            report_day = ?,
            report_time = ?,
            veto_window_hours = ?,
            check_before_execution = ?,
            table_size_gb_medium = ?,
            table_size_gb_high = ?,
            table_size_gb_critical = ?,
            index_fragmentation_percent = ?,
            max_execution_time_minutes = ?,
            auto_rollback_enabled = ?,
            degradation_threshold_percent = ?,
            wait_after_execution_minutes = ?,
            api_enabled = ?,
            api_host = ?,
            api_port = ?,
            cors_enabled = ?,
            analysis_days = ?,
            min_occurrences = ?,
            min_avg_duration_ms = ?,
            updated_at = CURRENT_TIMESTAMP,
            updated_by = 'api_user'
        WHERE id = 1
    """, (
        config.get('enabled'),
        schedule.get('analysis_day'),
        schedule.get('analysis_time'),
        schedule.get('execution_day'),
        schedule.get('execution_time'),
        schedule.get('report_day'),
        schedule.get('report_time'),
        veto_window.get('hours'),
        veto_window.get('check_before_execution'),
        risk_thresholds.get('table_size_gb_medium'),
        risk_thresholds.get('table_size_gb_high'),
        risk_thresholds.get('table_size_gb_critical'),
        risk_thresholds.get('index_fragmentation_percent'),
        risk_thresholds.get('max_execution_time_minutes'),
        auto_rollback.get('enabled'),
        auto_rollback.get('degradation_threshold_percent'),
        auto_rollback.get('wait_after_execution_minutes'),
        api.get('enabled'),
        api.get('host'),
        api.get('port'),
        api.get('cors_enabled'),
        analysis.get('days'),
        analysis.get('min_occurrences'),
        analysis.get('min_avg_duration_ms')
    ))

    metrics_store.execute("""
        INSERT INTO config_audit_log (changed_by, config_table, config_key, old_value, new_value)
        VALUES ('api_user', 'weekly_optimizer', 'global', ?, ?)
    """, (str(old_values), json.dumps(config)))

    return {"status": "updated"}


# ========== PROMPTS LLM ==========

VALID_DB_TYPES = {"sqlserver", "hana", "postgresql"}
VALID_PROMPT_TYPES = {"base_template", "task_instructions", "features", "index_syntax"}


def _validate_db_type(db_type: str):
    """Valida db_type contra lista de tipos permitidos."""
    if db_type not in VALID_DB_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"db_type inválido: '{db_type}'. Use: {', '.join(sorted(VALID_DB_TYPES))}"
        )


def _validate_prompt_type(prompt_type: str):
    """Valida prompt_type contra lista de tipos permitidos."""
    if prompt_type not in VALID_PROMPT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"prompt_type inválido: '{prompt_type}'. Use: {', '.join(sorted(VALID_PROMPT_TYPES))}"
        )


@router.get("/api/prompts")
async def get_prompts(db_type: Optional[str] = Query(None, description="Filtrar por tipo de banco")):
    """Retorna lista de prompts LLM."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    query = """
        SELECT id, db_type, prompt_type, name, content, version, is_active,
               updated_at, updated_by
        FROM llm_prompts
        WHERE is_active = TRUE
    """
    params = []

    if db_type:
        _validate_db_type(db_type)
        query += " AND db_type = ?"
        params.append(db_type)

    query += " ORDER BY db_type, prompt_type"

    results = metrics_store.execute_query(query, tuple(params) if params else None)

    return [
        {
            "id": r[0],
            "db_type": r[1],
            "prompt_type": r[2],
            "name": r[3],
            "content": r[4],
            "version": r[5],
            "is_active": r[6],
            "updated_at": r[7],
            "updated_by": r[8]
        }
        for r in results
    ]


@router.get("/api/prompts/{db_type}/{prompt_type}")
async def get_prompt(db_type: str, prompt_type: str):
    """Retorna prompt específico ativo."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    _validate_db_type(db_type)
    _validate_prompt_type(prompt_type)

    result = metrics_store.execute_query("""
        SELECT id, db_type, prompt_type, name, content, version, is_active,
               updated_at, updated_by
        FROM llm_prompts
        WHERE db_type = ? AND prompt_type = ? AND is_active = TRUE
    """, (db_type, prompt_type))

    if not result:
        raise HTTPException(status_code=404, detail="Prompt não encontrado")

    r = result[0]
    return {
        "id": r[0],
        "db_type": r[1],
        "prompt_type": r[2],
        "name": r[3],
        "content": r[4],
        "version": r[5],
        "is_active": r[6],
        "updated_at": r[7],
        "updated_by": r[8]
    }


@router.post("/api/prompts/{db_type}/{prompt_type}")
async def save_prompt(db_type: str, prompt_type: str, data: SavePromptRequest):
    """Salva ou atualiza um prompt LLM."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    _validate_db_type(db_type)
    _validate_prompt_type(prompt_type)

    try:
        success = metrics_store.save_llm_prompt(
            db_type=db_type,
            prompt_type=prompt_type,
            name=data.name,
            content=data.content,
            updated_by=data.updated_by,
            change_reason=data.change_reason
        )

        if success:
            return {"status": "saved", "db_type": db_type, "prompt_type": prompt_type}
        else:
            raise HTTPException(status_code=500, detail="Erro ao salvar prompt")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/prompts/{db_type}/{prompt_type}/history")
async def get_prompt_history(db_type: str, prompt_type: str):
    """Retorna histórico de versões de um prompt."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    _validate_db_type(db_type)
    _validate_prompt_type(prompt_type)

    # Buscar todas as versões (incluindo inativas)
    results = metrics_store.execute_query("""
        SELECT id, version, is_active, updated_at, updated_by,
               LENGTH(content) as content_length
        FROM llm_prompts
        WHERE db_type = ? AND prompt_type = ?
        ORDER BY version DESC
    """, (db_type, prompt_type))

    history = [
        {
            "id": r[0],
            "version": r[1],
            "is_active": r[2],
            "updated_at": r[3],
            "updated_by": r[4],
            "content_length": r[5]
        }
        for r in results
    ]

    # Buscar log de mudanças
    log_results = metrics_store.execute_query("""
        SELECT h.changed_at, h.changed_by, h.change_reason, p.version
        FROM llm_prompt_history h
        JOIN llm_prompts p ON h.prompt_id = p.id
        WHERE p.db_type = ? AND p.prompt_type = ?
        ORDER BY h.changed_at DESC
        LIMIT 50
    """, (db_type, prompt_type))

    changes = [
        {
            "changed_at": r[0],
            "changed_by": r[1],
            "change_reason": r[2],
            "version": r[3]
        }
        for r in log_results
    ]

    return {
        "history": history,
        "changes": changes
    }


@router.post("/api/prompts/{db_type}/{prompt_type}/rollback/{version}")
async def rollback_prompt(db_type: str, prompt_type: str, version: int, data: RollbackPromptRequest):
    """Restaura uma versão anterior de um prompt."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    _validate_db_type(db_type)
    _validate_prompt_type(prompt_type)

    try:
        success = metrics_store.restore_prompt_by_version(
            db_type=db_type,
            prompt_type=prompt_type,
            version=version,
            restored_by=data.restored_by,
            change_reason=data.change_reason or f'Rollback para versão {version}'
        )

        if success is None:
            raise HTTPException(status_code=404, detail=f"Versão {version} não encontrada")

        if success:
            return {"status": "restored", "version": version}
        else:
            raise HTTPException(status_code=500, detail="Erro ao restaurar versão")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/api/prompts/{db_type}/{prompt_type}")
async def delete_prompt(db_type: str, prompt_type: str):
    """Desativa um prompt (não deleta fisicamente)."""
    if not metrics_store:
        raise HTTPException(status_code=503, detail="MetricsStore não inicializado")

    _validate_db_type(db_type)
    _validate_prompt_type(prompt_type)

    try:
        # Verificar se existe prompt ativo antes de desativar
        existing = metrics_store.execute_query("""
            SELECT COUNT(*) FROM llm_prompts
            WHERE db_type = ? AND prompt_type = ? AND is_active = TRUE
        """, (db_type, prompt_type))

        if not existing or existing[0][0] == 0:
            raise HTTPException(
                status_code=404,
                detail=f"Nenhum prompt ativo encontrado para {db_type}/{prompt_type}"
            )

        metrics_store.execute("""
            UPDATE llm_prompts
            SET is_active = FALSE
            WHERE db_type = ? AND prompt_type = ? AND is_active = TRUE
        """, (db_type, prompt_type))

        return {"status": "deleted", "db_type": db_type, "prompt_type": prompt_type}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
