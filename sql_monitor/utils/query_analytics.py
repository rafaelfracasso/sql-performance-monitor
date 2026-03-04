"""
API de consultas analíticas para dashboards e relatórios.

Fornece queries prontas para análise de:
- Performance histórica
- Tendências de queries problemáticas
- Alertas e incidentes
- Estatísticas de monitoramento
- ROI de otimizações
"""
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from .metrics_store import MetricsStore


class QueryAnalytics:
    """
    API de alto nível para análises e dashboards.

    Fornece queries prontas para casos de uso comuns:
    - Dashboards executivos
    - Alertas operacionais
    - Análises de tendências
    - Relatórios de performance
    """

    def __init__(self, metrics_store: MetricsStore):
        """
        Inicializa a API de analytics.

        Args:
            metrics_store: Instância do MetricsStore
        """
        self.store = metrics_store

    # ========== DASHBOARDS EXECUTIVOS ==========

    def get_executive_summary(
        self,
        hours: int = 24,
        instance_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Retorna resumo executivo de performance.

        Ideal para: Dashboards de alto nível, relatórios de status

        Args:
            hours: Período de análise (horas)
            instance_name: Filtrar por instância específica

        Returns:
            Dicionário com métricas agregadas
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        where_clause = "WHERE collected_at >= ?"
        params = [cutoff]

        if instance_name:
            where_clause += " AND instance_name = ?"
            params.append(instance_name)

        # Workload (Substituindo Queries Únicas por Tempo de Banco)
        metrics_where = where_clause  # collected_at e instance_name existem em ambas
        
        workload_stats = conn.execute(f"""
            SELECT 
                SUM(duration_ms) as total_db_time,
                AVG(duration_ms) as avg_latency,
                COUNT(*) as total_occurrences
            FROM query_metrics
            {metrics_where}
        """, params).fetchone()

        total_db_time_ms = workload_stats[0] or 0.0
        avg_latency_ms = workload_stats[1] or 0.0
        total_occurrences_count = workload_stats[2] or 0

        # Análises LLM realizadas
        analyses = conn.execute(f"""
            SELECT COUNT(*) as total_analyses,
                   AVG(analysis_duration_ms) as avg_duration_ms,
                   SUM(tokens_used) as total_tokens,
                   SUM(estimated_cost_usd) as total_cost_usd,
                   SUM(estimated_cost_brl) as total_cost_brl
            FROM llm_analyses
            WHERE analyzed_at >= ?
            {' AND instance_name = ?' if instance_name else ''}
        """, params).fetchone()

        # Alertas por severidade
        alerts = conn.execute(f"""
            SELECT severity, COUNT(*) as count
            FROM performance_alerts
            WHERE alert_time >= ?
            {' AND instance_name = ?' if instance_name else ''}
            GROUP BY severity
        """, params).fetchall()

        alerts_by_severity = {row[0]: row[1] for row in alerts}

        # Top 5 instâncias com mais queries problemáticas
        top_instances = conn.execute(f"""
            SELECT instance_name, COUNT(*) as problem_count
            FROM queries_collected
            {where_clause}
            GROUP BY instance_name
            ORDER BY problem_count DESC
            LIMIT 5
        """, params).fetchall()

        return {
            'period_hours': hours,
            'total_db_time_ms': total_db_time_ms,
            'avg_latency_ms': avg_latency_ms,
            'total_occurrences': total_occurrences_count,
            'analyses_performed': analyses[0] or 0,
            'avg_analysis_duration_ms': analyses[1] or 0,
            'total_llm_tokens': analyses[2] or 0,
            'total_estimated_cost_usd': round(analyses[3] or 0.0, 4),
            'total_estimated_cost_brl': round(analyses[4] or 0.0, 4),
            'alerts': {
                'critical': alerts_by_severity.get('critical', 0),
                'high': alerts_by_severity.get('high', 0),
                'medium': alerts_by_severity.get('medium', 0),
                'low': alerts_by_severity.get('low', 0),
                'total': sum(alerts_by_severity.values())
            },
            'top_instances': [
                {'instance': row[0], 'problem_count': row[1]}
                for row in top_instances
            ]
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
        return self.store.get_recent_alerts(
            instance_name=instance_name,
            severity=severity,
            database_name=database_name,
            table_name=table_name,
            hours=hours,
            limit=limit
        )

    def get_performance_trends(
        self,
        instance_name: Optional[str] = None,
        days: int = 7,
        granularity: str = 'day'
    ) -> List[Dict[str, Any]]:
        """
        Retorna tendências de performance ao longo do tempo.

        Ideal para: Gráficos de linha, análise temporal

        Args:
            instance_name: Filtrar por instância
            days: Período de análise (dias)
            granularity: Granularidade ('hour', 'day', 'week')

        Returns:
            Lista de pontos temporais com métricas
        """
        cutoff = datetime.now() - timedelta(days=days)
        conn = self.store._get_connection()

        # Mapear granularidade para formato de data DuckDB
        format_map = {
            'hour': '%Y-%m-%d %H:00:00',
            'day': '%Y-%m-%d',
            'week': '%Y-W%V'
        }
        date_format = format_map.get(granularity, '%Y-%m-%d')

        where_clause = "WHERE qc.collected_at >= ?"
        params = [cutoff]

        if instance_name:
            where_clause += " AND qc.instance_name = ?"
            params.append(instance_name)

        results = conn.execute(f"""
            SELECT
                strftime(qc.collected_at, '{date_format}') as time_bucket,
                COUNT(DISTINCT qc.query_hash) as unique_queries,
                COUNT(*) as total_queries,
                AVG(qm.cpu_time_ms) as avg_cpu,
                AVG(qm.duration_ms) as avg_duration,
                AVG(qm.logical_reads) as avg_reads,
                MAX(qm.cpu_time_ms) as max_cpu,
                MAX(qm.duration_ms) as max_duration
            FROM queries_collected qc
            LEFT JOIN query_metrics qm ON qc.query_hash = qm.query_hash AND qc.collected_at = qm.collected_at
            {where_clause}
            GROUP BY time_bucket
            ORDER BY time_bucket
        """, params).fetchall()

        return [
            {
                'time_bucket': row[0],
                'unique_queries': row[1],
                'total_queries': row[2],
                'avg_cpu_ms': row[3] or 0,
                'avg_duration_ms': row[4] or 0,
                'avg_logical_reads': row[5] or 0,
                'max_cpu_ms': row[6] or 0,
                'max_duration_ms': row[7] or 0
            }
            for row in results
        ]

    def get_worst_performers(
        self,
        metric: str = 'severity',
        hours: int = 24,
        limit: int = 10,
        offset: int = 0,
        instance_name: Optional[str] = None,
        database_name: Optional[str] = None,
        login_name: Optional[str] = None,
        host_name: Optional[str] = None,
        program_name: Optional[str] = None,
        severity: Optional[str] = None,
        search_text: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Retorna queries com pior performance.

        Args:
            metric: Métrica para ordenação. 'severity' (padrão) ordena por severidade desc
                    depois por cpu_time_ms. Outras opções: cpu_time_ms, duration_ms,
                    logical_reads, physical_reads, writes, wait_time_ms, memory_mb,
                    total_cpu_impact, total_duration_impact.
            hours: Período de análise em horas
            limit: Número máximo de resultados
            offset: Offset para paginação
            instance_name: Filtrar por instância específica
            database_name: Filtrar por database
            login_name: Filtrar por usuário
            host_name: Filtrar por host
            program_name: Filtrar por aplicação
            severity: Filtrar por severidade
            search_text: Busca por texto na query

        Returns:
            Dicionário com queries, total, e metadados de paginação
        """
        VALID_METRICS = {
            'severity',
            'cpu_time_ms', 'duration_ms', 'logical_reads', 'physical_reads',
            'writes', 'wait_time_ms', 'memory_mb',
            'total_cpu_impact', 'total_duration_impact',
            'execution_count'
        }
        if metric not in VALID_METRICS:
            raise ValueError(f"Metrica invalida: {metric}. Use uma de: {', '.join(VALID_METRICS)}")

        # Metricas de impacto usam SUM, as normais usam AVG para ORDER BY
        is_impact_metric = metric.startswith('total_')

        cutoff = datetime.now() - timedelta(hours=hours)
        # Período anterior para cálculo de tendência
        prev_cutoff_start = datetime.now() - timedelta(hours=hours*2)
        prev_cutoff_end = cutoff
        conn = self.store._get_connection()

        where_clauses = ["qc.collected_at >= ?"]
        params: List[Any] = [cutoff]

        if instance_name:
            where_clauses.append("qc.instance_name = ?")
            params.append(instance_name)
        if database_name:
            where_clauses.append("qc.database_name = ?")
            params.append(database_name)
        if login_name:
            where_clauses.append("qc.login_name = ?")
            params.append(login_name)
        if host_name:
            where_clauses.append("qc.host_name = ?")
            params.append(host_name)
        if program_name:
            where_clauses.append("qc.program_name LIKE ?")
            params.append(f"%{program_name}%")
        if search_text:
            where_clauses.append("(qc.query_preview ILIKE ? OR qc.query_text ILIKE ?)")
            params.append(f"%{search_text}%")
            params.append(f"%{search_text}%")

        where_clause = "WHERE " + " AND ".join(where_clauses)

        # Primeiro, conta o total para paginação
        count_query = f"""
            SELECT COUNT(DISTINCT qc.query_hash)
            FROM queries_collected qc
            {where_clause}
        """
        total_count = conn.execute(count_query, params).fetchone()[0]

        # Query principal com métricas completas, tendência e timeline
        query = f"""
            WITH current_period AS (
                SELECT
                    qc.query_hash,
                    qc.instance_name,
                    qc.database_name,
                    qc.table_name,
                    qc.query_preview,
                    qc.db_type,
                    qc.login_name,
                    qc.host_name,
                    qc.program_name,
                    AVG(qm.cpu_time_ms) as avg_cpu_time_ms,
                    AVG(qm.duration_ms) as avg_duration_ms,
                    AVG(qm.logical_reads) as avg_logical_reads,
                    AVG(qm.physical_reads) as avg_physical_reads,
                    AVG(qm.writes) as avg_writes,
                    AVG(qm.wait_time_ms) as avg_wait_time_ms,
                    AVG(qm.memory_mb) as avg_memory_mb,
                    MAX(qm.cpu_time_ms) as max_cpu_time_ms,
                    MAX(qm.duration_ms) as max_duration_ms,
                    SUM(qm.cpu_time_ms) as total_cpu_impact,
                    SUM(qm.duration_ms) as total_duration_impact,
                    COUNT(*) as occurrences,
                    MAX(qc.collected_at) as last_seen,
                    MIN(qc.collected_at) as first_seen,
                    -- Severidade do LLM
                    MAX(CASE la.severity
                        WHEN 'critical' THEN 4
                        WHEN 'high' THEN 3
                        WHEN 'medium' THEN 2
                        WHEN 'low' THEN 1
                        ELSE 0
                    END) as llm_severity_num,
                    -- Severidade dos alertas
                    (SELECT MAX(CASE pa.severity
                        WHEN 'critical' THEN 4
                        WHEN 'high' THEN 3
                        WHEN 'medium' THEN 2
                        WHEN 'low' THEN 1
                        ELSE 0
                    END) FROM performance_alerts pa WHERE pa.query_hash = qc.query_hash) as alert_severity_num,
                    COUNT(DISTINCT la.query_hash) > 0 as has_analysis
                FROM queries_collected qc
                JOIN query_metrics qm ON qc.query_hash = qm.query_hash AND qc.collected_at = qm.collected_at
                LEFT JOIN llm_analyses la ON qc.query_hash = la.query_hash
                {where_clause}
                GROUP BY
                    qc.query_hash, qc.instance_name, qc.database_name, qc.table_name,
                    qc.query_preview, qc.db_type, qc.login_name, qc.host_name, qc.program_name
            ),
            previous_period AS (
                SELECT
                    qm.query_hash,
                    AVG(qm.cpu_time_ms) as prev_avg_cpu,
                    AVG(qm.duration_ms) as prev_avg_duration,
                    AVG(qm.logical_reads) as prev_avg_reads,
                    AVG(qm.physical_reads) as prev_avg_phys_reads,
                    AVG(qm.writes) as prev_avg_writes,
                    AVG(qm.wait_time_ms) as prev_avg_wait,
                    AVG(qm.memory_mb) as prev_avg_memory,
                    SUM(qm.cpu_time_ms) as prev_total_cpu,
                    SUM(qm.duration_ms) as prev_total_duration
                FROM query_metrics qm
                WHERE qm.collected_at >= ? AND qm.collected_at < ?
                GROUP BY qm.query_hash
            ),
            baseline_7d AS (
                SELECT
                    qm.query_hash,
                    AVG(qm.cpu_time_ms) as baseline_avg_cpu,
                    AVG(qm.duration_ms) as baseline_avg_duration,
                    STDDEV_POP(qm.cpu_time_ms) as baseline_stddev_cpu,
                    STDDEV_POP(qm.duration_ms) as baseline_stddev_duration,
                    COUNT(*) as baseline_samples
                FROM query_metrics qm
                WHERE qm.collected_at >= ? AND qm.collected_at < ?
                GROUP BY qm.query_hash
                HAVING COUNT(*) >= 3
            )
            SELECT
                cp.*,
                CASE GREATEST(cp.llm_severity_num, cp.alert_severity_num)
                    WHEN 4 THEN 'critical'
                    WHEN 3 THEN 'high'
                    WHEN 2 THEN 'medium'
                    WHEN 1 THEN 'low'
                    ELSE 'medium'
                END as max_severity,
                pp.prev_avg_cpu,
                pp.prev_avg_duration,
                -- Trend percentual para CPU
                CASE
                    WHEN pp.prev_avg_cpu IS NULL OR pp.prev_avg_cpu = 0 THEN NULL
                    ELSE ROUND(((cp.avg_cpu_time_ms - pp.prev_avg_cpu) / pp.prev_avg_cpu) * 100, 1)
                END as trend_pct,
                -- Trend classification
                CASE
                    WHEN pp.prev_avg_cpu IS NULL THEN 'new'
                    WHEN pp.prev_avg_cpu = 0 THEN 'stable'
                    WHEN ((cp.avg_cpu_time_ms - pp.prev_avg_cpu) / pp.prev_avg_cpu) > 0.2 THEN 'worsening'
                    WHEN ((cp.avg_cpu_time_ms - pp.prev_avg_cpu) / pp.prev_avg_cpu) < -0.2 THEN 'improving'
                    ELSE 'stable'
                END as trend,
                -- Baseline
                bl.baseline_avg_cpu,
                bl.baseline_stddev_cpu,
                bl.baseline_samples,
                CASE
                    WHEN bl.baseline_avg_cpu IS NULL OR bl.baseline_avg_cpu = 0 THEN NULL
                    ELSE ROUND(((cp.avg_cpu_time_ms - bl.baseline_avg_cpu) / bl.baseline_avg_cpu) * 100, 1)
                END as baseline_deviation_pct,
                CASE
                    WHEN bl.baseline_avg_cpu IS NULL OR bl.baseline_avg_cpu = 0 THEN NULL
                    ELSE ROUND(cp.avg_cpu_time_ms / bl.baseline_avg_cpu, 1)
                END as baseline_multiplier
            FROM current_period cp
            LEFT JOIN previous_period pp ON cp.query_hash = pp.query_hash
            LEFT JOIN baseline_7d bl ON cp.query_hash = bl.query_hash
            {"WHERE max_severity = ?" if severity else ""}
            ORDER BY {
                "GREATEST(cp.llm_severity_num, COALESCE(cp.alert_severity_num, 0)) DESC, cp.avg_cpu_time_ms" if metric == "severity"
                else "total_cpu_impact" if metric == "total_cpu_impact"
                else "total_duration_impact" if metric == "total_duration_impact"
                else "occurrences" if metric == "execution_count"
                else f"avg_{metric}"
            } DESC
            LIMIT ? OFFSET ?
        """

        # Baseline 7d: ultimos 7 dias ate o cutoff atual
        baseline_start = datetime.now() - timedelta(days=7)
        baseline_end = cutoff

        # Adiciona parametros: previous_period + baseline_7d + paginacao
        final_params = params + [prev_cutoff_start, prev_cutoff_end, baseline_start, baseline_end]
        if severity:
            final_params.append(severity)
        final_params.extend([limit, offset])

        results = conn.execute(query, final_params).fetchall()

        # Indices das colunas do current_period CTE:
        # 0-8: query_hash, instance_name, database_name, table_name, query_preview, db_type, login_name, host_name, program_name
        # 9-15: avg_cpu_time_ms, avg_duration_ms, avg_logical_reads, avg_physical_reads, avg_writes, avg_wait_time_ms, avg_memory_mb
        # 16-17: max_cpu_time_ms, max_duration_ms
        # 18-19: total_cpu_impact, total_duration_impact
        # 20: occurrences
        # 21-22: last_seen, first_seen
        # 23-25: llm_severity_num, alert_severity_num, has_analysis
        # Colunas adicionais do SELECT final:
        # 26: max_severity
        # 27-28: prev_avg_cpu, prev_avg_duration
        # 29: trend_pct
        # 30: trend
        # 31-33: baseline_avg_cpu, baseline_stddev_cpu, baseline_samples
        # 34: baseline_deviation_pct
        # 35: baseline_multiplier
        queries = [
            {
                'query_hash': row[0],
                'instance_name': row[1],
                'database_name': row[2],
                'table_name': row[3],
                'query_preview': row[4],
                'db_type': row[5],
                'login_name': row[6],
                'host_name': row[7],
                'program_name': row[8],
                'avg_cpu_time_ms': row[9] or 0.0,
                'avg_duration_ms': row[10] or 0.0,
                'avg_logical_reads': row[11] or 0.0,
                'avg_physical_reads': row[12] or 0.0,
                'avg_writes': row[13] or 0.0,
                'avg_wait_time_ms': row[14] or 0.0,
                'avg_memory_mb': row[15] or 0.0,
                'max_cpu_time_ms': row[16] or 0.0,
                'max_duration_ms': row[17] or 0.0,
                'total_cpu_impact': row[18] or 0.0,
                'total_duration_impact': row[19] or 0.0,
                'occurrences': row[20],
                'last_seen': row[21],
                'first_seen': row[22],
                'severity': row[26],  # max_severity
                'has_analysis': bool(row[25]),
                'trend': row[30],
                'trend_pct': row[29],
                'prev_avg_cpu': row[27],
                'prev_avg_duration': row[28],
                'baseline_avg_cpu': row[31],
                'baseline_stddev_cpu': row[32],
                'baseline_samples': row[33],
                'baseline_deviation_pct': row[34],
                'baseline_multiplier': row[35]
            }
            for row in results
        ]

        return {
            'queries': queries,
            'total': total_count,
            'limit': limit,
            'offset': offset,
            'has_more': (offset + limit) < total_count
        }

    def get_chronological_queries(
        self,
        hours: int = 24,
        limit: int = 50,
        offset: int = 0,
        instance_name: Optional[str] = None,
        database_name: Optional[str] = None,
        login_name: Optional[str] = None,
        host_name: Optional[str] = None,
        program_name: Optional[str] = None,
        search_text: Optional[str] = None,
        severity: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Retorna capturas individuais de queries em ordem cronologica.

        Diferente de get_worst_performers que agrega por query_hash,
        este metodo retorna cada captura individual ordenada por data.

        Args:
            hours: Periodo de analise em horas
            limit: Numero maximo de resultados
            offset: Offset para paginacao
            instance_name: Filtrar por instancia especifica
            database_name: Filtrar por database
            login_name: Filtrar por usuario
            host_name: Filtrar por host
            program_name: Filtrar por aplicacao
            search_text: Busca por texto na query

        Returns:
            Dicionario com queries, total, e metadados de paginacao
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        where_clauses = ["qc.collected_at >= ?"]
        params: List[Any] = [cutoff]

        if instance_name:
            where_clauses.append("qc.instance_name = ?")
            params.append(instance_name)
        if database_name:
            where_clauses.append("qc.database_name = ?")
            params.append(database_name)
        if login_name:
            where_clauses.append("qc.login_name = ?")
            params.append(login_name)
        if host_name:
            where_clauses.append("qc.host_name = ?")
            params.append(host_name)
        if program_name:
            where_clauses.append("qc.program_name LIKE ?")
            params.append(f"%{program_name}%")
        if search_text:
            where_clauses.append("(qc.query_preview ILIKE ? OR qc.query_text ILIKE ?)")
            params.append(f"%{search_text}%")
            params.append(f"%{search_text}%")

        where_clause = "WHERE " + " AND ".join(where_clauses)

        # severity é filtrado após o JOIN, precisa de having/where externo
        severity_filter = "AND sev.severity = ?" if severity else ""
        if severity:
            params_with_sev = params + [severity]
        else:
            params_with_sev = params

        # Conta total para paginacao
        count_query = f"""
            SELECT COUNT(*)
            FROM queries_collected qc
            JOIN query_metrics qm ON qc.query_hash = qm.query_hash AND qc.collected_at = qm.collected_at
            LEFT JOIN (
                SELECT query_hash,
                       CASE GREATEST(
                           MAX(CASE severity WHEN 'critical' THEN 4 WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END),
                           COALESCE((SELECT MAX(CASE pa.severity WHEN 'critical' THEN 4 WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END)
                                     FROM performance_alerts pa WHERE pa.query_hash = la_inner.query_hash), 0)
                       ) WHEN 4 THEN 'critical' WHEN 3 THEN 'high' WHEN 2 THEN 'medium' WHEN 1 THEN 'low' ELSE NULL END as severity
                FROM llm_analyses la_inner GROUP BY query_hash
            ) sev ON sev.query_hash = qc.query_hash
            {where_clause}
            {severity_filter}
        """
        total_count = conn.execute(count_query, params_with_sev).fetchone()[0]

        # Query principal - cada linha e uma captura individual
        query = f"""
            SELECT
                qc.id,
                qc.query_hash,
                qc.instance_name,
                qc.database_name,
                qc.table_name,
                qc.query_preview,
                qc.db_type,
                qc.login_name,
                qc.host_name,
                qc.program_name,
                qc.collected_at,
                qm.cpu_time_ms,
                qm.duration_ms,
                qm.logical_reads,
                qm.physical_reads,
                qm.writes,
                qm.wait_time_ms,
                qm.memory_mb,
                qm.row_count,
                sev.severity,
                CASE WHEN sev.severity IS NOT NULL THEN 1 ELSE 0 END as has_analysis
            FROM queries_collected qc
            JOIN query_metrics qm ON qc.query_hash = qm.query_hash AND qc.collected_at = qm.collected_at
            LEFT JOIN (
                SELECT query_hash,
                       CASE GREATEST(
                           MAX(CASE severity WHEN 'critical' THEN 4 WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END),
                           COALESCE((SELECT MAX(CASE pa.severity WHEN 'critical' THEN 4 WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END)
                                     FROM performance_alerts pa WHERE pa.query_hash = la_inner.query_hash), 0)
                       ) WHEN 4 THEN 'critical' WHEN 3 THEN 'high' WHEN 2 THEN 'medium' WHEN 1 THEN 'low' ELSE NULL END as severity
                FROM llm_analyses la_inner GROUP BY query_hash
            ) sev ON sev.query_hash = qc.query_hash
            {where_clause}
            {severity_filter}
            ORDER BY qc.collected_at DESC
            LIMIT ? OFFSET ?
        """

        final_params = params_with_sev + [limit, offset]
        results = conn.execute(query, final_params).fetchall()

        queries = [
            {
                'id': row[0],
                'query_hash': row[1],
                'instance_name': row[2],
                'database_name': row[3],
                'table_name': row[4],
                'query_preview': row[5],
                'db_type': row[6],
                'login_name': row[7],
                'host_name': row[8],
                'program_name': row[9],
                'collected_at': row[10],
                'cpu_time_ms': row[11] or 0.0,
                'duration_ms': row[12] or 0.0,
                'logical_reads': row[13] or 0.0,
                'physical_reads': row[14] or 0.0,
                'writes': row[15] or 0.0,
                'wait_time_ms': row[16] or 0.0,
                'memory_mb': row[17] or 0.0,
                'row_count': row[18] or 0,
                'severity': row[19],
                'has_analysis': bool(row[20])
            }
            for row in results
        ]

        return {
            'queries': queries,
            'total': total_count,
            'limit': limit,
            'offset': offset,
            'has_more': (offset + limit) < total_count
        }

    def get_filter_options(self, hours: int = 24) -> Dict[str, List[str]]:
        """Retorna listas de valores únicos para filtros de UI."""
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        databases = conn.execute("""
            SELECT DISTINCT database_name
            FROM queries_collected
            WHERE collected_at >= ? AND database_name IS NOT NULL
            ORDER BY database_name
        """, [cutoff]).fetchall()

        logins = conn.execute("""
            SELECT DISTINCT login_name
            FROM queries_collected
            WHERE collected_at >= ? AND login_name IS NOT NULL
            ORDER BY login_name
        """, [cutoff]).fetchall()

        hosts = conn.execute("""
            SELECT DISTINCT host_name
            FROM queries_collected
            WHERE collected_at >= ? AND host_name IS NOT NULL
            ORDER BY host_name
        """, [cutoff]).fetchall()

        programs = conn.execute("""
            SELECT DISTINCT program_name
            FROM queries_collected
            WHERE collected_at >= ? AND program_name IS NOT NULL
            ORDER BY program_name
            LIMIT 100
        """, [cutoff]).fetchall()

        return {
            'databases': [row[0] for row in databases],
            'logins': [row[0] for row in logins],
            'hosts': [row[0] for row in hosts],
            'programs': [row[0] for row in programs]
        }

    def get_queries_timeline(self, hours: int = 24, instance_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retorna timeline de capturas de queries para visualização.

        Args:
            hours: Período de análise em horas
            instance_name: Filtrar por instância

        Returns:
            Lista de pontos temporais com contagem de queries
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        # Granularidade baseada no período
        if hours <= 24:
            date_format = '%Y-%m-%d %H:00:00'
        elif hours <= 168:  # 7 dias
            date_format = '%Y-%m-%d %H:00:00' # Mantém hora para 7d para melhor resolução
        else:
            date_format = '%Y-%m-%d'

        params = [cutoff]
        instance_filter = ""
        if instance_name:
            instance_filter = " AND instance_name = ?"
            params.append(instance_name)

        # 1. Agregar métricas por bucket
        metrics_query = f"""
            WITH metrics_bucket AS (
                SELECT
                    strftime(collected_at, '{date_format}') as time_bucket,
                    COUNT(*) as capture_count,
                    COUNT(DISTINCT query_hash) as unique_queries,
                    SUM(duration_ms) as total_duration_ms,
                    AVG(cpu_time_ms) as avg_cpu_ms,
                    AVG(duration_ms) as avg_duration_ms,
                    MAX(cpu_time_ms) as max_cpu_ms
                FROM query_metrics
                WHERE collected_at >= ? {instance_filter}
                GROUP BY 1
            ),
            alerts_bucket AS (
                SELECT
                    strftime(alert_time, '{date_format}') as time_bucket,
                    SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical_count,
                    SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) as high_count
                FROM performance_alerts
                WHERE alert_time >= ? {instance_filter}
                GROUP BY 1
            )
            SELECT
                m.time_bucket,
                m.capture_count,
                m.unique_queries,
                m.total_duration_ms,
                m.avg_cpu_ms,
                m.avg_duration_ms,
                m.max_cpu_ms,
                COALESCE(a.critical_count, 0),
                COALESCE(a.high_count, 0)
            FROM metrics_bucket m
            LEFT JOIN alerts_bucket a ON m.time_bucket = a.time_bucket
            ORDER BY m.time_bucket ASC
        """

        # Duplicar params para a subquery de alerts
        full_params = params + params

        results = conn.execute(metrics_query, full_params).fetchall()

        return [
            {
                'time': row[0],
                'captures': row[1],
                'unique_queries': row[2],
                'total_duration_ms': round(row[3] or 0, 2),
                'avg_cpu_ms': round(row[4] or 0, 2),
                'avg_duration_ms': round(row[5] or 0, 2),
                'max_cpu_ms': row[6] or 0,
                'critical_alerts': row[7],
                'high_alerts': row[8]
            }
            for row in results
        ]

    def get_queries_distribution(self, hours: int = 24, instance_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Retorna distribuição de queries para gráficos.

        Args:
            hours: Período de análise
            instance_name: Filtrar por instância

        Returns:
            Dicionário com distribuições por severidade, instância, etc.
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        where_clause = "WHERE qc.collected_at >= ?"
        params = [cutoff]

        if instance_name:
            where_clause += " AND qc.instance_name = ?"
            params.append(instance_name)

        # Distribuição por severidade (baseada em alertas)
        severity_dist = conn.execute(f"""
            SELECT
                COALESCE(pa.severity, 'none') as severity,
                COUNT(DISTINCT qc.query_hash) as count
            FROM queries_collected qc
            LEFT JOIN performance_alerts pa ON qc.query_hash = pa.query_hash
            {where_clause}
            GROUP BY severity
            ORDER BY
                CASE severity
                    WHEN 'critical' THEN 1
                    WHEN 'high' THEN 2
                    WHEN 'medium' THEN 3
                    WHEN 'low' THEN 4
                    ELSE 5
                END
        """, params).fetchall()

        # Distribuição por instância
        instance_dist = conn.execute(f"""
            SELECT
                qc.instance_name,
                COUNT(DISTINCT qc.query_hash) as query_count,
                AVG(qm.cpu_time_ms) as avg_cpu_ms
            FROM queries_collected qc
            JOIN query_metrics qm ON qc.query_hash = qm.query_hash AND qc.collected_at = qm.collected_at
            {where_clause}
            GROUP BY qc.instance_name
            ORDER BY query_count DESC, avg_cpu_ms DESC, qc.instance_name
            LIMIT 10
        """, params).fetchall()

        # Uma unica CTE calcula todos os AVGs — sem 6 table scans repetidos
        all_metrics_rows = conn.execute(f"""
            WITH aggregated AS (
                SELECT
                    qc.query_hash,
                    qc.query_preview,
                    AVG(qm.cpu_time_ms)    AS avg_cpu,
                    AVG(qm.duration_ms)    AS avg_duration,
                    AVG(qm.logical_reads)  AS avg_reads,
                    AVG(qm.writes)         AS avg_writes,
                    AVG(qm.wait_time_ms)   AS avg_wait,
                    AVG(qm.memory_mb)      AS avg_memory,
                    COUNT(DISTINCT qc.id)  AS occurrences
                FROM queries_collected qc
                JOIN query_metrics qm ON qc.query_hash = qm.query_hash AND qc.collected_at = qm.collected_at
                {where_clause}
                GROUP BY qc.query_hash, qc.query_preview
            )
            SELECT query_hash, query_preview, avg_cpu, avg_duration,
                   avg_reads, avg_writes, avg_wait, avg_memory, occurrences
            FROM aggregated
        """, params).fetchall()

        def _preview(text):
            if not text:
                return ''
            return text[:50] + '...' if len(text) > 50 else text

        def _top10(rows, key_idx):
            sorted_rows = sorted(rows, key=lambda r: r[key_idx] or 0, reverse=True)[:10]
            return [
                {
                    'query_hash': r[0],
                    'preview': _preview(r[1]),
                    'value': round(r[key_idx] or 0, 2),
                    'occurrences': r[8]
                }
                for r in sorted_rows
            ]

        top_cpu      = _top10(all_metrics_rows, 2)
        top_duration = _top10(all_metrics_rows, 3)
        top_reads    = _top10(all_metrics_rows, 4)
        top_writes   = _top10(all_metrics_rows, 5)
        top_wait     = _top10(all_metrics_rows, 6)
        top_memory   = _top10(all_metrics_rows, 7)

        return {
            'by_severity': [
                {'severity': row[0], 'count': row[1]}
                for row in severity_dist
            ],
            'by_instance': [
                {'instance': row[0], 'count': row[1], 'avg_cpu_ms': round(row[2] or 0, 2)}
                for row in instance_dist
            ],
            'top_cpu_consumers': top_cpu,
            'top_duration_consumers': top_duration,
            'top_reads_consumers': top_reads,
            'top_writes_consumers': top_writes,
            'top_wait_consumers': top_wait,
            'top_memory_consumers': top_memory
        }

    @staticmethod
    def _truncate_text(text: str, max_length: int) -> str:
        """Trunca texto adicionando reticências se necessário."""
        return text if len(text) <= max_length else text[:max_length] + '...'

    def get_alert_hotspots(
        self,
        hours: int = 24,
        min_alerts: int = 3,
        instance_name: Optional[str] = None,
        database_name: Optional[str] = None,
        severity: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Identifica tabelas que geram mais alertas (hotspots).

        Args:
            hours: Período de análise em horas
            min_alerts: Mínimo de alertas para considerar hotspot
            instance_name: Filtrar por instância
            database_name: Filtrar por database
            severity: Filtrar por severidade

        Returns:
            Lista de hotspots ordenados por total de alertas
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        where_clauses = ["pa.alert_time >= ?"]
        params: List[Any] = [cutoff]

        if instance_name:
            where_clauses.append("pa.instance_name = ?")
            params.append(instance_name)
        
        if database_name:
            where_clauses.append("pa.database_name = ?")
            params.append(database_name)

        if severity:
            where_clauses.append("pa.severity = ?")
            params.append(severity)

        where_sql = " AND ".join(where_clauses)

        query = f"""
            WITH enriched AS (
                SELECT
                    pa.instance_name,
                    pa.database_name,
                    pa.severity,
                    pa.alert_time,
                    pa.alert_type,
                    pa.query_hash,
                    COALESCE(
                        NULLIF(pa.table_name, ''),
                        (
                            SELECT pa2.table_name
                            FROM performance_alerts pa2
                            WHERE pa2.query_hash = pa.query_hash
                              AND pa2.query_hash IS NOT NULL
                              AND pa2.table_name IS NOT NULL
                              AND pa2.table_name != ''
                            ORDER BY pa2.alert_time DESC
                            LIMIT 1
                        )
                    ) AS table_name
                FROM performance_alerts pa
                WHERE {where_sql}
            )
            SELECT
                instance_name,
                database_name,
                table_name,
                COUNT(*) as total_alerts,
                COUNT(*) FILTER (WHERE severity = 'critical') as critical_count,
                COUNT(*) FILTER (WHERE severity = 'high') as high_count,
                COUNT(*) FILTER (WHERE severity = 'medium') as medium_count,
                COUNT(*) FILTER (WHERE severity = 'low') as low_count,
                COUNT(DISTINCT query_hash) as affected_queries,
                MAX(alert_time) as last_alert,
                STRING_AGG(DISTINCT alert_type, ', ') as alert_types
            FROM enriched
            WHERE table_name IS NOT NULL AND table_name != ''
            GROUP BY instance_name, database_name, table_name
            HAVING total_alerts >= ?
            ORDER BY total_alerts DESC, critical_count DESC, high_count DESC, instance_name, table_name
        """

        results = conn.execute(query, params + [min_alerts]).fetchall()

        return [
            {
                'instance_name': row[0],
                'database_name': row[1],
                'table_name': row[2],
                'total_alerts': row[3],
                'critical_count': row[4],
                'high_count': row[5],
                'medium_count': row[6],
                'low_count': row[7],
                'affected_queries': row[8],
                'last_alert': row[9],
                'alert_types': row[10]
            }
            for row in results
        ]

    def get_cache_efficiency(
        self,
        instance_name: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, Any]:
        """
        Analisa eficiência do cache de análises LLM.

        Ideal para: Otimização de custos, ajuste de TTL

        Args:
            instance_name: Filtrar por instância
            hours: Período de análise

        Returns:
            Métricas de cache hit rate e economia
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        where_clause = "WHERE mc.cycle_started_at >= ?"
        params = [cutoff]

        if instance_name:
            where_clause += " AND mc.instance_name = ?"
            params.append(instance_name)

        result = conn.execute(f"""
            SELECT
                SUM(mc.queries_found) as total_queries,
                SUM(mc.queries_analyzed) as new_analyses,
                SUM(mc.cache_hits) as cache_hits,
                AVG(mc.cycle_duration_ms) as avg_cycle_ms
            FROM monitoring_cycles mc
            {where_clause}
        """, params).fetchone()

        total_queries = result[0] or 0
        new_analyses = result[1] or 0
        cache_hits = result[2] or 0

        # Calcular custos estimados (baseado em tokens Gemini)
        # Assumindo ~500 tokens por análise e $0.15 por 1M tokens (Gemini Flash)
        avg_tokens_per_analysis = 500
        cost_per_million_tokens = 0.15
        tokens_saved = cache_hits * avg_tokens_per_analysis
        cost_saved = (tokens_saved / 1_000_000) * cost_per_million_tokens

        total_attempts = cache_hits + new_analyses
        cache_hit_rate = (cache_hits / total_attempts * 100) if total_attempts > 0 else 0

        return {
            'period_hours': hours,
            'total_queries': total_queries,
            'new_analyses': new_analyses,
            'cache_hits': cache_hits,
            'cache_hit_rate_percent': round(cache_hit_rate, 2),
            'avg_cycle_duration_ms': result[3] or 0,
            'estimated_tokens_saved': tokens_saved,
            'estimated_cost_saved_usd': round(cost_saved, 4)
        }

    def get_table_analysis_history(
        self,
        database_name: str,
        schema_name: str,
        table_name: str,
        days: int = 7
    ) -> Dict[str, Any]:
        """
        Retorna histórico de análises de uma tabela específica.

        Ideal para: Investigação aprofundada de tabela problemática

        Args:
            database_name: Nome do database
            schema_name: Nome do schema
            table_name: Nome da tabela
            days: Período de análise

        Returns:
            Histórico completo da tabela
        """
        cutoff = datetime.now() - timedelta(days=days)
        conn = self.store._get_connection()

        # Queries problemáticas
        queries = conn.execute("""
            SELECT
                qc.query_hash,
                qc.query_preview,
                qc.collected_at,
                qm.cpu_time_ms,
                qm.duration_ms,
                qm.logical_reads,
                la.severity,
                la.analysis_text
            FROM queries_collected qc
            LEFT JOIN query_metrics qm ON qc.query_hash = qm.query_hash AND qc.collected_at = qm.collected_at
            LEFT JOIN llm_analyses la ON qc.query_hash = la.query_hash
            WHERE qc.database_name = ?
              AND qc.schema_name = ?
              AND qc.table_name = ?
              AND qc.collected_at >= ?
            ORDER BY qc.collected_at DESC
        """, [database_name, schema_name, table_name, cutoff]).fetchall()

        # Alertas
        alerts = conn.execute("""
            SELECT alert_time, alert_type, severity, actual_value
            FROM performance_alerts
            WHERE database_name = ?
              AND table_name = ?
              AND alert_time >= ?
            ORDER BY alert_time DESC
        """, [database_name, table_name, cutoff]).fetchall()

        # Metadados (último snapshot)
        metadata = conn.execute("""
            SELECT columns_json, indexes_json, missing_indexes_json, row_count, total_size_mb
            FROM table_metadata
            WHERE database_name = ?
              AND schema_name = ?
              AND table_name = ?
            ORDER BY captured_at DESC
            LIMIT 1
        """, [database_name, schema_name, table_name]).fetchone()

        return {
            'database': database_name,
            'schema': schema_name,
            'table': table_name,
            'period_days': days,
            'problem_queries': [
                {
                    'query_hash': q[0],
                    'query_preview': q[1],
                    'collected_at': q[2],
                    'cpu_time_ms': q[3],
                    'duration_ms': q[4],
                    'logical_reads': q[5],
                    'severity': q[6],
                    'has_analysis': bool(q[7])
                }
                for q in queries
            ],
            'alerts': [
                {
                    'alert_time': a[0],
                    'alert_type': a[1],
                    'severity': a[2],
                    'actual_value': a[3]
                }
                for a in alerts
            ],
            'latest_metadata': {
                'columns_json': metadata[0] if metadata else None,
                'indexes_json': metadata[1] if metadata else None,
                'missing_indexes_json': metadata[2] if metadata else None,
                'row_count': metadata[3] if metadata else None,
                'total_size_mb': metadata[4] if metadata else None
            } if metadata else None
        }

    def get_top_problematic_applications(self, hours: int = 24, limit: int = 50, instance_name: Optional[str] = None) -> List[Dict]:
        """Top aplicações com mais queries lentas."""
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        where_clauses = ["qc.collected_at >= ?", "qc.program_name IS NOT NULL"]
        params: List[Any] = [cutoff]
        if instance_name:
            where_clauses.append("qc.instance_name = ?")
            params.append(instance_name)

        where_clause = "WHERE " + " AND ".join(where_clauses)

        results = conn.execute(f"""
            SELECT
                qc.program_name,
                COUNT(DISTINCT qc.query_hash) as unique_queries,
                COUNT(DISTINCT qc.id) as total_occurrences,
                AVG(qm.cpu_time_ms) as avg_cpu,
                MAX(qm.cpu_time_ms) as max_cpu,
                COUNT(DISTINCT pa.id) as critical_alerts
            FROM queries_collected qc
            JOIN query_metrics qm ON qc.query_hash = qm.query_hash AND qc.collected_at = qm.collected_at
            LEFT JOIN performance_alerts pa ON qc.query_hash = pa.query_hash AND pa.severity = 'critical'
            {where_clause}
            GROUP BY qc.program_name
            ORDER BY avg_cpu DESC, critical_alerts DESC, unique_queries DESC, qc.program_name
            LIMIT ?
        """, params + [limit]).fetchall()

        return [
            {
                'program_name': r[0],
                'unique_queries': r[1],
                'total_occurrences': r[2],
                'avg_cpu_time_ms': r[3] or 0.0,
                'max_cpu_time_ms': r[4] or 0.0,
                'critical_alerts': r[5] or 0
            }
            for r in results
        ]

    def get_top_problematic_users(self, hours: int = 24, limit: int = 50, instance_name: Optional[str] = None) -> List[Dict]:
        """Top usuários com mais queries lentas."""
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        where_clauses = ["qc.collected_at >= ?", "qc.login_name IS NOT NULL"]
        params: List[Any] = [cutoff]
        if instance_name:
            where_clauses.append("qc.instance_name = ?")
            params.append(instance_name)

        where_clause = "WHERE " + " AND ".join(where_clauses)

        results = conn.execute(f"""
            SELECT
                qc.login_name,
                COUNT(DISTINCT qc.query_hash) as unique_queries,
                COUNT(DISTINCT qc.id) as total_occurrences,
                AVG(qm.cpu_time_ms) as avg_cpu,
                MAX(qm.cpu_time_ms) as max_cpu,
                COUNT(DISTINCT pa.id) as critical_alerts
            FROM queries_collected qc
            JOIN query_metrics qm ON qc.query_hash = qm.query_hash AND qc.collected_at = qm.collected_at
            LEFT JOIN performance_alerts pa ON qc.query_hash = pa.query_hash AND pa.severity = 'critical'
            {where_clause}
            GROUP BY qc.login_name
            ORDER BY avg_cpu DESC, critical_alerts DESC, unique_queries DESC, qc.login_name
            LIMIT ?
        """, params + [limit]).fetchall()

        return [
            {
                'login_name': r[0],
                'unique_queries': r[1],
                'total_occurrences': r[2],
                'avg_cpu_time_ms': r[3] or 0.0,
                'max_cpu_time_ms': r[4] or 0.0,
                'critical_alerts': r[5] or 0
            }
            for r in results
        ]

    def get_top_problematic_hosts(self, hours: int = 24, limit: int = 50, instance_name: Optional[str] = None) -> List[Dict]:
        """Top hosts com mais queries lentas."""
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        where_clauses = ["qc.collected_at >= ?", "qc.host_name IS NOT NULL"]
        params: List[Any] = [cutoff]
        if instance_name:
            where_clauses.append("qc.instance_name = ?")
            params.append(instance_name)

        where_clause = "WHERE " + " AND ".join(where_clauses)

        results = conn.execute(f"""
            SELECT
                qc.host_name,
                COUNT(DISTINCT qc.query_hash) as unique_queries,
                COUNT(DISTINCT qc.id) as total_occurrences,
                AVG(qm.cpu_time_ms) as avg_cpu,
                MAX(qm.cpu_time_ms) as max_cpu,
                COUNT(DISTINCT pa.id) as critical_alerts
            FROM queries_collected qc
            JOIN query_metrics qm ON qc.query_hash = qm.query_hash AND qc.collected_at = qm.collected_at
            LEFT JOIN performance_alerts pa ON qc.query_hash = pa.query_hash AND pa.severity = 'critical'
            {where_clause}
            GROUP BY qc.host_name
            ORDER BY avg_cpu DESC, critical_alerts DESC, unique_queries DESC, qc.host_name
            LIMIT ?
        """, params + [limit]).fetchall()

        return [
            {
                'host_name': r[0],
                'unique_queries': r[1],
                'total_occurrences': r[2],
                'avg_cpu_time_ms': r[3] or 0.0,
                'max_cpu_time_ms': r[4] or 0.0,
                'critical_alerts': r[5] or 0
            }
            for r in results
        ]

    def get_monitoring_health(self, hours: int = 24) -> Dict[str, Any]:
        """
        Retorna métricas de saúde do sistema de monitoramento.

        Args:
            hours: Período de análise em horas

        Returns:
            Dicionário com estatísticas de saúde do monitoramento
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        cycles_stats = self._get_cycles_statistics(conn, cutoff)
        instances_stats = self._get_instances_statistics(conn, cutoff)
        error_stats = self._get_error_statistics(conn, cutoff)
        llm_stats = self._get_llm_statistics(conn, cutoff)
        alerts_by_instance = self._get_alerts_by_instance(conn, cutoff)

        total_cycles = cycles_stats[0] or 0
        successful_cycles = cycles_stats[1] or 0
        failed_cycles = cycles_stats[2] or 0
        success_rate = (successful_cycles / total_cycles * 100) if total_cycles > 0 else 0.0
        error_rate = (failed_cycles / total_cycles * 100) if total_cycles > 0 else 0.0

        # Converter alertas para dicionario por instancia
        alerts_dict = {
            row[0]: {
                'critical': row[1] or 0,
                'high': row[2] or 0,
                'medium': row[3] or 0,
                'low': row[4] or 0
            }
            for row in alerts_by_instance
        }

        def build_instance_data(inst):
            inst_name = inst[0]
            inst_alerts = alerts_dict.get(inst_name, {'critical': 0, 'high': 0, 'medium': 0, 'low': 0})
            return {
                'name': inst_name,
                'type': inst[1],
                'queries_found': inst[2] or 0,
                'queries_analyzed': inst[3] or 0,
                'cache_hits': inst[4] or 0,
                'errors': inst[5] or 0,
                'total_cycles': inst[6] or 0,
                'avg_cycle_duration_ms': round(inst[7] or 0.0, 2),
                'last_cycle': inst[8],
                'alerts': inst_alerts,
                'health_score': self._calculate_health_score(
                    inst[2], inst[5], inst[6],
                    inst_alerts['critical'],
                    inst_alerts['high'],
                    inst_alerts['medium'],
                    inst_alerts['low']
                )
            }

        return {
            'period_hours': hours,
            'total_cycles': total_cycles,
            'successful_cycles': successful_cycles,
            'failed_cycles': failed_cycles,
            'success_rate_percent': round(success_rate, 2),
            'error_rate_percent': round(error_rate, 2),
            'avg_cycle_duration_ms': cycles_stats[3] or 0.0,
            'max_cycle_duration_ms': cycles_stats[4] or 0.0,
            'first_cycle': cycles_stats[5],
            'last_cycle': cycles_stats[6],
            'total_analyses': llm_stats[0] or 0,
            'total_cache_hits': sum(inst[4] for inst in instances_stats),
            'total_tokens': llm_stats[1] or 0,
            'active_instances': [build_instance_data(inst) for inst in instances_stats],
            'instances_with_errors': [
                {
                    'instance': err[0],
                    'total_errors': err[1],
                    'total_queries': err[2],
                    'error_rate_percent': round(err[3], 2) if err[3] else 0.0
                }
                for err in error_stats
            ]
        }

    @staticmethod
    def _get_cycles_statistics(conn, cutoff: datetime):
        """Retorna estatísticas de ciclos de monitoramento."""
        return conn.execute("""
            SELECT
                COUNT(*) as total_cycles,
                COUNT(*) FILTER (WHERE status = 'completed') as successful,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                AVG(cycle_duration_ms) as avg_duration_ms,
                MAX(cycle_duration_ms) as max_duration_ms,
                MIN(cycle_started_at) as first_cycle,
                MAX(cycle_ended_at) as last_cycle
            FROM monitoring_cycles
            WHERE cycle_started_at >= ?
        """, [cutoff]).fetchone()

    @staticmethod
    def _get_instances_statistics(conn, cutoff: datetime):
        """Retorna estatísticas por instância."""
        return conn.execute("""
            SELECT
                mc.instance_name,
                mc.db_type,
                SUM(mc.queries_found) as queries_found,
                SUM(mc.queries_analyzed) as queries_analyzed,
                SUM(mc.cache_hits) as cache_hits,
                SUM(mc.errors) as errors,
                COUNT(*) as total_cycles,
                AVG(mc.cycle_duration_ms) as avg_cycle_duration_ms,
                MAX(mc.cycle_ended_at) as last_cycle
            FROM monitoring_cycles mc
            WHERE mc.cycle_started_at >= ?
            GROUP BY mc.instance_name, mc.db_type
            ORDER BY mc.instance_name
        """, [cutoff]).fetchall()

    @staticmethod
    def _get_error_statistics(conn, cutoff: datetime):
        """Retorna estatisticas de erros por instancia."""
        return conn.execute("""
            SELECT
                instance_name,
                SUM(errors) as total_errors,
                SUM(queries_found) as total_queries,
                (SUM(errors)::FLOAT / NULLIF(SUM(queries_found), 0) * 100) as error_rate_percent
            FROM monitoring_cycles
            WHERE cycle_started_at >= ?
            GROUP BY instance_name
            HAVING total_errors > 0
            ORDER BY error_rate_percent DESC
        """, [cutoff]).fetchall()

    @staticmethod
    def _get_alerts_by_instance(conn, cutoff: datetime):
        """Retorna contagem de alertas por instancia e severidade."""
        return conn.execute("""
            SELECT
                instance_name,
                COUNT(*) FILTER (WHERE severity = 'critical') as critical_count,
                COUNT(*) FILTER (WHERE severity = 'high') as high_count,
                COUNT(*) FILTER (WHERE severity = 'medium') as medium_count,
                COUNT(*) FILTER (WHERE severity = 'low') as low_count
            FROM performance_alerts
            WHERE alert_time >= ?
            GROUP BY instance_name
        """, [cutoff]).fetchall()

    @staticmethod
    def _get_llm_statistics(conn, cutoff: datetime):
        """Retorna estatísticas de análises LLM."""
        return conn.execute("""
            SELECT
                COUNT(*) as total_analyses,
                SUM(tokens_used) as total_tokens,
                SUM(estimated_cost_usd) as total_cost
            FROM llm_analyses
            WHERE analyzed_at >= ?
        """, [cutoff]).fetchone()

    @staticmethod
    def _calculate_health_score(
        queries_found: int,
        errors: int,
        total_cycles: int,
        critical_alerts: int = 0,
        high_alerts: int = 0,
        medium_alerts: int = 0,
        low_alerts: int = 0
    ) -> float:
        """
        Calcula score de saude da instancia (0-100).
        Considera erros de monitoramento E alertas de performance.

        Penalidades por alertas:
        - Critical: -15 pontos cada
        - High: -8 pontos cada
        - Medium: -3 pontos cada
        - Low: -1 ponto cada
        """
        score = 100.0

        # Penalizacao por erros de monitoramento
        if total_cycles > 0 and errors > 0:
            if queries_found > 0:
                error_rate = (errors / queries_found * 100)
                score -= (error_rate * 5.0)
            else:
                score -= (errors * 20.0)

        # Penalizacao por alertas de performance
        alert_penalty = (
            critical_alerts * 15 +
            high_alerts * 8 +
            medium_alerts * 3 +
            low_alerts * 1
        )
        score -= alert_penalty

        return round(max(0.0, score), 1)

    def get_recommendation_summary(
        self,
        hours: int = 24,
        severity: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Retorna resumo de recomendações LLM por tipo de problema.

        Ideal para: Priorização de ações corretivas

        Args:
            hours: Período de análise
            severity: Filtrar por severidade

        Returns:
            Lista de recomendações agrupadas
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        where_clause = "WHERE la.analyzed_at >= ?"
        params = [cutoff]

        if severity:
            where_clause += " AND la.severity = ?"
            params.append(severity)

        results = conn.execute(f"""
            SELECT
                la.severity,
                la.instance_name,
                la.table_name,
                la.analysis_text,
                la.recommendations,
                la.analyzed_at
            FROM llm_analyses la
            {where_clause}
            ORDER BY
                CASE la.severity
                    WHEN 'critical' THEN 1
                    WHEN 'high' THEN 2
                    WHEN 'medium' THEN 3
                    WHEN 'low' THEN 4
                    ELSE 5
                END,
                la.analyzed_at DESC
        """, params).fetchall()

        return [
            {
                'severity': row[0],
                'instance_name': row[1],
                'table_name': row[2],
                'analysis_text': row[3],
                'recommendations': row[4],
                'analyzed_at': row[5]
            }
            for row in results
        ]

    def get_llm_usage_stats(self, days: int = 30) -> Dict[str, Any]:
        """
        Retorna estatísticas de uso do LLM.

        Args:
            days: Período de análise em dias.

        Returns:
            Dict com estatísticas de uso, custo e latência.
        """
        cutoff = datetime.now() - timedelta(days=days)
        conn = self.store._get_connection()

        # Totais
        totals = conn.execute("""
            SELECT
                COUNT(*) as total_analyses,
                SUM(tokens_used) as total_tokens,
                SUM(prompt_tokens) as total_prompt_tokens,
                SUM(completion_tokens) as total_completion_tokens,
                SUM(estimated_cost_usd) as total_cost_usd,
                SUM(estimated_cost_brl) as total_cost_brl,
                AVG(analysis_duration_ms) as avg_latency_ms
            FROM llm_analyses
            WHERE analyzed_at >= ?
        """, [cutoff]).fetchone()

        # Por Modelo
        by_model = conn.execute("""
            SELECT
                model_used,
                COUNT(*) as count,
                SUM(tokens_used) as tokens,
                SUM(estimated_cost_usd) as cost_usd,
                AVG(analysis_duration_ms) as avg_latency
            FROM llm_analyses
            WHERE analyzed_at >= ?
            GROUP BY model_used
            ORDER BY count DESC
        """, [cutoff]).fetchall()

        # Por Dia
        by_day = conn.execute("""
            SELECT
                strftime(analyzed_at, '%Y-%m-%d') as day,
                COUNT(*) as count,
                SUM(tokens_used) as tokens,
                SUM(estimated_cost_usd) as cost_usd
            FROM llm_analyses
            WHERE analyzed_at >= ?
            GROUP BY day
            ORDER BY day
        """, [cutoff]).fetchall()

        return {
            'totals': {
                'analyses': totals[0] or 0,
                'tokens': totals[1] or 0,
                'prompt_tokens': totals[2] or 0,
                'completion_tokens': totals[3] or 0,
                'cost_usd': totals[4] or 0.0,
                'cost_brl': totals[5] or 0.0,
                'avg_latency_ms': totals[6] or 0.0
            },
            'by_model': [
                {
                    'model': row[0],
                    'count': row[1],
                    'tokens': row[2],
                    'cost_usd': row[3],
                    'avg_latency': row[4]
                } for row in by_model
            ],
            'by_day': [
                {
                    'day': row[0],
                    'count': row[1],
                    'tokens': row[2],
                    'cost_usd': row[3]
                } for row in by_day
            ]
        }
