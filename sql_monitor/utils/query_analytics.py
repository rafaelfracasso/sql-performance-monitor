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

        # Queries coletadas
        total_queries = conn.execute(f"""
            SELECT COUNT(DISTINCT query_hash) as unique_queries,
                   COUNT(*) as total_occurrences
            FROM queries_collected
            {where_clause}
        """, params).fetchone()

        # Análises LLM realizadas
        analyses = conn.execute(f"""
            SELECT COUNT(*) as total_analyses,
                   AVG(analysis_duration_ms) as avg_duration_ms,
                   SUM(tokens_used) as total_tokens
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
            'unique_queries': total_queries[0] or 0,
            'total_occurrences': total_queries[1] or 0,
            'analyses_performed': analyses[0] or 0,
            'avg_analysis_duration_ms': analyses[1] or 0,
            'total_llm_tokens': analyses[2] or 0,
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
            LEFT JOIN query_metrics qm ON qc.query_hash = qm.query_hash
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
        metric: str = 'cpu_time_ms',
        hours: int = 24,
        limit: int = 10,
        instance_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Retorna queries com pior performance por métrica específica.

        Ideal para: Listas de problemas prioritários

        Args:
            metric: Métrica a ordenar (cpu_time_ms, duration_ms, logical_reads)
            hours: Período de análise
            limit: Número de resultados
            instance_name: Filtrar por instância

        Returns:
            Lista de queries ordenadas pela métrica
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        # Validar métrica
        valid_metrics = ['cpu_time_ms', 'duration_ms', 'logical_reads', 'physical_reads', 'writes']
        if metric not in valid_metrics:
            metric = 'cpu_time_ms'

        where_clause = "WHERE qc.collected_at >= ?"
        params = [cutoff]

        if instance_name:
            where_clause += " AND qc.instance_name = ?"
            params.append(instance_name)

        results = conn.execute(f"""
            SELECT
                qc.query_hash,
                qc.instance_name,
                qc.database_name,
                qc.table_name,
                qc.query_preview,
                AVG(qm.{metric}) as avg_metric,
                MAX(qm.{metric}) as max_metric,
                COUNT(*) as occurrences,
                la.severity,
                la.recommendations
            FROM queries_collected qc
            JOIN query_metrics qm ON qc.query_hash = qm.query_hash
            LEFT JOIN llm_analyses la ON qc.query_hash = la.query_hash
            {where_clause}
            GROUP BY qc.query_hash, qc.instance_name, qc.database_name, qc.table_name,
                     qc.query_preview, la.severity, la.recommendations
            ORDER BY avg_metric DESC
            LIMIT ?
        """, params + [limit]).fetchall()

        return [
            {
                'query_hash': row[0],
                'instance_name': row[1],
                'database_name': row[2],
                'table_name': row[3],
                'query_preview': row[4],
                f'avg_{metric}': row[5] or 0,
                f'max_{metric}': row[6] or 0,
                'occurrences': row[7],
                'severity': row[8],
                'has_analysis': bool(row[9]),
                'recommendations_preview': (row[9][:100] + '...') if row[9] and len(row[9]) > 100 else row[9]
            }
            for row in results
        ]

    def get_alert_hotspots(
        self,
        hours: int = 24,
        min_alerts: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Identifica tabelas/queries que geram mais alertas.

        Ideal para: Identificar pontos críticos que precisam atenção

        Args:
            hours: Período de análise
            min_alerts: Mínimo de alertas para considerar hotspot

        Returns:
            Lista de hotspots ordenados por número de alertas
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        results = conn.execute("""
            SELECT
                pa.instance_name,
                pa.database_name,
                pa.table_name,
                COUNT(*) as alert_count,
                COUNT(DISTINCT pa.query_hash) as affected_queries,
                MAX(pa.alert_time) as last_alert,
                STRING_AGG(DISTINCT pa.alert_type, ', ') as alert_types
            FROM performance_alerts pa
            WHERE pa.alert_time >= ?
            GROUP BY pa.instance_name, pa.database_name, pa.table_name
            HAVING alert_count >= ?
            ORDER BY alert_count DESC
        """, [cutoff, min_alerts]).fetchall()

        return [
            {
                'instance_name': row[0],
                'database_name': row[1],
                'table_name': row[2],
                'alert_count': row[3],
                'affected_queries': row[4],
                'last_alert': row[5],
                'alert_types': row[6]
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

        cache_hit_rate = (cache_hits / total_queries * 100) if total_queries > 0 else 0

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
            LEFT JOIN query_metrics qm ON qc.query_hash = qm.query_hash
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

    def get_monitoring_health(
        self,
        hours: int = 24
    ) -> Dict[str, Any]:
        """
        Retorna saúde do sistema de monitoramento.

        Ideal para: Ops/SRE monitorar o próprio monitor

        Args:
            hours: Período de análise

        Returns:
            Métricas de saúde do monitor
        """
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = self.store._get_connection()

        # Estatísticas de ciclos
        cycles = conn.execute("""
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

        # Instâncias ativas
        instances = conn.execute("""
            SELECT DISTINCT instance_name, db_type
            FROM monitoring_cycles
            WHERE cycle_started_at >= ?
            ORDER BY instance_name
        """, [cutoff]).fetchall()

        # Taxa de erro por instância
        error_rates = conn.execute("""
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

        success_rate = (cycles[1] / cycles[0] * 100) if cycles[0] > 0 else 0

        return {
            'period_hours': hours,
            'total_cycles': cycles[0] or 0,
            'successful_cycles': cycles[1] or 0,
            'failed_cycles': cycles[2] or 0,
            'success_rate_percent': round(success_rate, 2),
            'avg_cycle_duration_ms': cycles[3] or 0,
            'max_cycle_duration_ms': cycles[4] or 0,
            'first_cycle': cycles[5],
            'last_cycle': cycles[6],
            'active_instances': [
                {'name': inst[0], 'type': inst[1]}
                for inst in instances
            ],
            'instances_with_errors': [
                {
                    'instance': err[0],
                    'total_errors': err[1],
                    'total_queries': err[2],
                    'error_rate_percent': round(err[3], 2) if err[3] else 0
                }
                for err in error_rates
            ]
        }

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
                'analysis_preview': (row[3][:200] + '...') if row[3] and len(row[3]) > 200 else row[3],
                'recommendations': row[4],
                'analyzed_at': row[5]
            }
            for row in results
        ]
