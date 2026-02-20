"""
Calcula baselines dinamicas para queries e detecta desvios.

Usa rolling window de 7 dias para calcular AVG, STDDEV e P95,
gerando alertas quando metricas atuais desviam significativamente.
"""
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any


class BaselineCalculator:
    """
    Calcula baselines de performance de queries e detecta desvios.

    Usa cache em memoria com TTL para evitar recalcular baseline a cada query.
    """

    def __init__(self, metrics_store, cache_ttl_seconds: int = 3600):
        """
        Args:
            metrics_store: Instancia do MetricsStore
            cache_ttl_seconds: TTL do cache em segundos (padrao: 1h)
        """
        self.store = metrics_store
        self.cache_ttl = cache_ttl_seconds
        self._cache: Dict[str, Dict] = {}

    def _cache_key(self, query_hash: str, instance_name: str) -> str:
        return f"{query_hash}:{instance_name}"

    def _is_cache_valid(self, key: str) -> bool:
        entry = self._cache.get(key)
        if not entry:
            return False
        return (time.time() - entry['timestamp']) < self.cache_ttl

    def get_query_baseline(
        self,
        query_hash: str,
        instance_name: str,
        days: int = 7
    ) -> Optional[Dict[str, Any]]:
        """
        Calcula baseline de performance para uma query.

        Args:
            query_hash: Hash da query
            instance_name: Nome da instancia
            days: Janela de baseline em dias

        Returns:
            Dicionario com AVG, STDDEV, P95 por metrica, ou None se insuficiente
        """
        cache_key = self._cache_key(query_hash, instance_name)
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]['data']

        cutoff = datetime.now() - timedelta(days=days)
        conn = self.store._get_connection()

        result = conn.execute("""
            SELECT
                COUNT(*) as samples,
                AVG(cpu_time_ms) as avg_cpu,
                STDDEV_POP(cpu_time_ms) as stddev_cpu,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY cpu_time_ms) as p95_cpu,
                AVG(duration_ms) as avg_duration,
                STDDEV_POP(duration_ms) as stddev_duration,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_duration,
                AVG(logical_reads) as avg_reads,
                STDDEV_POP(logical_reads) as stddev_reads,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY logical_reads) as p95_reads,
                AVG(memory_mb) as avg_memory,
                STDDEV_POP(memory_mb) as stddev_memory
            FROM query_metrics
            WHERE query_hash = ?
                AND instance_name = ?
                AND collected_at >= ?
        """, [query_hash, instance_name, cutoff]).fetchone()

        samples = result[0] or 0
        if samples < 3:
            self._cache[cache_key] = {'data': None, 'timestamp': time.time()}
            return None

        baseline = {
            'samples': samples,
            'cpu_time_ms': {
                'avg': result[1] or 0,
                'stddev': result[2] or 0,
                'p95': result[3] or 0
            },
            'duration_ms': {
                'avg': result[4] or 0,
                'stddev': result[5] or 0,
                'p95': result[6] or 0
            },
            'logical_reads': {
                'avg': result[7] or 0,
                'stddev': result[8] or 0,
                'p95': result[9] or 0
            },
            'memory_mb': {
                'avg': result[10] or 0,
                'stddev': result[11] or 0
            }
        }

        self._cache[cache_key] = {'data': baseline, 'timestamp': time.time()}
        return baseline

    def check_deviation(
        self,
        query_hash: str,
        instance_name: str,
        current_metrics: Dict[str, Any],
        threshold_multiplier: float = 2.0
    ) -> List[Dict[str, Any]]:
        """
        Verifica se metricas atuais desviam significativamente da baseline.

        Args:
            query_hash: Hash da query
            instance_name: Nome da instancia
            current_metrics: Metricas atuais da query
            threshold_multiplier: Multiplicador do stddev para considerar desvio

        Returns:
            Lista de desvios detectados
        """
        baseline = self.get_query_baseline(query_hash, instance_name)
        if not baseline:
            return []

        deviations = []
        checks = [
            ('cpu_time_ms', current_metrics.get('cpu_time_ms', 0)),
            ('duration_ms', current_metrics.get('duration_ms', 0)),
            ('logical_reads', current_metrics.get('logical_reads', 0)),
        ]

        for metric_name, current_value in checks:
            if not current_value:
                continue

            bl = baseline.get(metric_name, {})
            avg = bl.get('avg', 0)
            stddev = bl.get('stddev', 0)

            if avg == 0 or stddev == 0:
                continue

            # Desvio se current > avg + (threshold_multiplier * stddev)
            upper_bound = avg + (threshold_multiplier * stddev)
            if current_value > upper_bound:
                multiplier = round(current_value / avg, 1) if avg > 0 else 0
                deviation_pct = round(((current_value - avg) / avg) * 100, 1)

                deviations.append({
                    'metric': metric_name,
                    'current_value': current_value,
                    'baseline_avg': round(avg, 2),
                    'baseline_stddev': round(stddev, 2),
                    'baseline_p95': round(bl.get('p95', 0), 2),
                    'upper_bound': round(upper_bound, 2),
                    'multiplier': multiplier,
                    'deviation_pct': deviation_pct,
                    'baseline_samples': baseline['samples']
                })

        return deviations

    def clear_cache(self):
        """Limpa todo o cache de baselines."""
        self._cache.clear()
