"""
Verifica se queries atendem aos critérios de performance problemáticos.
"""
from typing import Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from .metrics_store import MetricsStore


class PerformanceChecker:
    """Verifica se queries excedem thresholds de performance."""

    def __init__(self, metrics_store: 'MetricsStore', db_type: str):
        """
        Inicializa checker carregando thresholds do DuckDB.

        Args:
            metrics_store: Instância do MetricsStore.
            db_type: Tipo do banco (hana, sqlserver, postgresql).
        """
        self.metrics_store = metrics_store
        self.db_type = db_type
        self._load_thresholds()

    def _load_thresholds(self):
        """Carrega thresholds do DuckDB para o dbtype."""
        query = """
            SELECT execution_time_ms, cpu_time_ms, logical_reads,
                   physical_reads, writes, wait_time_ms, memory_mb, row_count
            FROM performance_thresholds_by_dbtype
            WHERE db_type = ?
        """
        result = self.metrics_store.execute_query(query, (self.db_type,))

        if not result or len(result) == 0:
            raise ValueError(
                f"Nenhum threshold encontrado para {self.db_type}. "
                f"Execute: python scripts/migrate_config_to_duckdb.py"
            )

        row = result[0]
        # Converter ms para seconds para execution_time (manter compatibilidade)
        self.execution_time_seconds = row[0] / 1000.0  # ms -> seconds
        self.cpu_time_ms = row[1]
        self.logical_reads = row[2]
        self.physical_reads = row[3]
        self.writes = row[4]
        self.wait_time_ms = row[5]
        self.memory_mb = row[6]
        self.row_count = row[7]

    def reload_thresholds(self):
        """Recarrega thresholds do DuckDB (após mudanças na UI)."""
        self._load_thresholds()

    def is_problematic(self, query_info: Dict) -> bool:
        """
        Verifica se query é problemática baseado nos thresholds.

        Args:
            query_info: Dict com informações da query.

        Returns:
            bool: True se query excede qualquer threshold.
        """
        # Verifica cada métrica (threshold negativo = check desabilitado)
        checks = [
            self.execution_time_seconds >= 0 and query_info.get('duration_seconds', 0) >= self.execution_time_seconds,
            self.cpu_time_ms >= 0 and query_info.get('cpu_time_ms', 0) >= self.cpu_time_ms,
            self.logical_reads >= 0 and query_info.get('logical_reads', 0) >= self.logical_reads,
            self.physical_reads >= 0 and query_info.get('physical_reads', 0) >= self.physical_reads,
            self.writes >= 0 and query_info.get('writes', 0) >= self.writes,
            self.memory_mb >= 0 and query_info.get('memory_mb', 0) >= self.memory_mb,
            query_info.get('has_table_scan', False)  # Table scan é sempre problemático
        ]

        return any(checks)

    def should_analyze_query(self, query_info: Dict) -> bool:
        """
        Verifica se a query deve ser analisada (alias para is_problematic).

        Args:
            query_info: Dict com informações da query.

        Returns:
            bool: True se query excede qualquer threshold.
        """
        return self.is_problematic(query_info)

    def filter_problematic_queries(self, queries: List[Dict]) -> List[Dict]:
        """
        Filtra apenas queries problemáticas.

        Args:
            queries: Lista de queries coletadas.

        Returns:
            Lista filtrada de queries problemáticas.
        """
        return [q for q in queries if self.is_problematic(q)]

    def get_violation_reasons(self, query_info: Dict) -> List[str]:
        """
        Retorna lista de motivos pelos quais a query é problemática.

        Args:
            query_info: Dict com informações da query.

        Returns:
            Lista de strings descrevendo violações.
        """
        reasons = []

        duration = query_info.get('duration_seconds', 0)
        if self.execution_time_seconds >= 0 and duration >= self.execution_time_seconds:
            reasons.append(
                f"Tempo de execução alto: {duration}s (limite: {self.execution_time_seconds}s)"
            )

        cpu = query_info.get('cpu_time_ms', 0)
        if self.cpu_time_ms >= 0 and cpu >= self.cpu_time_ms:
            reasons.append(
                f"Alto consumo de CPU: {cpu}ms (limite: {self.cpu_time_ms}ms)"
            )

        logical = query_info.get('logical_reads', 0)
        if self.logical_reads >= 0 and logical >= self.logical_reads:
            reasons.append(
                f"Muitas leituras lógicas: {logical:,} (limite: {self.logical_reads:,})"
            )

        physical = query_info.get('physical_reads', 0)
        if self.physical_reads >= 0 and physical >= self.physical_reads:
            reasons.append(
                f"Muitas leituras físicas: {physical:,} (limite: {self.physical_reads:,})"
            )

        writes = query_info.get('writes', 0)
        if self.writes >= 0 and writes >= self.writes:
            reasons.append(
                f"Muitas escritas: {writes:,} (limite: {self.writes:,})"
            )

        memory = query_info.get('memory_mb', 0)
        if self.memory_mb >= 0 and memory >= self.memory_mb:
            reasons.append(
                f"Alto consumo de memória: {memory:.1f}MB (limite: {self.memory_mb}MB)"
            )

        if query_info.get('has_table_scan', False):
            reasons.append("Table Scan detectado (varredura completa sem índice)")

        return reasons

    def format_metrics(self, query_info: Dict) -> str:
        """
        Formata métricas para exibição.

        Args:
            query_info: Dict com informações da query.

        Returns:
            String formatada com métricas.
        """
        lines = ["MÉTRICAS DE PERFORMANCE:"]

        metrics = [
            ("Tempo de execução", f"{query_info.get('duration_seconds', 'N/A')}s"),
            ("CPU Time", f"{query_info.get('cpu_time_ms', 'N/A')}ms"),
            ("Logical Reads", f"{query_info.get('logical_reads', 'N/A'):,}" if query_info.get('logical_reads') else 'N/A'),
            ("Physical Reads", f"{query_info.get('physical_reads', 'N/A'):,}" if query_info.get('physical_reads') else 'N/A'),
            ("Writes", f"{query_info.get('writes', 'N/A'):,}" if query_info.get('writes') else 'N/A'),
        ]

        for label, value in metrics:
            lines.append(f"  - {label}: {value}")

        # Adiciona status
        if query_info.get('status'):
            lines.append(f"  - Status: {query_info.get('status')}")

        if query_info.get('has_table_scan'):
            lines.append("  - ⚠️  TABLE SCAN DETECTADO")

        return "\n".join(lines)

    def get_summary(self, query_info: Dict) -> str:
        """
        Retorna resumo de por que a query é problemática.

        Args:
            query_info: Dict com informações da query.

        Returns:
            String com resumo.
        """
        reasons = self.get_violation_reasons(query_info)

        if not reasons:
            return "Query dentro dos limites normais."

        summary = "PROBLEMAS DETECTADOS:\n"
        for i, reason in enumerate(reasons, 1):
            summary += f"{i}. {reason}\n"

        return summary.strip()
