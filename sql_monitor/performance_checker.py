"""
Verifica se queries atendem aos critérios de performance problemáticos.
"""
from typing import Dict, List


class PerformanceChecker:
    """Verifica se queries excedem thresholds de performance."""

    def __init__(self, thresholds: Dict):
        """
        Inicializa checker com thresholds configurados.

        Args:
            thresholds: Dict com valores limites para cada métrica.
        """
        self.execution_time_seconds = thresholds.get('execution_time_seconds', 30)
        self.cpu_time_ms = thresholds.get('cpu_time_ms', 10000)
        self.logical_reads = thresholds.get('logical_reads', 50000)
        self.physical_reads = thresholds.get('physical_reads', 10000)
        self.writes = thresholds.get('writes', 5000)

    def is_problematic(self, query_info: Dict) -> bool:
        """
        Verifica se query é problemática baseado nos thresholds.

        Args:
            query_info: Dict com informações da query.

        Returns:
            bool: True se query excede qualquer threshold.
        """
        # Verifica cada métrica
        checks = [
            query_info.get('duration_seconds', 0) >= self.execution_time_seconds,
            query_info.get('cpu_time_ms', 0) >= self.cpu_time_ms,
            query_info.get('logical_reads', 0) >= self.logical_reads,
            query_info.get('physical_reads', 0) >= self.physical_reads,
            query_info.get('writes', 0) >= self.writes,
            query_info.get('has_table_scan', False)  # Table scan é sempre problemático
        ]

        return any(checks)

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
        if duration >= self.execution_time_seconds:
            reasons.append(
                f"Tempo de execução alto: {duration}s (limite: {self.execution_time_seconds}s)"
            )

        cpu = query_info.get('cpu_time_ms', 0)
        if cpu >= self.cpu_time_ms:
            reasons.append(
                f"Alto consumo de CPU: {cpu}ms (limite: {self.cpu_time_ms}ms)"
            )

        logical = query_info.get('logical_reads', 0)
        if logical >= self.logical_reads:
            reasons.append(
                f"Muitas leituras lógicas: {logical:,} (limite: {self.logical_reads:,})"
            )

        physical = query_info.get('physical_reads', 0)
        if physical >= self.physical_reads:
            reasons.append(
                f"Muitas leituras físicas: {physical:,} (limite: {self.physical_reads:,})"
            )

        writes = query_info.get('writes', 0)
        if writes >= self.writes:
            reasons.append(
                f"Muitas escritas: {writes:,} (limite: {self.writes:,})"
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
