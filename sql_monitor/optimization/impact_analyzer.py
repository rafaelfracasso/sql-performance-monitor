"""
ImpactAnalyzer - Análise de impacto e ROI de otimizações.

Compara métricas antes vs depois, calcula ROI real, detecta regressões
e gera relatórios executivos.
"""
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import logging

from ..utils.metrics_store import MetricsStore


logger = logging.getLogger(__name__)


class ImpactReport:
    """Relatório de impacto de uma execução."""

    def __init__(
        self,
        plan_id: str,
        execution_date: datetime,
        total_optimizations: int,
        successful: int,
        failed: int,
        rolled_back: int,
        total_improvement_percent: float,
        total_degradation_percent: float,
        roi_metrics: Dict[str, Any],
        best_improvements: List[Dict[str, Any]],
        worst_regressions: List[Dict[str, Any]],
        recommendations: List[str]
    ):
        self.plan_id = plan_id
        self.execution_date = execution_date
        self.total_optimizations = total_optimizations
        self.successful = successful
        self.failed = failed
        self.rolled_back = rolled_back
        self.total_improvement_percent = total_improvement_percent
        self.total_degradation_percent = total_degradation_percent
        self.roi_metrics = roi_metrics
        self.best_improvements = best_improvements
        self.worst_regressions = worst_regressions
        self.recommendations = recommendations

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário."""
        return {
            'plan_id': self.plan_id,
            'execution_date': self.execution_date.isoformat(),
            'summary': {
                'total_optimizations': self.total_optimizations,
                'successful': self.successful,
                'failed': self.failed,
                'rolled_back': self.rolled_back,
                'success_rate': (self.successful / self.total_optimizations * 100)
                    if self.total_optimizations > 0 else 0
            },
            'impact': {
                'total_improvement_percent': self.total_improvement_percent,
                'total_degradation_percent': self.total_degradation_percent,
                'net_improvement': self.total_improvement_percent - self.total_degradation_percent
            },
            'roi': self.roi_metrics,
            'best_improvements': self.best_improvements,
            'worst_regressions': self.worst_regressions,
            'recommendations': self.recommendations
        }


class ImpactAnalyzer:
    """
    Analisador de impacto e ROI de otimizações.
    """

    def __init__(self, metrics_store: MetricsStore):
        """
        Inicializa analisador.

        Args:
            metrics_store: Store de métricas
        """
        self.metrics_store = metrics_store

    def analyze_plan_impact(
        self,
        plan_id: str,
        days_before: int = 7,
        days_after: int = 7
    ) -> ImpactReport:
        """
        Analisa impacto de um plano executado.

        Compara métricas 7 dias antes vs 7 dias depois da execução.

        Args:
            plan_id: ID do plano
            days_before: Dias antes da execução para baseline
            days_after: Dias depois da execução para comparação

        Returns:
            ImpactReport com análise completa
        """
        logger.info(f"Analisando impacto do plano {plan_id}")

        # Buscar execuções do plano
        executions = self.metrics_store.get_execution_history(plan_id=plan_id)

        if not executions:
            raise ValueError(f"Nenhuma execução encontrada para plano {plan_id}")

        # Data de execução (primeira execução do plano)
        execution_date = executions[0]['executed_at']
        if isinstance(execution_date, str):
            execution_date = datetime.fromisoformat(execution_date)

        # Estatísticas básicas
        total_optimizations = len(executions)
        successful = sum(1 for e in executions if e['status'] == 'success')
        failed = sum(1 for e in executions if e['status'] == 'failed')
        rolled_back = sum(1 for e in executions if e['rolled_back'])

        # Calcular melhorias/degradações
        improvements = []
        degradations = []

        for exec in executions:
            if exec['improvement_percent'] and exec['improvement_percent'] > 0:
                improvements.append({
                    'optimization_id': exec['optimization_id'],
                    'improvement_percent': exec['improvement_percent'],
                    'status': exec['status']
                })

            if exec['degradation_percent'] and exec['degradation_percent'] > 0:
                degradations.append({
                    'optimization_id': exec['optimization_id'],
                    'degradation_percent': exec['degradation_percent'],
                    'status': exec['status'],
                    'rolled_back': exec['rolled_back']
                })

        total_improvement = sum(i['improvement_percent'] for i in improvements)
        total_degradation = sum(d['degradation_percent'] for d in degradations)

        # Calcular ROI
        roi_metrics = self._calculate_roi(
            executions=executions,
            days_before=days_before,
            days_after=days_after,
            execution_date=execution_date
        )

        # Top melhorias
        best_improvements = sorted(
            improvements,
            key=lambda x: x['improvement_percent'],
            reverse=True
        )[:5]

        # Piores regressões (que não foram rolled back)
        worst_regressions = sorted(
            [d for d in degradations if not d['rolled_back']],
            key=lambda x: x['degradation_percent'],
            reverse=True
        )[:5]

        # Gerar recomendações
        recommendations = self._generate_recommendations(
            executions=executions,
            improvements=improvements,
            degradations=degradations,
            roi_metrics=roi_metrics
        )

        logger.info(
            f"Análise concluída: {successful}/{total_optimizations} sucessos, "
            f"melhoria total: {total_improvement:.1f}%, "
            f"degradação total: {total_degradation:.1f}%"
        )

        return ImpactReport(
            plan_id=plan_id,
            execution_date=execution_date,
            total_optimizations=total_optimizations,
            successful=successful,
            failed=failed,
            rolled_back=rolled_back,
            total_improvement_percent=total_improvement,
            total_degradation_percent=total_degradation,
            roi_metrics=roi_metrics,
            best_improvements=best_improvements,
            worst_regressions=worst_regressions,
            recommendations=recommendations
        )

    def _calculate_roi(
        self,
        executions: List[Dict[str, Any]],
        days_before: int,
        days_after: int,
        execution_date: datetime
    ) -> Dict[str, Any]:
        """
        Calcula ROI baseado em métricas reais.

        Args:
            executions: Lista de execuções
            days_before: Dias antes para baseline
            days_after: Dias depois para comparação
            execution_date: Data da execução

        Returns:
            Dicionário com métricas de ROI
        """
        # Períodos de comparação
        baseline_start = execution_date - timedelta(days=days_before)
        baseline_end = execution_date

        comparison_start = execution_date
        comparison_end = execution_date + timedelta(days=days_after)

        # Buscar métricas agregadas para os períodos
        baseline_metrics = self._get_aggregated_metrics(
            baseline_start, baseline_end
        )

        comparison_metrics = self._get_aggregated_metrics(
            comparison_start, comparison_end
        )

        # Calcular variações
        cpu_reduction = 0
        if baseline_metrics['avg_cpu_ms'] > 0:
            cpu_reduction = (
                (baseline_metrics['avg_cpu_ms'] - comparison_metrics['avg_cpu_ms'])
                / baseline_metrics['avg_cpu_ms'] * 100
            )

        duration_reduction = 0
        if baseline_metrics['avg_duration_ms'] > 0:
            duration_reduction = (
                (baseline_metrics['avg_duration_ms'] - comparison_metrics['avg_duration_ms'])
                / baseline_metrics['avg_duration_ms'] * 100
            )

        reads_reduction = 0
        if baseline_metrics['avg_logical_reads'] > 0:
            reads_reduction = (
                (baseline_metrics['avg_logical_reads'] - comparison_metrics['avg_logical_reads'])
                / baseline_metrics['avg_logical_reads'] * 100
            )

        # Tempo total de execução das otimizações
        total_execution_time_hours = sum(
            e['duration_seconds'] for e in executions
        ) / 3600

        # Estimativa de economia de tempo (queries mais rápidas)
        # Assumindo queries rodando 24/7
        queries_per_day = comparison_metrics['query_count'] / days_after
        avg_query_saving_ms = (
            baseline_metrics['avg_duration_ms'] - comparison_metrics['avg_duration_ms']
        )

        if avg_query_saving_ms > 0 and queries_per_day > 0:
            # Economia em horas por dia
            time_saved_per_day_hours = (
                queries_per_day * avg_query_saving_ms / 1000 / 3600
            )
            # ROI em dias
            roi_days = total_execution_time_hours / time_saved_per_day_hours if time_saved_per_day_hours > 0 else float('inf')
        else:
            time_saved_per_day_hours = 0
            roi_days = float('inf')

        return {
            'baseline_period': {
                'start': baseline_start.isoformat(),
                'end': baseline_end.isoformat(),
                'avg_cpu_ms': baseline_metrics['avg_cpu_ms'],
                'avg_duration_ms': baseline_metrics['avg_duration_ms'],
                'avg_logical_reads': baseline_metrics['avg_logical_reads'],
                'query_count': baseline_metrics['query_count']
            },
            'comparison_period': {
                'start': comparison_start.isoformat(),
                'end': comparison_end.isoformat(),
                'avg_cpu_ms': comparison_metrics['avg_cpu_ms'],
                'avg_duration_ms': comparison_metrics['avg_duration_ms'],
                'avg_logical_reads': comparison_metrics['avg_logical_reads'],
                'query_count': comparison_metrics['query_count']
            },
            'improvements': {
                'cpu_reduction_percent': cpu_reduction,
                'duration_reduction_percent': duration_reduction,
                'reads_reduction_percent': reads_reduction
            },
            'roi': {
                'total_execution_time_hours': total_execution_time_hours,
                'time_saved_per_day_hours': time_saved_per_day_hours,
                'roi_days': roi_days if roi_days != float('inf') else None,
                'payback_reached': roi_days <= days_after if roi_days != float('inf') else False
            }
        }

    def _get_aggregated_metrics(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Busca métricas agregadas para um período.

        Args:
            start_date: Data inicial
            end_date: Data final

        Returns:
            Dicionário com métricas agregadas
        """
        hours = (end_date - start_date).total_seconds() / 3600
        stats = self.metrics_store.get_recent_query_stats(hours=int(hours))

        if not stats:
            return {
                'avg_cpu_ms': 0,
                'avg_duration_ms': 0,
                'avg_logical_reads': 0,
                'query_count': 0
            }

        return {
            'avg_cpu_ms': sum(s['avg_cpu_time_ms'] for s in stats) / len(stats),
            'avg_duration_ms': sum(s['avg_duration_ms'] for s in stats) / len(stats),
            'avg_logical_reads': sum(s['avg_logical_reads'] for s in stats) / len(stats),
            'query_count': sum(s['occurrences'] for s in stats)
        }

    def _generate_recommendations(
        self,
        executions: List[Dict[str, Any]],
        improvements: List[Dict[str, Any]],
        degradations: List[Dict[str, Any]],
        roi_metrics: Dict[str, Any]
    ) -> List[str]:
        """
        Gera recomendações baseadas na análise.

        Args:
            executions: Lista de execuções
            improvements: Lista de melhorias
            degradations: Lista de degradações
            roi_metrics: Métricas de ROI

        Returns:
            Lista de recomendações
        """
        recommendations = []

        # Análise de taxa de sucesso
        success_rate = sum(1 for e in executions if e['status'] == 'success') / len(executions) * 100

        if success_rate < 80:
            recommendations.append(
                f"Taxa de sucesso baixa ({success_rate:.1f}%). "
                "Revisar otimizações que falharam para melhorar qualidade do plano."
            )
        elif success_rate >= 95:
            recommendations.append(
                f"Excelente taxa de sucesso ({success_rate:.1f}%). "
                "Sistema de classificação de risco está funcionando bem."
            )

        # Análise de rollbacks
        rollback_rate = sum(1 for e in executions if e['rolled_back']) / len(executions) * 100

        if rollback_rate > 10:
            recommendations.append(
                f"Taxa de rollback alta ({rollback_rate:.1f}%). "
                "Considerar ajustar threshold de degradação ou melhorar estimativas de impacto."
            )

        # Análise de melhorias
        if len(improvements) > 0:
            avg_improvement = sum(i['improvement_percent'] for i in improvements) / len(improvements)
            recommendations.append(
                f"Melhoria média de {avg_improvement:.1f}% nas otimizações bem-sucedidas. "
                "Continuar focando em otimizações similares."
            )

        # Análise de degradações não revertidas
        non_rolled_degradations = [d for d in degradations if not d['rolled_back']]

        if len(non_rolled_degradations) > 0:
            recommendations.append(
                f"{len(non_rolled_degradations)} otimizações causaram degradação não revertida. "
                "Investigar e considerar reverter manualmente."
            )

        # Análise de ROI
        roi_days = roi_metrics['roi']['roi_days']

        if roi_days and roi_days < 30:
            recommendations.append(
                f"Excelente ROI: payback em {roi_days:.0f} dias. "
                "Otimizações valeram o investimento de tempo."
            )
        elif roi_days and roi_days > 90:
            recommendations.append(
                f"ROI baixo: payback em {roi_days:.0f} dias. "
                "Focar em otimizações com maior impacto."
            )

        # Recomendação geral
        if not recommendations:
            recommendations.append(
                "Plano executado com sucesso. Continuar monitorando métricas nas próximas semanas."
            )

        return recommendations

    def generate_executive_summary(
        self,
        plan_id: str
    ) -> str:
        """
        Gera resumo executivo em formato texto.

        Args:
            plan_id: ID do plano

        Returns:
            String com resumo formatado
        """
        report = self.analyze_plan_impact(plan_id)

        summary = f"""
========================================
RELATÓRIO DE IMPACTO - PLANO {plan_id}
========================================

Data de Execução: {report.execution_date.strftime('%d/%m/%Y %H:%M')}

RESUMO:
-------
- Total de Otimizações: {report.total_optimizations}
- Bem-sucedidas: {report.successful} ({report.successful/report.total_optimizations*100:.1f}%)
- Falhadas: {report.failed}
- Revertidas (Rollback): {report.rolled_back}

IMPACTO:
--------
- Melhoria Total: {report.total_improvement_percent:.1f}%
- Degradação Total: {report.total_degradation_percent:.1f}%
- Impacto Líquido: {report.total_improvement_percent - report.total_degradation_percent:.1f}%

ROI:
----
- Tempo de Execução: {report.roi_metrics['roi']['total_execution_time_hours']:.2f} horas
- Economia por Dia: {report.roi_metrics['roi']['time_saved_per_day_hours']:.2f} horas
"""

        if report.roi_metrics['roi']['roi_days']:
            summary += f"- Payback: {report.roi_metrics['roi']['roi_days']:.0f} dias\n"
        else:
            summary += "- Payback: Não alcançado ainda\n"

        summary += f"""
MELHORIAS:
----------
{report.roi_metrics['improvements']['cpu_reduction_percent']:.1f}% redução em CPU time
{report.roi_metrics['improvements']['duration_reduction_percent']:.1f}% redução em duration
{report.roi_metrics['improvements']['reads_reduction_percent']:.1f}% redução em logical reads

TOP 5 MELHORIAS:
"""
        for i, improvement in enumerate(report.best_improvements, 1):
            summary += f"{i}. {improvement['optimization_id']}: {improvement['improvement_percent']:.1f}%\n"

        if report.worst_regressions:
            summary += "\nREGRESSÕES (NÃO REVERTIDAS):\n"
            for i, regression in enumerate(report.worst_regressions, 1):
                summary += f"{i}. {regression['optimization_id']}: {regression['degradation_percent']:.1f}%\n"

        summary += "\nRECOMENDAÇÕES:\n"
        for i, rec in enumerate(report.recommendations, 1):
            summary += f"{i}. {rec}\n"

        summary += "\n========================================\n"

        return summary
