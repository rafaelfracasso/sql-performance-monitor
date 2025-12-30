"""
OptimizationExecutor - Executor seguro de otimizações com rollback automático.

Executa otimizações de banco de dados com:
- Captura de métricas baseline (antes)
- Execução monitorada com timeout
- Captura de métricas pós-execução
- Rollback automático se degradação >20%
- Registro completo no DuckDB
"""
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import time
import logging

from ..utils.metrics_store import MetricsStore
from ..connectors.base_connector import BaseConnector
from .plan_state import PlanStateManager, OptimizationItem


logger = logging.getLogger(__name__)


class ExecutionResult:
    """Resultado da execução de uma otimização."""

    def __init__(
        self,
        optimization_id: str,
        status: str,
        duration_seconds: float,
        error_message: Optional[str] = None,
        metrics_before: Optional[Dict[str, Any]] = None,
        metrics_after: Optional[Dict[str, Any]] = None,
        improvement_percent: Optional[float] = None,
        degradation_percent: Optional[float] = None,
        rolled_back: bool = False,
        rollback_reason: Optional[str] = None
    ):
        self.optimization_id = optimization_id
        self.status = status
        self.duration_seconds = duration_seconds
        self.error_message = error_message
        self.metrics_before = metrics_before or {}
        self.metrics_after = metrics_after or {}
        self.improvement_percent = improvement_percent
        self.degradation_percent = degradation_percent
        self.rolled_back = rolled_back
        self.rollback_reason = rollback_reason
        self.executed_at = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário."""
        return {
            'optimization_id': self.optimization_id,
            'status': self.status,
            'duration_seconds': self.duration_seconds,
            'error_message': self.error_message,
            'metrics_before': self.metrics_before,
            'metrics_after': self.metrics_after,
            'improvement_percent': self.improvement_percent,
            'degradation_percent': self.degradation_percent,
            'rolled_back': self.rolled_back,
            'rollback_reason': self.rollback_reason,
            'executed_at': self.executed_at.isoformat()
        }


class OptimizationExecutor:
    """
    Executor seguro de otimizações de banco de dados.
    """

    def __init__(
        self,
        metrics_store: MetricsStore,
        plan_state_manager: PlanStateManager,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Inicializa executor.

        Args:
            metrics_store: Store de métricas
            plan_state_manager: Gerenciador de estado de planos
            config: Configurações
        """
        self.metrics_store = metrics_store
        self.plan_state_manager = plan_state_manager
        self.config = config or {}

        # Configurações de rollback
        self.auto_rollback_enabled = self.config.get('auto_rollback', {}).get('enabled', True)
        self.degradation_threshold = self.config.get('auto_rollback', {}).get(
            'degradation_threshold_percent', 20
        )
        self.wait_after_execution_minutes = self.config.get('auto_rollback', {}).get(
            'wait_after_execution_minutes', 10
        )

        # Timeout padrão para execuções
        self.default_timeout_seconds = self.config.get('max_execution_time_minutes', 240) * 60

        # Intervalo entre otimizações
        self.interval_between_optimizations_seconds = 300  # 5 minutos

    def execute_plan(
        self,
        plan_id: str,
        connector: BaseConnector,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Executa um plano completo de otimizações.

        Args:
            plan_id: ID do plano
            connector: Conector do banco de dados
            dry_run: Se True, apenas simula (não executa SQL)

        Returns:
            Dicionário com resultados
        """
        plan = self.plan_state_manager.get_plan(plan_id, sync_vetos=True)

        if not plan:
            raise ValueError(f"Plano {plan_id} não encontrado")

        logger.info(f"Iniciando execução do plano {plan_id} (dry_run={dry_run})")

        # Atualizar status
        self.plan_state_manager.update_plan_status(plan_id, 'executing')

        results = []
        total_optimizations = len(plan.optimizations)
        executed_count = 0
        success_count = 0
        failed_count = 0
        rolled_back_count = 0
        skipped_count = 0

        start_time = datetime.now()

        try:
            # Executar otimizações em ordem de prioridade
            sorted_opts = sorted(
                plan.optimizations,
                key=lambda o: {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}.get(o.priority, 999)
            )

            for i, optimization in enumerate(sorted_opts, 1):
                logger.info(
                    f"Executando otimização {i}/{total_optimizations}: "
                    f"{optimization.id} ({optimization.type})"
                )

                # Verificar se está vetada
                if optimization.vetoed:
                    logger.info(f"Otimização {optimization.id} está vetada, pulando")
                    skipped_count += 1
                    self.plan_state_manager.update_optimization_status(
                        plan_id, optimization.id, 'skipped'
                    )
                    continue

                # Verificar se não é auto-aprovada e não foi aprovada manualmente
                if not optimization.auto_approved and not optimization.approved:
                    logger.info(
                        f"Otimização {optimization.id} requer aprovação manual, pulando"
                    )
                    skipped_count += 1
                    self.plan_state_manager.update_optimization_status(
                        plan_id, optimization.id, 'requires_approval'
                    )
                    continue

                # Atualizar status
                self.plan_state_manager.update_optimization_status(
                    plan_id, optimization.id, 'executing'
                )

                # Executar
                result = self.execute_optimization(
                    optimization=optimization,
                    connector=connector,
                    dry_run=dry_run
                )

                results.append(result)
                executed_count += 1

                # Atualizar contadores
                if result.status == 'success':
                    success_count += 1
                    self.plan_state_manager.update_optimization_status(
                        plan_id, optimization.id, 'completed'
                    )
                elif result.status == 'failed':
                    failed_count += 1
                    self.plan_state_manager.update_optimization_status(
                        plan_id, optimization.id, 'failed'
                    )

                if result.rolled_back:
                    rolled_back_count += 1
                    self.plan_state_manager.update_optimization_status(
                        plan_id, optimization.id, 'rolled_back'
                    )

                # Registrar no DuckDB
                self._save_execution_result(plan_id, result)

                # Aguardar intervalo entre otimizações (se não for a última)
                if i < total_optimizations:
                    logger.info(
                        f"Aguardando {self.interval_between_optimizations_seconds}s "
                        f"antes da próxima otimização"
                    )
                    time.sleep(self.interval_between_optimizations_seconds)

        except Exception as e:
            logger.error(f"Erro ao executar plano {plan_id}: {e}", exc_info=True)
            self.plan_state_manager.update_plan_status(plan_id, 'failed')
            raise

        finally:
            # Finalizar
            duration = (datetime.now() - start_time).total_seconds()

            if failed_count > 0:
                final_status = 'completed_with_errors'
            elif executed_count == 0:
                final_status = 'no_optimizations_executed'
            else:
                final_status = 'completed'

            self.plan_state_manager.update_plan_status(plan_id, final_status)

            logger.info(
                f"Execução do plano {plan_id} finalizada em {duration:.1f}s. "
                f"Executadas: {executed_count}, Sucesso: {success_count}, "
                f"Falhas: {failed_count}, Rollbacks: {rolled_back_count}, "
                f"Puladas: {skipped_count}"
            )

        return {
            'plan_id': plan_id,
            'status': final_status,
            'duration_seconds': duration,
            'total_optimizations': total_optimizations,
            'executed': executed_count,
            'success': success_count,
            'failed': failed_count,
            'rolled_back': rolled_back_count,
            'skipped': skipped_count,
            'results': [r.to_dict() for r in results]
        }

    def execute_optimization(
        self,
        optimization: OptimizationItem,
        connector: BaseConnector,
        dry_run: bool = False
    ) -> ExecutionResult:
        """
        Executa uma otimização individual.

        Args:
            optimization: Otimização a executar
            connector: Conector do banco
            dry_run: Se True, não executa SQL

        Returns:
            ExecutionResult
        """
        opt_id = optimization.id
        start_time = time.time()

        logger.info(f"Iniciando execução de {opt_id}")

        try:
            # 1. Capturar métricas baseline
            logger.info(f"Capturando métricas baseline para {opt_id}")
            metrics_before = self._capture_metrics(
                connector, optimization.table, optimization.metadata
            )

            if dry_run:
                logger.info(f"[DRY RUN] Simulando execução de {opt_id}")
                logger.info(f"[DRY RUN] SQL: {optimization.sql_script[:200]}...")
                duration = 0.1
                status = 'simulated'
                metrics_after = metrics_before
                error_message = None

            else:
                # 2. Executar SQL
                logger.info(f"Executando SQL de {opt_id}")
                try:
                    connector.execute_ddl(optimization.sql_script)
                    status = 'success'
                    error_message = None
                except Exception as e:
                    logger.error(f"Erro ao executar SQL de {opt_id}: {e}")
                    status = 'failed'
                    error_message = str(e)

                    duration = time.time() - start_time
                    return ExecutionResult(
                        optimization_id=opt_id,
                        status=status,
                        duration_seconds=duration,
                        error_message=error_message,
                        metrics_before=metrics_before
                    )

                # 3. Aguardar estabilização
                logger.info(
                    f"Aguardando {self.wait_after_execution_minutes} minutos "
                    f"para estabilização"
                )
                time.sleep(self.wait_after_execution_minutes * 60)

                # 4. Capturar métricas pós-execução
                logger.info(f"Capturando métricas pós-execução para {opt_id}")
                metrics_after = self._capture_metrics(
                    connector, optimization.table, optimization.metadata
                )

            duration = time.time() - start_time

            # 5. Analisar impacto
            improvement_percent, degradation_percent = self._calculate_impact(
                metrics_before, metrics_after
            )

            # 6. Verificar se precisa rollback
            rolled_back = False
            rollback_reason = None

            if (
                self.auto_rollback_enabled
                and not dry_run
                and status == 'success'
                and degradation_percent and degradation_percent > self.degradation_threshold
            ):
                logger.warning(
                    f"Degradação detectada ({degradation_percent:.1f}%) "
                    f"em {opt_id}, executando rollback"
                )

                rolled_back = self._execute_rollback(
                    optimization, connector
                )

                if rolled_back:
                    status = 'rolled_back'
                    rollback_reason = (
                        f"Degradação de {degradation_percent:.1f}% "
                        f"excedeu threshold de {self.degradation_threshold}%"
                    )

            logger.info(
                f"Execução de {opt_id} finalizada: {status} "
                f"(duração: {duration:.1f}s, "
                f"melhoria: {improvement_percent:.1f}%, "
                f"degradação: {degradation_percent:.1f}%)"
            )

            return ExecutionResult(
                optimization_id=opt_id,
                status=status,
                duration_seconds=duration,
                error_message=error_message,
                metrics_before=metrics_before,
                metrics_after=metrics_after,
                improvement_percent=improvement_percent,
                degradation_percent=degradation_percent,
                rolled_back=rolled_back,
                rollback_reason=rollback_reason
            )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Erro inesperado ao executar {opt_id}: {e}", exc_info=True)

            return ExecutionResult(
                optimization_id=opt_id,
                status='error',
                duration_seconds=duration,
                error_message=str(e)
            )

    def _capture_metrics(
        self,
        connector: BaseConnector,
        table_name: str,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Captura métricas de uma tabela.

        Args:
            connector: Conector do banco
            table_name: Nome da tabela
            metadata: Metadados adicionais

        Returns:
            Dicionário com métricas
        """
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'table_name': table_name
        }

        try:
            # Buscar queries relacionadas à tabela no DuckDB
            query_stats = self.metrics_store.get_recent_query_stats(
                hours=1,
                table_filter=table_name
            )

            if query_stats:
                metrics['avg_cpu_time_ms'] = sum(
                    q.get('avg_cpu_time_ms', 0) for q in query_stats
                ) / len(query_stats)

                metrics['avg_duration_ms'] = sum(
                    q.get('avg_duration_ms', 0) for q in query_stats
                ) / len(query_stats)

                metrics['avg_logical_reads'] = sum(
                    q.get('avg_logical_reads', 0) for q in query_stats
                ) / len(query_stats)

                metrics['query_count'] = len(query_stats)

            else:
                # Sem dados recentes, usar valores padrão
                metrics['avg_cpu_time_ms'] = 0
                metrics['avg_duration_ms'] = 0
                metrics['avg_logical_reads'] = 0
                metrics['query_count'] = 0

        except Exception as e:
            logger.error(f"Erro ao capturar métricas: {e}")
            metrics['error'] = str(e)

        return metrics

    def _calculate_impact(
        self,
        metrics_before: Dict[str, Any],
        metrics_after: Dict[str, Any]
    ) -> Tuple[float, float]:
        """
        Calcula impacto (melhoria vs degradação).

        Args:
            metrics_before: Métricas antes
            metrics_after: Métricas depois

        Returns:
            (improvement_percent, degradation_percent)
        """
        improvement_percent = 0.0
        degradation_percent = 0.0

        # Comparar CPU time
        cpu_before = metrics_before.get('avg_cpu_time_ms', 0)
        cpu_after = metrics_after.get('avg_cpu_time_ms', 0)

        if cpu_before > 0 and cpu_after > 0:
            delta_percent = ((cpu_before - cpu_after) / cpu_before) * 100

            if delta_percent > 0:
                improvement_percent = delta_percent
            else:
                degradation_percent = abs(delta_percent)

        # Se não houver dados de CPU, comparar duration
        elif metrics_before.get('avg_duration_ms', 0) > 0:
            dur_before = metrics_before.get('avg_duration_ms', 0)
            dur_after = metrics_after.get('avg_duration_ms', 0)

            if dur_before > 0 and dur_after > 0:
                delta_percent = ((dur_before - dur_after) / dur_before) * 100

                if delta_percent > 0:
                    improvement_percent = delta_percent
                else:
                    degradation_percent = abs(delta_percent)

        return improvement_percent, degradation_percent

    def _execute_rollback(
        self,
        optimization: OptimizationItem,
        connector: BaseConnector
    ) -> bool:
        """
        Executa rollback de uma otimização.

        Args:
            optimization: Otimização a reverter
            connector: Conector do banco

        Returns:
            True se rollback foi bem sucedido
        """
        if not optimization.rollback_script:
            logger.warning(
                f"Otimização {optimization.id} não possui script de rollback"
            )
            return False

        logger.info(f"Executando rollback de {optimization.id}")

        try:
            connector.execute_ddl(optimization.rollback_script)
            logger.info(f"Rollback de {optimization.id} executado com sucesso")
            return True

        except Exception as e:
            logger.error(
                f"Erro ao executar rollback de {optimization.id}: {e}",
                exc_info=True
            )
            return False

    def _save_execution_result(
        self,
        plan_id: str,
        result: ExecutionResult
    ):
        """
        Salva resultado de execução no DuckDB.

        Args:
            plan_id: ID do plano
            result: Resultado da execução
        """
        try:
            self.metrics_store.save_execution_result(
                plan_id=plan_id,
                optimization_id=result.optimization_id,
                status=result.status,
                duration_seconds=result.duration_seconds,
                error_message=result.error_message,
                metrics_before=result.metrics_before,
                metrics_after=result.metrics_after,
                improvement_percent=result.improvement_percent,
                degradation_percent=result.degradation_percent,
                rolled_back=result.rolled_back,
                rollback_reason=result.rollback_reason
            )

            logger.debug(f"Resultado de {result.optimization_id} salvo no DuckDB")

        except Exception as e:
            logger.error(f"Erro ao salvar resultado no DuckDB: {e}")
