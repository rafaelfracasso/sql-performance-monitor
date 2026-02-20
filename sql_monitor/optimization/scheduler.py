"""
WeeklyOptimizerScheduler - Agendador do sistema de otimização semanal.

Agenda:
- Quinta-feira 18:00: Gera plano semanal
- Domingo 02:00: Executa plano (se não vetado)
- Segunda 08:00: Gera relatório de impacto
"""
import schedule
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import threading

from .weekly_planner import WeeklyOptimizationPlanner
from .risk_classifier import RiskClassifier
from .approval_engine import AutoApprovalEngine
from .veto_system import VetoSystem
from .plan_state import PlanStateManager, OptimizationPlan, OptimizationItem
from .executor import OptimizationExecutor
from .impact_analyzer import ImpactAnalyzer
from ..utils.metrics_store import MetricsStore
from ..utils.teams_notifier import TeamsNotifier
from ..connectors.base_connector import BaseConnector


logger = logging.getLogger(__name__)


class WeeklyOptimizerScheduler:
    """
    Agendador do sistema de otimização semanal.
    """

    def __init__(
        self,
        metrics_store: MetricsStore,
        connectors: Dict[str, BaseConnector],
        teams_notifier: Optional[TeamsNotifier] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Inicializa scheduler.

        Args:
            metrics_store: Store de métricas
            connectors: Dicionário de conectores {instance_name: connector}
            teams_notifier: Notificador Teams (opcional)
            config: Configurações
        """
        self.metrics_store = metrics_store
        self.connectors = connectors
        self.teams_notifier = teams_notifier
        self.config = config or {}

        # Configurações de horários
        schedule_config = self.config.get('schedule', {})
        self.analysis_day = schedule_config.get('analysis_day', 'thursday')
        self.analysis_time = schedule_config.get('analysis_time', '18:00')
        self.execution_day = schedule_config.get('execution_day', 'sunday')
        self.execution_time = schedule_config.get('execution_time', '02:00')
        self.report_day = schedule_config.get('report_day', 'monday')
        self.report_time = schedule_config.get('report_time', '08:00')

        # Configurações do sistema
        self.veto_window_hours = self.config.get('veto_window', {}).get('hours', 72)
        self.analysis_days = self.config.get('analysis', {}).get('days', 7)

        # Inicializar componentes
        self.veto_system = VetoSystem(metrics_store=self.metrics_store)
        self.plan_state_manager = PlanStateManager(metrics_store=self.metrics_store, veto_system=self.veto_system)
        self.risk_classifier = RiskClassifier(
            config=self.config.get('risk_thresholds', {})
        )
        self.approval_engine = AutoApprovalEngine(
            config=self.config
        )
        self.executor = OptimizationExecutor(
            metrics_store=self.metrics_store,
            plan_state_manager=self.plan_state_manager,
            config=self.config
        )
        self.impact_analyzer = ImpactAnalyzer(
            metrics_store=self.metrics_store
        )
        self.weekly_planner = WeeklyOptimizationPlanner(
            metrics_store=self.metrics_store,
            config=self.config
        )

        # Thread de execução
        self._scheduler_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._running = False

    def start(self):
        """Inicia o scheduler em thread separada."""
        if self._running:
            logger.warning("Scheduler já está rodando")
            return

        logger.info("Iniciando Weekly Optimizer Scheduler")

        # Configurar agendamentos
        self._setup_schedule()

        # Iniciar thread
        self._running = True
        self._stop_event.clear()
        self._scheduler_thread = threading.Thread(
            target=self._run_scheduler,
            daemon=True,
            name="WeeklyOptimizerScheduler"
        )
        self._scheduler_thread.start()

        logger.info(
            f"Scheduler iniciado. Próximas execuções:\n"
            f"  - Análise: {self.analysis_day} às {self.analysis_time}\n"
            f"  - Execução: {self.execution_day} às {self.execution_time}\n"
            f"  - Relatório: {self.report_day} às {self.report_time}"
        )

    def stop(self):
        """Para o scheduler."""
        if not self._running:
            return

        logger.info("Parando scheduler...")
        self._running = False
        self._stop_event.set()

        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)

        schedule.clear()
        logger.info("Scheduler parado")

    def _setup_schedule(self):
        """Configura os agendamentos."""
        # Limpar agendamentos anteriores
        schedule.clear()

        # Quinta-feira 18:00 - Gerar plano
        getattr(schedule.every(), self.analysis_day).at(self.analysis_time).do(
            self._job_generate_plan
        )

        # Domingo 02:00 - Executar plano
        getattr(schedule.every(), self.execution_day).at(self.execution_time).do(
            self._job_execute_plan
        )

        # Segunda 08:00 - Gerar relatório
        getattr(schedule.every(), self.report_day).at(self.report_time).do(
            self._job_generate_report
        )

        logger.info("Agendamentos configurados")

    def _run_scheduler(self):
        """Loop principal do scheduler."""
        logger.info("Thread do scheduler iniciada")

        while not self._stop_event.is_set():
            try:
                schedule.run_pending()
                time.sleep(60)  # Verificar a cada minuto

            except Exception as e:
                logger.error(f"Erro no scheduler: {e}", exc_info=True)
                time.sleep(60)

        logger.info("Thread do scheduler finalizada")

    def _job_generate_plan(self):
        """
        Job: Gera plano semanal (quinta-feira 18:00).
        """
        logger.info("=" * 60)
        logger.info("JOB: Gerando plano semanal de otimização")
        logger.info("=" * 60)

        try:
            # Gerar ID do plano
            plan_id = datetime.now().strftime('%Y%m%d_%H%M%S')

            # Gerar plano usando WeeklyOptimizationPlanner
            plan_data = self.weekly_planner.generate_weekly_plan(days=self.analysis_days)

            if not plan_data or not plan_data.get('optimizations'):
                logger.warning("Nenhuma otimização gerada no plano")
                return

            # Classificar riscos e aprovar automaticamente
            optimizations_with_risk = []
            metadata_map = {}

            for opt in plan_data['optimizations']:
                # Obter metadados da tabela
                metadata = self._get_table_metadata(opt)
                metadata_map[opt['id']] = metadata

                # Classificar risco
                risk_assessment = self.risk_classifier.classify_optimization(
                    opt, metadata
                )

                # Criar OptimizationItem
                opt_item = OptimizationItem(
                    id=opt['id'],
                    type=opt['type'],
                    priority=opt.get('priority', 'medium'),
                    risk_level=risk_assessment.risk_level.value,
                    table=opt.get('table', ''),
                    description=opt.get('description', ''),
                    estimated_improvement_percent=opt.get('estimated_improvement_percent', 0),
                    estimated_duration_minutes=opt.get('estimated_duration_minutes', 30),
                    sql_script=opt.get('sql_script', ''),
                    rollback_script=opt.get('rollback_script'),
                    auto_approved=risk_assessment.auto_approved,
                    metadata=metadata
                )

                optimizations_with_risk.append(opt_item)

            # Avaliar plano com approval engine
            evaluation = self.approval_engine.evaluate_plan(
                [opt.to_dict() for opt in optimizations_with_risk],
                metadata_map
            )

            # Calcular janela de execução
            execution_window = self.approval_engine.calculate_execution_window(
                plan_generated_at=datetime.now(),
                veto_window_hours=self.veto_window_hours
            )

            # Criar plano
            plan = OptimizationPlan(
                plan_id=plan_id,
                generated_at=datetime.now(),
                execution_scheduled_at=execution_window['execution_scheduled_at'],
                analysis_period_days=self.analysis_days,
                status='pending',
                veto_window_expires_at=execution_window['veto_expires_at'],
                total_optimizations=len(optimizations_with_risk),
                auto_approved_count=evaluation['statistics']['auto_approved'],
                requires_review_count=evaluation['statistics']['requires_review'],
                blocked_count=evaluation['statistics']['blocked'],
                optimizations=optimizations_with_risk
            )

            # Salvar plano
            self.plan_state_manager.save_plan(plan)

            logger.info(
                f"Plano {plan_id} gerado com sucesso:\n"
                f"  - Total: {plan.total_optimizations} otimizações\n"
                f"  - Auto-aprovadas: {plan.auto_approved_count}\n"
                f"  - Requer revisão: {plan.requires_review_count}\n"
                f"  - Bloqueadas: {plan.blocked_count}\n"
                f"  - Execução programada: {execution_window['execution_scheduled_at']}\n"
                f"  - Janela de veto expira: {execution_window['veto_expires_at']}"
            )

            # Notificar Teams
            if self.teams_notifier:
                self._notify_plan_generated(plan, evaluation)

        except Exception as e:
            logger.error(f"Erro ao gerar plano: {e}", exc_info=True)

    def _job_execute_plan(self):
        """
        Job: Executa plano semanal (domingo 02:00).
        """
        logger.info("=" * 60)
        logger.info("JOB: Executando plano semanal de otimização")
        logger.info("=" * 60)

        try:
            # Buscar plano mais recente pendente
            plans = self.plan_state_manager.list_plans(limit=1, status_filter='pending')

            if not plans:
                logger.warning("Nenhum plano pendente encontrado para execução")
                return

            plan = plans[0]

            # Verificar se está vetado
            if self.veto_system.is_plan_vetoed(plan.plan_id):
                logger.warning(f"Plano {plan.plan_id} foi vetado, não será executado")
                self.plan_state_manager.update_plan_status(plan.plan_id, 'vetoed')
                return

            # Verificar se janela de veto expirou
            if datetime.now() < plan.veto_window_expires_at:
                logger.warning(
                    f"Janela de veto do plano {plan.plan_id} ainda está ativa "
                    f"(expira em {plan.veto_window_expires_at})"
                )
                return

            logger.info(f"Executando plano {plan.plan_id}")

            # Executar plano para cada instância
            results_by_instance = {}

            for instance_name, connector in self.connectors.items():
                logger.info(f"Executando otimizações para instância {instance_name}")

                try:
                    result = self.executor.execute_plan(
                        plan_id=plan.plan_id,
                        connector=connector,
                        dry_run=False
                    )

                    results_by_instance[instance_name] = result

                except Exception as e:
                    logger.error(
                        f"Erro ao executar plano para instância {instance_name}: {e}",
                        exc_info=True
                    )
                    results_by_instance[instance_name] = {'error': str(e)}

            logger.info(f"Plano {plan.plan_id} executado para todas as instâncias")

            # Notificar Teams
            if self.teams_notifier:
                self._notify_plan_executed(plan, results_by_instance)

        except Exception as e:
            logger.error(f"Erro ao executar plano: {e}", exc_info=True)

    def _job_generate_report(self):
        """
        Job: Gera relatório de impacto (segunda 08:00).
        """
        logger.info("=" * 60)
        logger.info("JOB: Gerando relatório de impacto")
        logger.info("=" * 60)

        try:
            # Buscar plano executado mais recentemente
            # (deveria ter sido executado no domingo)
            executions = self.metrics_store.get_execution_history(limit=100)

            if not executions:
                logger.warning("Nenhuma execução encontrada para relatório")
                return

            # Pegar plan_id da execução mais recente
            plan_id = executions[0]['plan_id']

            # Verificar se execução foi há pelo menos 1 dia
            # (queremos dar tempo para acumular métricas)
            last_exec = executions[0]['executed_at']
            if isinstance(last_exec, str):
                last_exec = datetime.fromisoformat(last_exec)

            hours_since_exec = (datetime.now() - last_exec).total_seconds() / 3600

            if hours_since_exec < 24:
                logger.info(
                    f"Execução muito recente ({hours_since_exec:.1f}h atrás), "
                    "aguardando mais tempo para coletar métricas"
                )
                return

            logger.info(f"Gerando relatório de impacto para plano {plan_id}")

            # Gerar relatório
            report = self.impact_analyzer.analyze_plan_impact(
                plan_id=plan_id,
                days_before=7,
                days_after=min(7, int(hours_since_exec / 24))
            )

            # Gerar resumo executivo
            summary = self.impact_analyzer.generate_executive_summary(plan_id)

            logger.info(f"Relatório gerado:\n{summary}")

            # Notificar Teams
            if self.teams_notifier:
                self._notify_impact_report(report, summary)

        except Exception as e:
            logger.error(f"Erro ao gerar relatório: {e}", exc_info=True)

    def _get_table_metadata(self, optimization: Dict[str, Any]) -> Dict[str, Any]:
        """
        Busca metadados de uma tabela.

        Args:
            optimization: Dicionário com dados da otimização

        Returns:
            Dicionário com metadados
        """
        table_name = optimization.get('table', '')
        instance_name = optimization.get('instance_name', '')

        if not table_name:
            return {}

        # Buscar no DuckDB
        try:
            # Aqui poderíamos buscar de table_metadata
            # Por enquanto, retornar valores padrão
            return {
                'table_name': table_name,
                'instance_name': instance_name,
                'table_size_gb': 10  # Placeholder
            }

        except Exception as e:
            logger.error(f"Erro ao buscar metadados da tabela {table_name}: {e}")
            return {}

    def _notify_plan_generated(
        self,
        plan: OptimizationPlan,
        evaluation: Dict[str, Any]
    ):
        """
        Notifica Teams sobre plano gerado.

        Args:
            plan: Plano gerado
            evaluation: Avaliação do approval engine
        """
        try:
            logger.info(f"Enviando notificação Teams sobre plano {plan.plan_id}")

            # Usar método específico para adaptive card (será implementado)
            self.teams_notifier.send_plan_generated_card(plan, evaluation)

        except Exception as e:
            logger.error(f"Erro ao enviar notificação Teams: {e}")

    def _notify_plan_executed(
        self,
        plan: OptimizationPlan,
        results: Dict[str, Any]
    ):
        """
        Notifica Teams sobre plano executado.

        Args:
            plan: Plano executado
            results: Resultados por instância
        """
        try:
            logger.info(f"Enviando notificação Teams sobre execução do plano {plan.plan_id}")

            self.teams_notifier.send_plan_executed_card(plan, results)

        except Exception as e:
            logger.error(f"Erro ao enviar notificação Teams: {e}")

    def _notify_impact_report(
        self,
        report: Any,
        summary: str
    ):
        """
        Notifica Teams sobre relatório de impacto.

        Args:
            report: Relatório de impacto
            summary: Resumo executivo
        """
        try:
            logger.info(f"Enviando relatório de impacto via Teams")

            self.teams_notifier.send_impact_report_card(report, summary)

        except Exception as e:
            logger.error(f"Erro ao enviar relatório via Teams: {e}")

    def run_job_now(self, job_name: str):
        """
        Executa um job manualmente (para testes).

        Args:
            job_name: Nome do job ('generate', 'execute', 'report')
        """
        logger.info(f"Executando job manualmente: {job_name}")

        if job_name == 'generate':
            self._job_generate_plan()
        elif job_name == 'execute':
            self._job_execute_plan()
        elif job_name == 'report':
            self._job_generate_report()
        else:
            logger.error(f"Job desconhecido: {job_name}")
