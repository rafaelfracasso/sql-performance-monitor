"""
AutoApprovalEngine - Motor de aprovação automática de otimizações.

Decide automaticamente quais otimizações podem ser executadas com base
no nível de risco classificado pelo RiskClassifier.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging

from .risk_classifier import RiskClassifier, RiskLevel, RiskAssessment


logger = logging.getLogger(__name__)


class ApprovalDecision:
    """Decisão de aprovação para uma otimização."""

    def __init__(
        self,
        approved: bool,
        auto_approved: bool,
        risk_assessment: RiskAssessment,
        decision_reason: str,
        requires_veto_window: bool = False,
        execution_allowed: bool = False
    ):
        self.approved = approved
        self.auto_approved = auto_approved
        self.risk_assessment = risk_assessment
        self.decision_reason = decision_reason
        self.requires_veto_window = requires_veto_window
        self.execution_allowed = execution_allowed
        self.decided_at = datetime.now()


class AutoApprovalEngine:
    """
    Motor de aprovação automática baseado em classificação de risco.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Inicializa motor de aprovação.

        Args:
            config: Configurações do motor
        """
        self.config = config or {}
        self.risk_classifier = RiskClassifier(
            config=self.config.get('risk_thresholds', {})
        )

        # Configurações de aprovação automática por nível de risco
        self.auto_approval_rules = {
            RiskLevel.LOW: {
                'auto_approve': True,
                'notify': False,
                'veto_window': False,
                'execute_immediately': True
            },
            RiskLevel.MEDIUM: {
                'auto_approve': True,
                'notify': True,
                'veto_window': False,
                'execute_immediately': True
            },
            RiskLevel.HIGH: {
                'auto_approve': True,
                'notify': True,
                'veto_window': True,
                'execute_immediately': False  # Aguarda janela de veto
            },
            RiskLevel.CRITICAL: {
                'auto_approve': False,
                'notify': True,
                'veto_window': True,
                'execute_immediately': False
            }
        }

    def evaluate_optimization(
        self,
        optimization: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> ApprovalDecision:
        """
        Avalia uma otimização e decide se deve ser aprovada automaticamente.

        Args:
            optimization: Dados da otimização
            metadata: Metadados adicionais

        Returns:
            ApprovalDecision com a decisão
        """
        # Classificar risco
        risk_assessment = self.risk_classifier.classify_optimization(
            optimization, metadata
        )

        # Obter regras para este nível de risco
        rules = self.auto_approval_rules[risk_assessment.risk_level]

        # Decidir aprovação
        approved = rules['auto_approve'] and risk_assessment.auto_approved
        requires_veto_window = rules['veto_window']
        execution_allowed = rules['execute_immediately'] and approved

        # Razão da decisão
        if not approved:
            decision_reason = (
                f"Risco {risk_assessment.risk_level.value.upper()}: "
                f"{risk_assessment.reason}. Requer aprovação manual."
            )
        elif requires_veto_window:
            decision_reason = (
                f"Auto-aprovado com risco {risk_assessment.risk_level.value.upper()}. "
                f"Aguardando janela de veto de 72h."
            )
        else:
            decision_reason = (
                f"Auto-aprovado com risco {risk_assessment.risk_level.value.upper()}. "
                f"Pode executar imediatamente."
            )

        logger.info(
            f"Otimização '{optimization.get('id')}' - "
            f"Risco: {risk_assessment.risk_level.value}, "
            f"Aprovada: {approved}, "
            f"Auto: {rules['auto_approve']}"
        )

        return ApprovalDecision(
            approved=approved,
            auto_approved=rules['auto_approve'],
            risk_assessment=risk_assessment,
            decision_reason=decision_reason,
            requires_veto_window=requires_veto_window,
            execution_allowed=execution_allowed
        )

    def evaluate_plan(
        self,
        optimizations: List[Dict[str, Any]],
        metadata_map: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Avalia um plano completo de otimizações.

        Args:
            optimizations: Lista de otimizações
            metadata_map: Mapa de opt_id -> metadata

        Returns:
            Dicionário com decisões e estatísticas
        """
        metadata_map = metadata_map or {}
        decisions = {}
        stats = {
            'total': len(optimizations),
            'auto_approved': 0,
            'requires_review': 0,
            'requires_veto_window': 0,
            'can_execute_immediately': 0,
            'blocked': 0,
            'by_risk_level': {
                'low': 0,
                'medium': 0,
                'high': 0,
                'critical': 0
            }
        }

        for opt in optimizations:
            opt_id = opt.get('id')
            metadata = metadata_map.get(opt_id, {})

            decision = self.evaluate_optimization(opt, metadata)
            decisions[opt_id] = decision

            # Atualizar estatísticas
            risk_level = decision.risk_assessment.risk_level.value
            stats['by_risk_level'][risk_level] += 1

            if decision.auto_approved:
                stats['auto_approved'] += 1
            else:
                stats['requires_review'] += 1

            if decision.requires_veto_window:
                stats['requires_veto_window'] += 1

            if decision.execution_allowed:
                stats['can_execute_immediately'] += 1

            if not decision.approved:
                stats['blocked'] += 1

        logger.info(
            f"Plano avaliado: {stats['total']} otimizações, "
            f"{stats['auto_approved']} auto-aprovadas, "
            f"{stats['blocked']} bloqueadas"
        )

        return {
            'decisions': decisions,
            'statistics': stats,
            'evaluated_at': datetime.now().isoformat()
        }

    def should_notify(self, decision: ApprovalDecision) -> bool:
        """
        Verifica se deve enviar notificação para esta decisão.

        Args:
            decision: Decisão de aprovação

        Returns:
            True se deve notificar
        """
        risk_level = decision.risk_assessment.risk_level
        rules = self.auto_approval_rules[risk_level]
        return rules['notify']

    def get_notification_priority(self, decision: ApprovalDecision) -> str:
        """
        Retorna prioridade da notificação.

        Args:
            decision: Decisão de aprovação

        Returns:
            Prioridade: 'low', 'normal', 'high', 'critical'
        """
        risk_level = decision.risk_assessment.risk_level

        if risk_level == RiskLevel.CRITICAL:
            return 'critical'
        elif risk_level == RiskLevel.HIGH:
            return 'high'
        elif risk_level == RiskLevel.MEDIUM:
            return 'normal'
        else:
            return 'low'

    def calculate_execution_window(
        self,
        plan_generated_at: datetime,
        veto_window_hours: int = 72
    ) -> Dict[str, Any]:
        """
        Calcula janela de execução baseada na janela de veto.

        Args:
            plan_generated_at: Data/hora de geração do plano
            veto_window_hours: Horas de janela de veto (padrão 72h)

        Returns:
            Dicionário com datas de expiração e execução
        """
        veto_expires_at = plan_generated_at + timedelta(hours=veto_window_hours)

        # Por padrão, executar no próximo domingo às 02:00 após janela de veto
        days_until_sunday = (6 - plan_generated_at.weekday()) % 7
        if days_until_sunday == 0:
            days_until_sunday = 7  # Próximo domingo, não hoje

        execution_date = plan_generated_at + timedelta(days=days_until_sunday)
        execution_date = execution_date.replace(hour=2, minute=0, second=0, microsecond=0)

        # Se a janela de veto vai além da data de execução, ajustar
        if veto_expires_at > execution_date:
            # Executar 1 hora após veto expirar
            execution_date = veto_expires_at + timedelta(hours=1)

        hours_until_execution = (execution_date - plan_generated_at).total_seconds() / 3600

        return {
            'plan_generated_at': plan_generated_at,
            'veto_window_hours': veto_window_hours,
            'veto_expires_at': veto_expires_at,
            'execution_scheduled_at': execution_date,
            'hours_until_execution': hours_until_execution,
            'veto_window_active': datetime.now() < veto_expires_at
        }

    def generate_approval_summary(
        self,
        plan_evaluation: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Gera resumo para notificação.

        Args:
            plan_evaluation: Resultado de evaluate_plan()

        Returns:
            Resumo formatado para notificação
        """
        stats = plan_evaluation['statistics']
        decisions = plan_evaluation['decisions']

        # Agrupar otimizações por categoria
        categories = {
            'auto_approved_low_risk': [],
            'auto_approved_medium_risk': [],
            'auto_approved_high_risk': [],
            'requires_manual_approval': []
        }

        for opt_id, decision in decisions.items():
            risk = decision.risk_assessment.risk_level

            if not decision.approved:
                categories['requires_manual_approval'].append(opt_id)
            elif risk == RiskLevel.LOW:
                categories['auto_approved_low_risk'].append(opt_id)
            elif risk == RiskLevel.MEDIUM:
                categories['auto_approved_medium_risk'].append(opt_id)
            elif risk == RiskLevel.HIGH:
                categories['auto_approved_high_risk'].append(opt_id)

        return {
            'total_optimizations': stats['total'],
            'auto_approved': stats['auto_approved'],
            'requires_review': stats['requires_review'],
            'requires_veto_window': stats['requires_veto_window'],
            'can_execute_immediately': stats['can_execute_immediately'],
            'blocked': stats['blocked'],
            'risk_distribution': stats['by_risk_level'],
            'categories': categories,
            'evaluated_at': plan_evaluation['evaluated_at']
        }
