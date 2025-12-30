"""
PlanStateManager - Gerenciador de estado de planos de otimização.

Armazena e recupera planos gerados, com histórico completo e
integração com VetoSystem.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict, field
import json
import logging
from pathlib import Path

from .veto_system import VetoSystem


logger = logging.getLogger(__name__)


@dataclass
class OptimizationItem:
    """Item de otimização dentro de um plano."""
    id: str
    type: str
    priority: str
    risk_level: str
    table: str
    description: str
    estimated_improvement_percent: float
    estimated_duration_minutes: int
    sql_script: str
    rollback_script: Optional[str]
    auto_approved: bool
    approved: Optional[bool] = None
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    vetoed: bool = False
    vetoed_by: Optional[str] = None
    vetoed_at: Optional[datetime] = None
    veto_reason: Optional[str] = None
    execution_status: str = "pending"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário."""
        d = asdict(self)
        if self.approved_at:
            d['approved_at'] = self.approved_at.isoformat()
        if self.vetoed_at:
            d['vetoed_at'] = self.vetoed_at.isoformat()
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OptimizationItem':
        """Cria a partir de dicionário."""
        if data.get('approved_at'):
            data['approved_at'] = datetime.fromisoformat(data['approved_at'])
        if data.get('vetoed_at'):
            data['vetoed_at'] = datetime.fromisoformat(data['vetoed_at'])
        if 'metadata' not in data:
            data['metadata'] = {}
        return cls(**data)


@dataclass
class OptimizationPlan:
    """Plano completo de otimização."""
    plan_id: str
    generated_at: datetime
    execution_scheduled_at: datetime
    analysis_period_days: int
    status: str  # pending, approved, vetoed, executing, completed, failed
    veto_window_expires_at: datetime
    total_optimizations: int
    auto_approved_count: int
    requires_review_count: int
    blocked_count: int
    optimizations: List[OptimizationItem]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário."""
        return {
            'plan_id': self.plan_id,
            'generated_at': self.generated_at.isoformat(),
            'execution_scheduled_at': self.execution_scheduled_at.isoformat(),
            'analysis_period_days': self.analysis_period_days,
            'status': self.status,
            'veto_window_expires_at': self.veto_window_expires_at.isoformat(),
            'total_optimizations': self.total_optimizations,
            'auto_approved_count': self.auto_approved_count,
            'requires_review_count': self.requires_review_count,
            'blocked_count': self.blocked_count,
            'optimizations': [opt.to_dict() for opt in self.optimizations],
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OptimizationPlan':
        """Cria a partir de dicionário."""
        data['generated_at'] = datetime.fromisoformat(data['generated_at'])
        data['execution_scheduled_at'] = datetime.fromisoformat(data['execution_scheduled_at'])
        data['veto_window_expires_at'] = datetime.fromisoformat(data['veto_window_expires_at'])
        data['optimizations'] = [
            OptimizationItem.from_dict(opt) for opt in data['optimizations']
        ]
        if 'metadata' not in data:
            data['metadata'] = {}
        return cls(**data)


class PlanStateManager:
    """
    Gerenciador de estado de planos de otimização.
    """

    def __init__(
        self,
        storage_path: Optional[str] = None,
        veto_system: Optional[VetoSystem] = None
    ):
        """
        Inicializa gerenciador de estado.

        Args:
            storage_path: Caminho base para armazenamento de planos
            veto_system: Sistema de veto (será criado se não fornecido)
        """
        if storage_path is None:
            storage_path = "sql_monitor_data/plans"

        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)

        self.veto_system = veto_system or VetoSystem()

        # Cache de planos em memória
        self._plans_cache: Dict[str, OptimizationPlan] = {}
        self._load_plans_index()

    def _load_plans_index(self):
        """Carrega índice de planos existentes."""
        plan_files = list(self.storage_path.glob("*.json"))
        logger.info(f"Encontrados {len(plan_files)} planos no disco")

        for plan_file in plan_files:
            try:
                plan = self.load_plan(plan_file.stem)
                if plan:
                    self._plans_cache[plan.plan_id] = plan
            except Exception as e:
                logger.error(f"Erro ao carregar plano {plan_file}: {e}")

    def save_plan(self, plan: OptimizationPlan) -> bool:
        """
        Salva um plano em disco.

        Args:
            plan: Plano a salvar

        Returns:
            True se salvou com sucesso
        """
        plan_file = self.storage_path / f"{plan.plan_id}.json"

        try:
            with open(plan_file, 'w') as f:
                json.dump(plan.to_dict(), f, indent=2)

            self._plans_cache[plan.plan_id] = plan
            logger.info(f"Plano {plan.plan_id} salvo com sucesso")
            return True

        except Exception as e:
            logger.error(f"Erro ao salvar plano {plan.plan_id}: {e}")
            return False

    def load_plan(self, plan_id: str) -> Optional[OptimizationPlan]:
        """
        Carrega um plano do disco.

        Args:
            plan_id: ID do plano

        Returns:
            OptimizationPlan ou None se não encontrado
        """
        # Verificar cache primeiro
        if plan_id in self._plans_cache:
            return self._plans_cache[plan_id]

        plan_file = self.storage_path / f"{plan_id}.json"

        if not plan_file.exists():
            logger.warning(f"Plano {plan_id} não encontrado")
            return None

        try:
            with open(plan_file, 'r') as f:
                data = json.load(f)

            plan = OptimizationPlan.from_dict(data)
            self._plans_cache[plan_id] = plan

            logger.debug(f"Plano {plan_id} carregado do disco")
            return plan

        except Exception as e:
            logger.error(f"Erro ao carregar plano {plan_id}: {e}")
            return None

    def get_plan(self, plan_id: str, sync_vetos: bool = True) -> Optional[OptimizationPlan]:
        """
        Obtém um plano e sincroniza com vetos se solicitado.

        Args:
            plan_id: ID do plano
            sync_vetos: Se True, atualiza status de vetos

        Returns:
            OptimizationPlan ou None
        """
        plan = self.load_plan(plan_id)

        if not plan:
            return None

        if sync_vetos:
            self._sync_plan_with_vetos(plan)

        return plan

    def _sync_plan_with_vetos(self, plan: OptimizationPlan):
        """
        Sincroniza estado do plano com sistema de vetos.

        Args:
            plan: Plano a sincronizar
        """
        # Verificar se plano completo está vetado
        if self.veto_system.is_plan_vetoed(plan.plan_id):
            plan.status = 'vetoed'

        # Sincronizar vetos de itens individuais
        vetoed_items = self.veto_system.get_vetoed_items(plan.plan_id)

        if vetoed_items == ['*']:
            # Todos os itens estão vetados
            for opt in plan.optimizations:
                opt.vetoed = True
        else:
            # Atualizar itens específicos
            for opt in plan.optimizations:
                opt.vetoed = self.veto_system.is_item_vetoed(plan.plan_id, opt.id)

    def list_plans(
        self,
        limit: Optional[int] = None,
        status_filter: Optional[str] = None
    ) -> List[OptimizationPlan]:
        """
        Lista todos os planos.

        Args:
            limit: Limite de resultados
            status_filter: Filtrar por status

        Returns:
            Lista de OptimizationPlan
        """
        plans = list(self._plans_cache.values())

        # Filtrar por status
        if status_filter:
            plans = [p for p in plans if p.status == status_filter]

        # Ordenar por data de geração (mais recente primeiro)
        plans.sort(key=lambda p: p.generated_at, reverse=True)

        # Limitar resultados
        if limit:
            plans = plans[:limit]

        return plans

    def update_plan_status(self, plan_id: str, status: str) -> bool:
        """
        Atualiza status de um plano.

        Args:
            plan_id: ID do plano
            status: Novo status

        Returns:
            True se atualizou com sucesso
        """
        plan = self.load_plan(plan_id)

        if not plan:
            return False

        plan.status = status
        return self.save_plan(plan)

    def update_optimization_status(
        self,
        plan_id: str,
        opt_id: str,
        status: str
    ) -> bool:
        """
        Atualiza status de uma otimização específica.

        Args:
            plan_id: ID do plano
            opt_id: ID da otimização
            status: Novo status

        Returns:
            True se atualizou com sucesso
        """
        plan = self.load_plan(plan_id)

        if not plan:
            return False

        for opt in plan.optimizations:
            if opt.id == opt_id:
                opt.execution_status = status
                return self.save_plan(plan)

        logger.warning(f"Otimização {opt_id} não encontrada no plano {plan_id}")
        return False

    def approve_plan(
        self,
        plan_id: str,
        approved_by: str,
        execute_now: bool = False
    ) -> bool:
        """
        Aprova um plano para execução.

        Args:
            plan_id: ID do plano
            approved_by: Quem aprovou
            execute_now: Se True, marca para execução imediata

        Returns:
            True se aprovou com sucesso
        """
        plan = self.load_plan(plan_id)

        if not plan:
            return False

        plan.status = 'approved'

        if execute_now:
            plan.execution_scheduled_at = datetime.now()
            plan.status = 'ready_to_execute'

        # Marcar todas otimizações como aprovadas
        for opt in plan.optimizations:
            if not opt.vetoed:
                opt.approved = True
                opt.approved_by = approved_by
                opt.approved_at = datetime.now()

        logger.info(f"Plano {plan_id} aprovado por {approved_by}")
        return self.save_plan(plan)

    def get_plan_summary(self, plan_id: str) -> Optional[Dict[str, Any]]:
        """
        Retorna resumo de um plano.

        Args:
            plan_id: ID do plano

        Returns:
            Dicionário com resumo
        """
        plan = self.get_plan(plan_id, sync_vetos=True)

        if not plan:
            return None

        vetoed_count = sum(1 for opt in plan.optimizations if opt.vetoed)
        approved_count = sum(1 for opt in plan.optimizations if opt.approved)
        pending_count = sum(
            1 for opt in plan.optimizations
            if not opt.vetoed and not opt.approved
        )

        veto_status = self.veto_system.get_veto_window_status(
            plan_id, plan.veto_window_expires_at
        )

        return {
            'plan_id': plan.plan_id,
            'status': plan.status,
            'generated_at': plan.generated_at.isoformat(),
            'execution_scheduled_at': plan.execution_scheduled_at.isoformat(),
            'total_optimizations': plan.total_optimizations,
            'auto_approved_count': plan.auto_approved_count,
            'requires_review_count': plan.requires_review_count,
            'blocked_count': plan.blocked_count,
            'vetoed_count': vetoed_count,
            'approved_count': approved_count,
            'pending_count': pending_count,
            'veto_window_status': veto_status
        }

    def cleanup_old_plans(self, keep_days: int = 90) -> int:
        """
        Remove planos antigos.

        Args:
            keep_days: Manter planos dos últimos N dias

        Returns:
            Número de planos removidos
        """
        cutoff_date = datetime.now() - timedelta(days=keep_days)
        removed = 0

        for plan in list(self._plans_cache.values()):
            if plan.generated_at < cutoff_date and plan.status in ['completed', 'failed', 'vetoed']:
                plan_file = self.storage_path / f"{plan.plan_id}.json"

                try:
                    plan_file.unlink()
                    del self._plans_cache[plan.plan_id]
                    removed += 1
                    logger.info(f"Plano antigo removido: {plan.plan_id}")
                except Exception as e:
                    logger.error(f"Erro ao remover plano {plan.plan_id}: {e}")

        return removed
