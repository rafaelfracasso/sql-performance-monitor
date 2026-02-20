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
    """Item de sugestão de otimização."""
    id: str
    type: str
    priority: str
    risk_level: str
    table: str
    description: str
    sql_script: str
    rollback_script: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OptimizationItem':
        """Cria a partir de dicionário."""
        if 'metadata' not in data:
            data['metadata'] = {}
            
        # Filtrar chaves extras
        valid_keys = cls.__dataclass_fields__.keys()
        filtered_data = {k: v for k, v in data.items() if k in valid_keys}
            
        return cls(**filtered_data)


@dataclass
class OptimizationPlan:
    """Relatório de sugestões de otimização."""
    plan_id: str
    generated_at: datetime
    analysis_period_days: int
    total_optimizations: int
    optimizations: List[OptimizationItem]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário."""
        return {
            'plan_id': self.plan_id,
            'generated_at': self.generated_at.isoformat(),
            'analysis_period_days': self.analysis_period_days,
            'total_optimizations': self.total_optimizations,
            'optimizations': [opt.to_dict() for opt in self.optimizations],
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OptimizationPlan':
        """Cria a partir de dicionário."""
        if isinstance(data.get('generated_at'), str):
            data['generated_at'] = datetime.fromisoformat(data['generated_at'])
            
        data['optimizations'] = [
            OptimizationItem.from_dict(opt) for opt in data.get('optimizations', [])
        ]
        if 'metadata' not in data:
            data['metadata'] = {}
            
        valid_keys = cls.__dataclass_fields__.keys()
        filtered_data = {k: v for k, v in data.items() if k in valid_keys}
            
        return cls(**filtered_data)


class PlanStateManager:
    """
    Gerenciador de estado de planos de otimização.
    Agora persiste no DuckDB via MetricsStore.
    """

    def __init__(
        self,
        metrics_store: Any, # MetricsStore
        veto_system: Optional[VetoSystem] = None
    ):
        """
        Inicializa gerenciador de estado.

        Args:
            metrics_store: Instância do MetricsStore
            veto_system: Sistema de veto
        """
        self.metrics_store = metrics_store
        self.veto_system = veto_system or VetoSystem(metrics_store=metrics_store)

    def save_plan(self, plan: OptimizationPlan) -> bool:
        """
        Salva um plano.

        Args:
            plan: Plano a salvar

        Returns:
            True se salvou com sucesso
        """
        # Converter objeto dataclass para dicionário compatível com MetricsStore
        plan_dict = plan.to_dict()
        return self.metrics_store.save_optimization_plan(plan_dict)

    def load_plan(self, plan_id: str) -> Optional[OptimizationPlan]:
        """
        Carrega um plano.

        Args:
            plan_id: ID do plano

        Returns:
            OptimizationPlan ou None se não encontrado
        """
        data = self.metrics_store.get_optimization_plan(plan_id)
        if not data:
            return None
            
        try:
            return OptimizationPlan.from_dict(data)
        except Exception as e:
            logger.error(f"Erro ao deserializar plano {plan_id}: {e}")
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
        limit: int = 50
    ) -> List[OptimizationPlan]:
        """
        Lista planos de sugestões.
        """
        summaries = self.metrics_store.list_optimization_plans(limit=limit)
        
        plans = []
        for s in summaries:
            p = self.load_plan(s['plan_id'])
            if p:
                plans.append(p)
                
        return plans

    def get_plan_summary(self, plan_id: str) -> Optional[Dict[str, Any]]:
        """
        Retorna resumo de um plano de sugestões.
        """
        plan = self.load_plan(plan_id)

        if not plan:
            return None

        return {
            'plan_id': plan.plan_id,
            'generated_at': plan.generated_at.isoformat(),
            'total_optimizations': plan.total_optimizations,
            'analysis_period_days': plan.analysis_period_days
        }

    def delete_plan(self, plan_id: str) -> bool:
        """
        Remove um plano.
        
        Args:
            plan_id: ID do plano
            
        Returns:
            True se sucesso
        """
        return self.metrics_store.delete_optimization_plan(plan_id)

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
        
        # Listar todos os planos (limit alto para pegar históricos)
        plans = self.list_plans(limit=1000)
        
        for plan in plans:
            if plan.generated_at < cutoff_date:
                if self.delete_plan(plan.plan_id):
                    removed += 1
                    logger.info(f"Plano antigo removido: {plan.plan_id}")
                    
        return removed
