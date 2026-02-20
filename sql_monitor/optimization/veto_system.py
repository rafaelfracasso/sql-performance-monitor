"""
VetoSystem - Sistema de veto de otimizacoes via API REST.

Gerencia vetos granulares (plano completo ou itens especificos),
registra auditoria, e verifica janela de veto ativa.

Persistencia via DuckDB (MetricsStore) - substitui armazenamento JSON.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass, asdict
import json
import logging


logger = logging.getLogger(__name__)


@dataclass
class VetoRecord:
    """Registro de veto."""
    veto_id: str
    plan_id: str
    veto_type: str  # 'complete' ou 'partial'
    vetoed_at: datetime
    vetoed_by: str
    veto_reason: str
    vetoed_items: List[str]  # IDs dos itens vetados (vazio se veto completo)
    veto_expires_at: datetime
    active: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionario."""
        d = asdict(self)
        d['vetoed_at'] = self.vetoed_at.isoformat() if isinstance(self.vetoed_at, datetime) else str(self.vetoed_at)
        d['veto_expires_at'] = self.veto_expires_at.isoformat() if isinstance(self.veto_expires_at, datetime) else str(self.veto_expires_at)
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'VetoRecord':
        """Cria a partir de dicionario."""
        if isinstance(data.get('vetoed_at'), str):
            data['vetoed_at'] = datetime.fromisoformat(data['vetoed_at'])
        if isinstance(data.get('veto_expires_at'), str):
            data['veto_expires_at'] = datetime.fromisoformat(data['veto_expires_at'])
        # Garantir que vetoed_items e lista
        if isinstance(data.get('vetoed_items'), str):
            try:
                data['vetoed_items'] = json.loads(data['vetoed_items'])
            except (ValueError, TypeError):
                data['vetoed_items'] = []
        elif data.get('vetoed_items') is None:
            data['vetoed_items'] = []
        # Filtrar chaves validas
        valid_keys = {f.name for f in cls.__dataclass_fields__.values()} if hasattr(cls, '__dataclass_fields__') else set()
        if valid_keys:
            data = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**data)


class VetoSystem:
    """
    Sistema de gerenciamento de vetos de otimizacoes.
    Persistencia via DuckDB (MetricsStore).
    """

    def __init__(self, metrics_store=None):
        """
        Inicializa sistema de veto.

        Args:
            metrics_store: Instancia do MetricsStore para persistencia DuckDB
        """
        self.metrics_store = metrics_store

    def _get_veto(self, veto_id: str) -> Optional[VetoRecord]:
        """Busca um veto especifico pelo ID."""
        if not self.metrics_store:
            return None
        rows = self.metrics_store.execute_query(
            "SELECT veto_id, plan_id, veto_type, vetoed_at, vetoed_by, "
            "veto_reason, vetoed_items, veto_expires_at, active "
            "FROM veto_records WHERE veto_id = ?",
            (veto_id,)
        )
        if not rows:
            return None
        d = self.metrics_store._veto_row_to_dict(rows[0])
        return VetoRecord.from_dict(d)

    def veto_plan(
        self,
        plan_id: str,
        vetoed_by: str,
        reason: str,
        veto_expires_at: datetime
    ) -> VetoRecord:
        """
        Veta um plano completo.

        Args:
            plan_id: ID do plano
            vetoed_by: Quem vetou (email/nome)
            reason: Motivo do veto
            veto_expires_at: Quando o veto expira

        Returns:
            VetoRecord criado
        """
        veto_id = f"{plan_id}_complete"

        veto = VetoRecord(
            veto_id=veto_id,
            plan_id=plan_id,
            veto_type='complete',
            vetoed_at=datetime.now(),
            vetoed_by=vetoed_by,
            veto_reason=reason,
            vetoed_items=[],
            veto_expires_at=veto_expires_at,
            active=True
        )

        if self.metrics_store:
            self.metrics_store.save_veto(veto.to_dict())

        logger.info(
            f"Plano {plan_id} vetado completamente por {vetoed_by}. "
            f"Razao: {reason}"
        )

        return veto

    def veto_item(
        self,
        plan_id: str,
        item_id: str,
        vetoed_by: str,
        reason: str,
        veto_expires_at: datetime
    ) -> VetoRecord:
        """
        Veta um item especifico de um plano.

        Args:
            plan_id: ID do plano
            item_id: ID do item a vetar
            vetoed_by: Quem vetou
            reason: Motivo do veto
            veto_expires_at: Quando o veto expira

        Returns:
            VetoRecord criado ou atualizado
        """
        veto_id = f"{plan_id}_partial"

        existing = self._get_veto(veto_id)

        if existing:
            if item_id not in existing.vetoed_items:
                existing.vetoed_items.append(item_id)
                if self.metrics_store:
                    self.metrics_store.update_veto_items(veto_id, existing.vetoed_items)
                logger.info(f"Item {item_id} adicionado ao veto parcial do plano {plan_id}")
            else:
                logger.warning(f"Item {item_id} ja estava vetado no plano {plan_id}")
            return existing
        else:
            veto = VetoRecord(
                veto_id=veto_id,
                plan_id=plan_id,
                veto_type='partial',
                vetoed_at=datetime.now(),
                vetoed_by=vetoed_by,
                veto_reason=reason,
                vetoed_items=[item_id],
                veto_expires_at=veto_expires_at,
                active=True
            )

            if self.metrics_store:
                self.metrics_store.save_veto(veto.to_dict())

            logger.info(
                f"Item {item_id} do plano {plan_id} vetado por {vetoed_by}. "
                f"Razao: {reason}"
            )

            return veto

    def remove_veto(self, plan_id: str, veto_type: str = 'complete') -> bool:
        """
        Remove um veto.

        Args:
            plan_id: ID do plano
            veto_type: Tipo do veto ('complete' ou 'partial')

        Returns:
            True se removeu com sucesso
        """
        veto_id = f"{plan_id}_{veto_type}"

        if self.metrics_store:
            success = self.metrics_store.delete_veto(veto_id)
            if success:
                logger.info(f"Veto {veto_type} removido do plano {plan_id}")
                return True

        logger.warning(f"Veto {veto_id} nao encontrado")
        return False

    def remove_item_veto(self, plan_id: str, item_id: str) -> bool:
        """
        Remove veto de um item especifico.

        Args:
            plan_id: ID do plano
            item_id: ID do item

        Returns:
            True se removeu com sucesso
        """
        veto_id = f"{plan_id}_partial"

        existing = self._get_veto(veto_id)
        if not existing:
            logger.warning(f"Veto parcial nao encontrado para plano {plan_id}")
            return False

        if item_id in existing.vetoed_items:
            existing.vetoed_items.remove(item_id)

            if not existing.vetoed_items:
                if self.metrics_store:
                    self.metrics_store.delete_veto(veto_id)
                logger.info(f"Veto parcial removido do plano {plan_id} (sem itens restantes)")
            else:
                if self.metrics_store:
                    self.metrics_store.update_veto_items(veto_id, existing.vetoed_items)
                logger.info(f"Item {item_id} removido do veto parcial do plano {plan_id}")

            return True

        logger.warning(f"Item {item_id} nao estava vetado no plano {plan_id}")
        return False

    def is_plan_vetoed(self, plan_id: str) -> bool:
        """
        Verifica se um plano esta vetado (veto completo).

        Args:
            plan_id: ID do plano

        Returns:
            True se plano esta vetado
        """
        veto_id = f"{plan_id}_complete"

        veto = self._get_veto(veto_id)
        if not veto:
            return False

        if not veto.active or datetime.now() >= veto.veto_expires_at:
            logger.info(f"Veto do plano {plan_id} expirou")
            if self.metrics_store:
                self.metrics_store.save_veto({
                    **veto.to_dict(),
                    'active': False
                })
            return False

        return True

    def is_item_vetoed(self, plan_id: str, item_id: str) -> bool:
        """
        Verifica se um item especifico esta vetado.

        Args:
            plan_id: ID do plano
            item_id: ID do item

        Returns:
            True se item esta vetado
        """
        if self.is_plan_vetoed(plan_id):
            return True

        veto_id = f"{plan_id}_partial"
        veto = self._get_veto(veto_id)

        if not veto:
            return False

        if not veto.active or datetime.now() >= veto.veto_expires_at:
            logger.info(f"Veto parcial do plano {plan_id} expirou")
            if self.metrics_store:
                self.metrics_store.save_veto({
                    **veto.to_dict(),
                    'active': False
                })
            return False

        return item_id in veto.vetoed_items

    def get_plan_vetos(self, plan_id: str) -> List[VetoRecord]:
        """
        Obtem todos os vetos de um plano.

        Args:
            plan_id: ID do plano

        Returns:
            Lista de VetoRecord
        """
        if not self.metrics_store:
            return []

        veto_dicts = self.metrics_store.get_vetos_for_plan(plan_id)
        vetos = []
        for d in veto_dicts:
            try:
                vetos.append(VetoRecord.from_dict(d))
            except Exception as e:
                logger.error(f"Erro ao deserializar veto: {e}")

        return vetos

    def get_vetoed_items(self, plan_id: str) -> List[str]:
        """
        Obtem lista de IDs de itens vetados de um plano.

        Args:
            plan_id: ID do plano

        Returns:
            Lista de IDs de itens vetados
        """
        if self.is_plan_vetoed(plan_id):
            return ['*']

        veto_id = f"{plan_id}_partial"
        veto = self._get_veto(veto_id)

        if not veto:
            return []

        if not veto.active or datetime.now() >= veto.veto_expires_at:
            return []

        return veto.vetoed_items.copy()

    def get_veto_window_status(
        self,
        plan_id: str,
        veto_expires_at: datetime
    ) -> Dict[str, Any]:
        """
        Obtem status da janela de veto.

        Args:
            plan_id: ID do plano
            veto_expires_at: Quando a janela expira

        Returns:
            Dicionario com status
        """
        now = datetime.now()
        is_active = now < veto_expires_at
        is_vetoed = self.is_plan_vetoed(plan_id)

        hours_remaining = 0
        if is_active:
            hours_remaining = (veto_expires_at - now).total_seconds() / 3600

        vetos = self.get_plan_vetos(plan_id)

        return {
            'plan_id': plan_id,
            'veto_window_active': is_active,
            'veto_expires_at': veto_expires_at.isoformat(),
            'hours_remaining': hours_remaining,
            'is_vetoed': is_vetoed,
            'veto_count': len(vetos),
            'vetoed_items': self.get_vetoed_items(plan_id),
            'vetos': [v.to_dict() for v in vetos]
        }

    def cleanup_expired_vetos(self) -> int:
        """
        Desativa vetos expirados.

        Returns:
            Numero de vetos desativados
        """
        if not self.metrics_store:
            return 0

        count = self.metrics_store.cleanup_expired_vetos()
        if count:
            logger.info(f"Desativados {count} vetos expirados")
        return count

    def get_all_active_vetos(self) -> List[VetoRecord]:
        """
        Retorna todos os vetos ativos.

        Returns:
            Lista de VetoRecord ativos
        """
        if not self.metrics_store:
            return []

        veto_dicts = self.metrics_store.get_all_active_vetos()
        vetos = []
        for d in veto_dicts:
            try:
                vetos.append(VetoRecord.from_dict(d))
            except Exception as e:
                logger.error(f"Erro ao deserializar veto: {e}")

        return vetos

    def get_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatisticas do sistema de veto.

        Returns:
            Dicionario com estatisticas
        """
        active_vetos = self.get_all_active_vetos()

        complete_vetos = sum(1 for v in active_vetos if v.veto_type == 'complete')
        partial_vetos = sum(1 for v in active_vetos if v.veto_type == 'partial')

        total_vetoed_items = sum(
            len(v.vetoed_items) for v in active_vetos if v.veto_type == 'partial'
        )

        # Contar total incluindo inativos
        total = 0
        if self.metrics_store:
            try:
                result = self.metrics_store.execute_query("SELECT COUNT(*) FROM veto_records")
                total = result[0][0] if result else 0
            except Exception:
                total = len(active_vetos)

        return {
            'total_vetos': total,
            'active_vetos': len(active_vetos),
            'complete_vetos': complete_vetos,
            'partial_vetos': partial_vetos,
            'total_vetoed_items': total_vetoed_items,
            'inactive_vetos': total - len(active_vetos)
        }
