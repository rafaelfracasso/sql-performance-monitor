"""
VetoSystem - Sistema de veto de otimizações via API REST.

Gerencia vetos granulares (plano completo ou itens específicos),
registra auditoria, e verifica janela de veto ativa.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass, asdict
import json
import logging
from pathlib import Path


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
        """Converte para dicionário."""
        d = asdict(self)
        d['vetoed_at'] = self.vetoed_at.isoformat()
        d['veto_expires_at'] = self.veto_expires_at.isoformat()
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'VetoRecord':
        """Cria a partir de dicionário."""
        data['vetoed_at'] = datetime.fromisoformat(data['vetoed_at'])
        data['veto_expires_at'] = datetime.fromisoformat(data['veto_expires_at'])
        return cls(**data)


class VetoSystem:
    """
    Sistema de gerenciamento de vetos de otimizações.
    """

    def __init__(self, storage_path: Optional[str] = None):
        """
        Inicializa sistema de veto.

        Args:
            storage_path: Caminho para arquivo de armazenamento de vetos
        """
        if storage_path is None:
            storage_path = "sql_monitor_data/vetos.json"

        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)

        # Carregar vetos existentes
        self.vetos: Dict[str, VetoRecord] = {}
        self._load_vetos()

    def _load_vetos(self):
        """Carrega vetos do arquivo."""
        if not self.storage_path.exists():
            logger.info("Arquivo de vetos não existe, criando novo")
            self._save_vetos()
            return

        try:
            with open(self.storage_path, 'r') as f:
                data = json.load(f)

            self.vetos = {
                veto_id: VetoRecord.from_dict(veto_data)
                for veto_id, veto_data in data.items()
            }

            logger.info(f"Carregados {len(self.vetos)} vetos do arquivo")

        except Exception as e:
            logger.error(f"Erro ao carregar vetos: {e}")
            self.vetos = {}

    def _save_vetos(self):
        """Salva vetos no arquivo."""
        try:
            data = {
                veto_id: veto.to_dict()
                for veto_id, veto in self.vetos.items()
            }

            with open(self.storage_path, 'w') as f:
                json.dump(data, f, indent=2)

            logger.debug(f"Vetos salvos em {self.storage_path}")

        except Exception as e:
            logger.error(f"Erro ao salvar vetos: {e}")

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

        self.vetos[veto_id] = veto
        self._save_vetos()

        logger.info(
            f"Plano {plan_id} vetado completamente por {vetoed_by}. "
            f"Razão: {reason}"
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
        Veta um item específico de um plano.

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

        # Se já existe veto parcial, adicionar item
        if veto_id in self.vetos:
            veto = self.vetos[veto_id]

            if item_id not in veto.vetoed_items:
                veto.vetoed_items.append(item_id)
                logger.info(f"Item {item_id} adicionado ao veto parcial do plano {plan_id}")
            else:
                logger.warning(f"Item {item_id} já estava vetado no plano {plan_id}")

        else:
            # Criar novo veto parcial
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

            self.vetos[veto_id] = veto

            logger.info(
                f"Item {item_id} do plano {plan_id} vetado por {vetoed_by}. "
                f"Razão: {reason}"
            )

        self._save_vetos()
        return veto

    def remove_veto(self, plan_id: str, veto_type: str = 'complete') -> bool:
        """
        Remove um veto completo.

        Args:
            plan_id: ID do plano
            veto_type: Tipo do veto ('complete' ou 'partial')

        Returns:
            True se removeu com sucesso
        """
        veto_id = f"{plan_id}_{veto_type}"

        if veto_id in self.vetos:
            del self.vetos[veto_id]
            self._save_vetos()

            logger.info(f"Veto {veto_type} removido do plano {plan_id}")
            return True

        logger.warning(f"Veto {veto_id} não encontrado")
        return False

    def remove_item_veto(self, plan_id: str, item_id: str) -> bool:
        """
        Remove veto de um item específico.

        Args:
            plan_id: ID do plano
            item_id: ID do item

        Returns:
            True se removeu com sucesso
        """
        veto_id = f"{plan_id}_partial"

        if veto_id not in self.vetos:
            logger.warning(f"Veto parcial não encontrado para plano {plan_id}")
            return False

        veto = self.vetos[veto_id]

        if item_id in veto.vetoed_items:
            veto.vetoed_items.remove(item_id)

            # Se não sobrou nenhum item vetado, remover veto parcial
            if not veto.vetoed_items:
                del self.vetos[veto_id]
                logger.info(f"Veto parcial removido do plano {plan_id} (sem itens restantes)")
            else:
                logger.info(f"Item {item_id} removido do veto parcial do plano {plan_id}")

            self._save_vetos()
            return True

        logger.warning(f"Item {item_id} não estava vetado no plano {plan_id}")
        return False

    def is_plan_vetoed(self, plan_id: str) -> bool:
        """
        Verifica se um plano está vetado (veto completo).

        Args:
            plan_id: ID do plano

        Returns:
            True se plano está vetado
        """
        veto_id = f"{plan_id}_complete"

        if veto_id not in self.vetos:
            return False

        veto = self.vetos[veto_id]

        # Verificar se veto ainda está ativo (não expirou)
        if not veto.active or datetime.now() >= veto.veto_expires_at:
            logger.info(f"Veto do plano {plan_id} expirou")
            veto.active = False
            self._save_vetos()
            return False

        return True

    def is_item_vetoed(self, plan_id: str, item_id: str) -> bool:
        """
        Verifica se um item específico está vetado.

        Args:
            plan_id: ID do plano
            item_id: ID do item

        Returns:
            True se item está vetado
        """
        # Verificar se plano completo está vetado
        if self.is_plan_vetoed(plan_id):
            return True

        # Verificar veto parcial
        veto_id = f"{plan_id}_partial"

        if veto_id not in self.vetos:
            return False

        veto = self.vetos[veto_id]

        # Verificar se veto ainda está ativo
        if not veto.active or datetime.now() >= veto.veto_expires_at:
            logger.info(f"Veto parcial do plano {plan_id} expirou")
            veto.active = False
            self._save_vetos()
            return False

        return item_id in veto.vetoed_items

    def get_plan_vetos(self, plan_id: str) -> List[VetoRecord]:
        """
        Obtém todos os vetos de um plano.

        Args:
            plan_id: ID do plano

        Returns:
            Lista de VetoRecord
        """
        vetos = []

        for veto in self.vetos.values():
            if veto.plan_id == plan_id and veto.active:
                # Verificar se não expirou
                if datetime.now() < veto.veto_expires_at:
                    vetos.append(veto)
                else:
                    veto.active = False

        if any(not v.active for v in self.vetos.values()):
            self._save_vetos()

        return vetos

    def get_vetoed_items(self, plan_id: str) -> List[str]:
        """
        Obtém lista de IDs de itens vetados de um plano.

        Args:
            plan_id: ID do plano

        Returns:
            Lista de IDs de itens vetados
        """
        # Se plano completo está vetado, todos os itens estão vetados
        if self.is_plan_vetoed(plan_id):
            return ['*']  # Convenção: '*' = todos

        # Buscar veto parcial
        veto_id = f"{plan_id}_partial"

        if veto_id not in self.vetos:
            return []

        veto = self.vetos[veto_id]

        if not veto.active or datetime.now() >= veto.veto_expires_at:
            return []

        return veto.vetoed_items.copy()

    def get_veto_window_status(
        self,
        plan_id: str,
        veto_expires_at: datetime
    ) -> Dict[str, Any]:
        """
        Obtém status da janela de veto.

        Args:
            plan_id: ID do plano
            veto_expires_at: Quando a janela expira

        Returns:
            Dicionário com status
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
        Remove vetos expirados.

        Returns:
            Número de vetos removidos
        """
        now = datetime.now()
        expired = []

        for veto_id, veto in self.vetos.items():
            if veto.active and now >= veto.veto_expires_at:
                veto.active = False
                expired.append(veto_id)

        if expired:
            logger.info(f"Desativados {len(expired)} vetos expirados")
            self._save_vetos()

        return len(expired)

    def get_all_active_vetos(self) -> List[VetoRecord]:
        """
        Retorna todos os vetos ativos.

        Returns:
            Lista de VetoRecord ativos
        """
        now = datetime.now()
        active = []

        for veto in self.vetos.values():
            if veto.active and now < veto.veto_expires_at:
                active.append(veto)

        return active

    def get_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas do sistema de veto.

        Returns:
            Dicionário com estatísticas
        """
        active_vetos = self.get_all_active_vetos()

        complete_vetos = sum(1 for v in active_vetos if v.veto_type == 'complete')
        partial_vetos = sum(1 for v in active_vetos if v.veto_type == 'partial')

        total_vetoed_items = sum(
            len(v.vetoed_items) for v in active_vetos if v.veto_type == 'partial'
        )

        return {
            'total_vetos': len(self.vetos),
            'active_vetos': len(active_vetos),
            'complete_vetos': complete_vetos,
            'partial_vetos': partial_vetos,
            'total_vetoed_items': total_vetoed_items,
            'inactive_vetos': len(self.vetos) - len(active_vetos)
        }
