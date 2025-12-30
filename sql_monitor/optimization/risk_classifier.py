"""
RiskClassifier - Classificação de risco de otimizações.

Classifica otimizações em 4 níveis:
- LOW: Auto-aprovado sem notificação
- MEDIUM: Auto-aprovado com notificação
- HIGH: Auto-aprovado com notificação e janela de veto
- CRITICAL: Bloqueado para execução automática
"""
from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass
import re


class RiskLevel(Enum):
    """Níveis de risco."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class RiskAssessment:
    """Resultado da avaliação de risco."""
    risk_level: RiskLevel
    auto_approved: bool
    requires_notification: bool
    requires_veto_window: bool
    reason: str
    recommendations: list[str]


class RiskClassifier:
    """
    Classificador de risco para otimizações de banco de dados.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Inicializa classificador.

        Args:
            config: Configurações de thresholds de risco
        """
        self.config = config or {}

        # Thresholds padrão (podem ser substituídos por config)
        self.table_size_thresholds = {
            'medium_gb': self.config.get('table_size_gb_medium', 100),
            'high_gb': self.config.get('table_size_gb_high', 500),
            'critical_gb': self.config.get('table_size_gb_critical', 1000)
        }

        self.index_fragmentation_threshold = self.config.get(
            'index_fragmentation_percent', 50
        )

        self.max_execution_time_minutes = self.config.get(
            'max_execution_time_minutes', 240
        )

    def classify_optimization(
        self,
        optimization: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> RiskAssessment:
        """
        Classifica uma otimização baseado em tipo, tamanho, e contexto.

        Args:
            optimization: Dicionário com dados da otimização
            metadata: Metadados adicionais (tamanho da tabela, etc)

        Returns:
            RiskAssessment com nível de risco e recomendações
        """
        opt_type = optimization.get('type', '').lower()
        table_name = optimization.get('table', '')
        metadata = metadata or {}

        # Classificar por tipo
        if opt_type == 'update_statistics':
            return self._classify_update_statistics(optimization, metadata)

        elif opt_type == 'create_index':
            return self._classify_create_index(optimization, metadata)

        elif opt_type == 'rebuild_index':
            return self._classify_rebuild_index(optimization, metadata)

        elif opt_type == 'reorganize_index':
            return self._classify_reorganize_index(optimization, metadata)

        elif opt_type == 'vacuum_analyze':
            return self._classify_vacuum_analyze(optimization, metadata)

        elif opt_type == 'delta_merge':
            return self._classify_delta_merge(optimization, metadata)

        elif opt_type == 'query_rewrite':
            return self._classify_query_rewrite(optimization, metadata)

        elif opt_type == 'drop_index':
            return self._classify_drop_index(optimization, metadata)

        elif opt_type == 'alter_table':
            return self._classify_alter_table(optimization, metadata)

        else:
            # Tipo desconhecido = CRITICAL por segurança
            return RiskAssessment(
                risk_level=RiskLevel.CRITICAL,
                auto_approved=False,
                requires_notification=True,
                requires_veto_window=True,
                reason=f"Tipo de otimização desconhecido: {opt_type}",
                recommendations=["Revisar manualmente antes de executar"]
            )

    def _classify_update_statistics(
        self,
        optimization: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> RiskAssessment:
        """Classifica UPDATE STATISTICS."""
        # UPDATE STATISTICS é geralmente seguro e rápido
        return RiskAssessment(
            risk_level=RiskLevel.LOW,
            auto_approved=True,
            requires_notification=False,
            requires_veto_window=False,
            reason="UPDATE STATISTICS é operação de baixo risco e impacto",
            recommendations=[
                "Executa rapidamente",
                "Melhora planos de execução",
                "Não bloqueia tabelas"
            ]
        )

    def _classify_create_index(
        self,
        optimization: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> RiskAssessment:
        """Classifica CREATE INDEX."""
        table_size_gb = metadata.get('table_size_gb', 0)
        sql_script = optimization.get('sql_script', '').upper()

        # Verificar se usa opção ONLINE/CONCURRENTLY
        has_online_option = (
            'ONLINE' in sql_script or
            'CONCURRENTLY' in sql_script
        )

        # Classificar baseado no tamanho da tabela
        if table_size_gb >= self.table_size_thresholds['critical_gb']:
            return RiskAssessment(
                risk_level=RiskLevel.CRITICAL,
                auto_approved=False,
                requires_notification=True,
                requires_veto_window=True,
                reason=f"Tabela muito grande ({table_size_gb:.0f} GB)",
                recommendations=[
                    "Criar índice em horário de baixa demanda",
                    "Monitorar espaço em disco",
                    "Considerar executar manualmente",
                    "Verificar se ONLINE/CONCURRENTLY está disponível"
                ]
            )

        elif table_size_gb >= self.table_size_thresholds['high_gb']:
            return RiskAssessment(
                risk_level=RiskLevel.HIGH,
                auto_approved=True,
                requires_notification=True,
                requires_veto_window=True,
                reason=f"Tabela grande ({table_size_gb:.0f} GB), aguardar janela de veto",
                recommendations=[
                    "Execução pode levar várias horas",
                    "Monitorar espaço em disco durante criação",
                    "Usar opção ONLINE se disponível"
                ]
            )

        elif table_size_gb >= self.table_size_thresholds['medium_gb']:
            if has_online_option:
                return RiskAssessment(
                    risk_level=RiskLevel.MEDIUM,
                    auto_approved=True,
                    requires_notification=True,
                    requires_veto_window=False,
                    reason=f"Tabela média ({table_size_gb:.0f} GB) com ONLINE/CONCURRENTLY",
                    recommendations=[
                        "Índice será criado sem bloquear leituras",
                        "Tempo estimado: 30-60 minutos"
                    ]
                )
            else:
                return RiskAssessment(
                    risk_level=RiskLevel.HIGH,
                    auto_approved=True,
                    requires_notification=True,
                    requires_veto_window=True,
                    reason=f"Tabela média ({table_size_gb:.0f} GB) SEM opção ONLINE",
                    recommendations=[
                        "Criação pode bloquear escritas",
                        "Considerar adicionar ONLINE/CONCURRENTLY",
                        "Executar em horário de baixa demanda"
                    ]
                )

        else:
            # Tabela pequena
            return RiskAssessment(
                risk_level=RiskLevel.LOW if has_online_option else RiskLevel.MEDIUM,
                auto_approved=True,
                requires_notification=has_online_option == False,
                requires_veto_window=False,
                reason=f"Tabela pequena ({table_size_gb:.0f} GB)",
                recommendations=[
                    "Criação rápida (< 5 minutos)",
                    "Baixo impacto no sistema"
                ]
            )

    def _classify_rebuild_index(
        self,
        optimization: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> RiskAssessment:
        """Classifica REBUILD INDEX."""
        table_size_gb = metadata.get('table_size_gb', 0)
        fragmentation_percent = optimization.get('fragmentation_percent', 0)
        sql_script = optimization.get('sql_script', '').upper()

        has_online_option = 'ONLINE' in sql_script

        # Se fragmentação é muito baixa, não vale a pena
        if fragmentation_percent < self.index_fragmentation_threshold:
            return RiskAssessment(
                risk_level=RiskLevel.LOW,
                auto_approved=False,  # Não executar
                requires_notification=True,
                requires_veto_window=False,
                reason=f"Fragmentação baixa ({fragmentation_percent:.1f}%), rebuild desnecessário",
                recommendations=[
                    "Aguardar fragmentação atingir >50%",
                    "Considerar REORGANIZE ao invés de REBUILD"
                ]
            )

        # Rebuild de índice grande
        if table_size_gb >= self.table_size_thresholds['high_gb']:
            return RiskAssessment(
                risk_level=RiskLevel.HIGH,
                auto_approved=True,
                requires_notification=True,
                requires_veto_window=True,
                reason=f"Rebuild de índice grande ({table_size_gb:.0f} GB)",
                recommendations=[
                    "Operação pode levar horas",
                    "Requer espaço extra em disco (até 2x o tamanho do índice)",
                    "Usar ONLINE se possível"
                ]
            )

        return RiskAssessment(
            risk_level=RiskLevel.MEDIUM,
            auto_approved=True,
            requires_notification=True,
            requires_veto_window=False,
            reason=f"Rebuild com fragmentação {fragmentation_percent:.1f}%",
            recommendations=[
                "Melhora performance de queries",
                "Libera espaço fragmentado"
            ]
        )

    def _classify_reorganize_index(
        self,
        optimization: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> RiskAssessment:
        """Classifica REORGANIZE INDEX."""
        # REORGANIZE é mais seguro que REBUILD
        return RiskAssessment(
            risk_level=RiskLevel.LOW,
            auto_approved=True,
            requires_notification=False,
            requires_veto_window=False,
            reason="REORGANIZE INDEX é operação online e segura",
            recommendations=[
                "Não bloqueia tabela",
                "Executa em background",
                "Melhora performance gradualmente"
            ]
        )

    def _classify_vacuum_analyze(
        self,
        optimization: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> RiskAssessment:
        """Classifica VACUUM ANALYZE (PostgreSQL)."""
        table_size_gb = metadata.get('table_size_gb', 0)
        sql_script = optimization.get('sql_script', '').upper()

        # VACUUM FULL é muito mais agressivo
        is_full_vacuum = 'VACUUM FULL' in sql_script

        if is_full_vacuum:
            if table_size_gb >= self.table_size_thresholds['high_gb']:
                return RiskAssessment(
                    risk_level=RiskLevel.CRITICAL,
                    auto_approved=False,
                    requires_notification=True,
                    requires_veto_window=True,
                    reason="VACUUM FULL em tabela grande bloqueia totalmente",
                    recommendations=[
                        "VACUUM FULL trava a tabela completamente",
                        "Considerar usar VACUUM normal",
                        "Executar apenas em janela de manutenção"
                    ]
                )
            else:
                return RiskAssessment(
                    risk_level=RiskLevel.HIGH,
                    auto_approved=True,
                    requires_notification=True,
                    requires_veto_window=True,
                    reason="VACUUM FULL bloqueia tabela durante execução",
                    recommendations=[
                        "Executar em horário de baixa demanda",
                        "Monitorar espaço em disco"
                    ]
                )

        # VACUUM normal é seguro
        return RiskAssessment(
            risk_level=RiskLevel.LOW,
            auto_approved=True,
            requires_notification=False,
            requires_veto_window=False,
            reason="VACUUM ANALYZE é operação de manutenção segura",
            recommendations=[
                "Remove tuplas mortas",
                "Atualiza estatísticas",
                "Não bloqueia leituras/escritas"
            ]
        )

    def _classify_delta_merge(
        self,
        optimization: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> RiskAssessment:
        """Classifica DELTA MERGE (SAP HANA)."""
        table_size_gb = metadata.get('table_size_gb', 0)

        if table_size_gb >= self.table_size_thresholds['high_gb']:
            return RiskAssessment(
                risk_level=RiskLevel.MEDIUM,
                auto_approved=True,
                requires_notification=True,
                requires_veto_window=False,
                reason=f"Delta merge em tabela grande ({table_size_gb:.0f} GB)",
                recommendations=[
                    "Operação pode levar minutos",
                    "Melhora performance de queries",
                    "Executado online"
                ]
            )

        return RiskAssessment(
            risk_level=RiskLevel.LOW,
            auto_approved=True,
            requires_notification=False,
            requires_veto_window=False,
            reason="Delta merge é operação rotineira no HANA",
            recommendations=[
                "Consolida dados do delta store",
                "Melhora performance de queries"
            ]
        )

    def _classify_query_rewrite(
        self,
        optimization: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> RiskAssessment:
        """Classifica QUERY REWRITE (sugestões)."""
        # Query rewrite não é executável automaticamente
        return RiskAssessment(
            risk_level=RiskLevel.MEDIUM,
            auto_approved=False,
            requires_notification=True,
            requires_veto_window=False,
            reason="Query rewrite requer intervenção manual",
            recommendations=[
                "Revisar sugestão de reescrita",
                "Testar em ambiente de dev/QA primeiro",
                "Validar resultados antes de aplicar em produção",
                "Atualizar código da aplicação"
            ]
        )

    def _classify_drop_index(
        self,
        optimization: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> RiskAssessment:
        """Classifica DROP INDEX."""
        # DROP INDEX é operação destrutiva
        return RiskAssessment(
            risk_level=RiskLevel.CRITICAL,
            auto_approved=False,
            requires_notification=True,
            requires_veto_window=True,
            reason="DROP INDEX é operação destrutiva e irreversível",
            recommendations=[
                "Validar que índice realmente não é usado",
                "Fazer backup do DDL do índice",
                "Monitorar performance após remoção",
                "Considerar desabilitar ao invés de dropar"
            ]
        )

    def _classify_alter_table(
        self,
        optimization: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> RiskAssessment:
        """Classifica ALTER TABLE."""
        sql_script = optimization.get('sql_script', '').upper()

        # Detectar operações destrutivas
        is_destructive = any(keyword in sql_script for keyword in [
            'DROP COLUMN',
            'DROP CONSTRAINT',
            'TRUNCATE',
            'DELETE'
        ])

        if is_destructive:
            return RiskAssessment(
                risk_level=RiskLevel.CRITICAL,
                auto_approved=False,
                requires_notification=True,
                requires_veto_window=True,
                reason="ALTER TABLE com operação destrutiva",
                recommendations=[
                    "Fazer backup antes de executar",
                    "Validar impacto em aplicações",
                    "Executar manualmente"
                ]
            )

        # ALTER TABLE não destrutivo
        return RiskAssessment(
            risk_level=RiskLevel.HIGH,
            auto_approved=True,
            requires_notification=True,
            requires_veto_window=True,
            reason="ALTER TABLE requer validação",
            recommendations=[
                "Verificar locks durante execução",
                "Testar em ambiente de dev primeiro",
                "Executar em horário de baixa demanda"
            ]
        )

    def get_summary(self, assessments: list[RiskAssessment]) -> Dict[str, Any]:
        """
        Gera resumo de múltiplas avaliações de risco.

        Args:
            assessments: Lista de avaliações

        Returns:
            Dicionário com estatísticas
        """
        total = len(assessments)

        counts = {
            'low': sum(1 for a in assessments if a.risk_level == RiskLevel.LOW),
            'medium': sum(1 for a in assessments if a.risk_level == RiskLevel.MEDIUM),
            'high': sum(1 for a in assessments if a.risk_level == RiskLevel.HIGH),
            'critical': sum(1 for a in assessments if a.risk_level == RiskLevel.CRITICAL)
        }

        auto_approved = sum(1 for a in assessments if a.auto_approved)
        requires_review = sum(1 for a in assessments if not a.auto_approved)

        return {
            'total': total,
            'counts_by_level': counts,
            'auto_approved': auto_approved,
            'requires_review': requires_review,
            'auto_approval_rate': (auto_approved / total * 100) if total > 0 else 0
        }
