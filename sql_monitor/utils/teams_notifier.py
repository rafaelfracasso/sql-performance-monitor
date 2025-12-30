"""
Notificador para Microsoft Teams via webhook do Power Automate.
Envia alertas de queries problemáticas no formato Prometheus/Alertmanager.
"""
import hashlib
import requests
from datetime import datetime
from typing import Dict, Optional
from .sql_formatter import format_sql_for_teams


class TeamsNotifier:
    """
    Gerencia envio de notificações para Microsoft Teams via Power Automate.

    Envia payloads no formato Prometheus/Alertmanager que são processados
    por um flow existente do Power Automate.
    """

    def __init__(self, config: Dict):
        """
        Inicializa notificador do Teams.

        Args:
            config: Dicionário com configurações:
                - enabled (bool): Ativa/desativa notificações
                - webhook_url (str): URL do webhook do Power Automate
                - notify_on_cache_hit (bool): Notifica queries em cache
                - priority_filter (list): Lista de prioridades para notificar
                - server_name (str): Nome do servidor SQL
        """
        self.enabled = config.get('enabled', False)
        self.webhook_url = config.get('webhook_url', '')
        self.notify_on_cache_hit = config.get('notify_on_cache_hit', True)
        self.priority_filter = config.get('priority_filter', [])
        self.server_name = config.get('server_name', 'SQL-Server')
        self.timeout = config.get('timeout', 10)

        # Estatísticas
        self.total_sent = 0
        self.total_failed = 0

    def send_query_alert(
        self,
        query_info: Dict,
        sanitized_query: str,
        database: str,
        schema: str,
        table: str,
        performance_summary: str,
        llm_analysis: Dict,
        log_file: str,
        is_cache_hit: bool = False,
        cache_info: Optional[Dict] = None
    ) -> bool:
        """
        Envia alerta de query problemática para o Teams.

        Args:
            query_info: Informações da query (métricas, tempo, etc).
            sanitized_query: Query sanitizada (sem valores literais).
            database: Nome do database.
            schema: Nome do schema.
            table: Nome da tabela.
            performance_summary: Resumo de performance.
            llm_analysis: Análise do LLM (explanation, suggestions, priority).
            log_file: Caminho do arquivo de log.
            is_cache_hit: Se True, é um cache hit (query já analisada).
            cache_info: Informações do cache (se cache hit).

        Returns:
            True se enviou com sucesso, False caso contrário.
        """
        if not self.enabled or not self.webhook_url:
            return False

        # Filtra por prioridade (se configurado)
        if self.priority_filter:
            priority = llm_analysis.get('priority', 'MÉDIO')
            if priority not in self.priority_filter:
                return False

        # Constrói payload no formato Prometheus/Alertmanager
        payload = self._build_alert_payload(
            server=self.server_name,
            database=database,
            schema=schema,
            table=table,
            priority=llm_analysis.get('priority', 'MÉDIO'),
            query_info=query_info,
            sanitized_query=sanitized_query,
            llm_analysis=llm_analysis,
            log_file=log_file,
            is_cache_hit=is_cache_hit,
            cache_info=cache_info
        )

        # Envia para Power Automate
        return self._send_to_teams(payload)

    def _build_alert_payload(
        self,
        server: str,
        database: str,
        schema: str,
        table: str,
        priority: str,
        query_info: Dict,
        sanitized_query: str,
        llm_analysis: Dict,
        log_file: str,
        is_cache_hit: bool,
        cache_info: Optional[Dict] = None
    ) -> Dict:
        """
        Constrói payload no formato Prometheus/Alertmanager para Power Automate.

        Returns:
            Dicionário com estrutura de alerta compatível com o flow existente.
        """
        # Monta descrição completa com todas as informações
        description_parts = []

        # Status (nova análise ou cache hit)
        if is_cache_hit and cache_info:
            hours_ago = self._get_hours_since(cache_info['analyzed_at'])
            description_parts.append(
                f"🔄 Cache Hit - Analisada há {hours_ago:.1f}h | "
                f"Vista {cache_info['seen_count']} vezes"
            )
        else:
            description_parts.append("✨ Nova análise realizada pelo LLM")

        # Métricas de performance
        metrics = []
        if 'duration_seconds' in query_info:
            metrics.append(f"⏱️ Tempo: {query_info['duration_seconds']}s")
        if 'cpu_time_ms' in query_info:
            metrics.append(f"💻 CPU: {query_info['cpu_time_ms']}ms")
        if 'logical_reads' in query_info:
            reads = query_info['logical_reads']
            metrics.append(f"💾 Logical Reads: {reads:,}")
        if 'physical_reads' in query_info:
            reads = query_info['physical_reads']
            metrics.append(f"📀 Physical Reads: {reads:,}")

        if metrics:
            description_parts.extend(metrics)

        # Query preview formatada
        query_formatted = format_sql_for_teams(sanitized_query, max_length=400)
        description_parts.append(f"\n🔍 Query:\n{query_formatted}")

        # Análise LLM
        if llm_analysis.get('explanation'):
            explanation = llm_analysis['explanation'][:300]
            description_parts.append(f"\n🤖 Análise: {explanation}")

        # Sugestões de índices
        if llm_analysis.get('suggestions'):
            suggestions = llm_analysis['suggestions'][:300]
            description_parts.append(f"\n💡 Sugestão: {suggestions}")

        # Log completo
        description_parts.append(f"\n📄 Log: {log_file}")

        # Junta tudo
        description = "\n".join(description_parts)

        # Determina status (firing ou resolved)
        # resolved = cache hit (opcional, para diferenciar visualmente)
        status = "resolved" if is_cache_hit else "firing"

        # Monta payload no formato Prometheus/Alertmanager
        payload = {
            "receiver": "sql-monitor",
            "status": status,
            "alerts": [{
                "status": status,
                "labels": {
                    "alertname": "QueryProblematica",
                    "instance": f"{server}\\{database}",
                    "name": f"{schema}.{table}",
                    "severity": priority
                },
                "annotations": {
                    "title": f"⚠️ Query Problemática - {schema}.{table}",
                    "description": description
                },
                "startsAt": datetime.now().isoformat() + "Z",
                "endsAt": "0001-01-01T00:00:00Z",
                "generatorURL": f"file:///{log_file}",
                "fingerprint": hashlib.md5(f"{database}{schema}{table}".encode()).hexdigest()
            }],
            "groupLabels": {},
            "commonLabels": {
                "alertname": "QueryProblematica",
                "severity": priority
            },
            "commonAnnotations": {},
            "externalURL": "http://sql-monitor",
            "version": "4",
            "groupKey": f"{{}}:{{alertname=\"QueryProblematica\"}}",
            "truncatedAlerts": 0
        }

        return payload

    def _send_to_teams(self, payload: Dict) -> bool:
        """
        Envia payload para webhook do Power Automate via HTTP POST.

        Args:
            payload: Payload no formato Prometheus/Alertmanager.

        Returns:
            True se enviou com sucesso (status 202), False caso contrário.
        """
        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=self.timeout
            )

            if response.status_code == 202:
                self.total_sent += 1
                return True
            else:
                self.total_failed += 1
                print(f"⚠️  Teams webhook retornou status {response.status_code}")
                return False

        except requests.exceptions.Timeout:
            self.total_failed += 1
            print(f"⚠️  Timeout ao enviar para Teams (>{self.timeout}s)")
            return False

        except Exception as e:
            self.total_failed += 1
            print(f"⚠️  Erro ao enviar para Teams: {e}")
            return False

    def _get_hours_since(self, timestamp_str: str) -> float:
        """
        Calcula horas desde um timestamp ISO.

        Args:
            timestamp_str: Timestamp no formato ISO (ex: "2025-12-18T10:30:15").

        Returns:
            Número de horas (float) desde o timestamp.
        """
        try:
            timestamp = datetime.fromisoformat(timestamp_str)
            delta = datetime.now() - timestamp
            return delta.total_seconds() / 3600
        except Exception:
            return 0.0

    def test_connection(self) -> bool:
        """
        Testa conexão com o webhook do Teams enviando payload de teste.

        Returns:
            True se webhook responde, False caso contrário.
        """
        if not self.enabled or not self.webhook_url:
            return False

        test_payload = {
            "receiver": "sql-monitor",
            "status": "firing",
            "alerts": [{
                "status": "firing",
                "labels": {
                    "alertname": "TesteConexao",
                    "instance": "test",
                    "name": "test.test",
                    "severity": "INFO"
                },
                "annotations": {
                    "title": "✅ Teste de Conexão - SQL Monitor",
                    "description": "Este é um teste de conexão com o Teams. Se você está vendo esta mensagem, a integração está funcionando!"
                },
                "startsAt": datetime.now().isoformat() + "Z",
                "endsAt": "0001-01-01T00:00:00Z",
                "generatorURL": "http://sql-monitor/test",
                "fingerprint": "test123"
            }],
            "groupLabels": {},
            "commonLabels": {
                "alertname": "TesteConexao",
                "severity": "INFO"
            },
            "commonAnnotations": {},
            "externalURL": "http://sql-monitor",
            "version": "4",
            "groupKey": "{}:{alertname=\"TesteConexao\"}",
            "truncatedAlerts": 0
        }

        return self._send_to_teams(test_payload)

    def get_statistics(self) -> Dict:
        """
        Retorna estatísticas de envio.

        Returns:
            Dicionário com total_sent e total_failed.
        """
        return {
            'total_sent': self.total_sent,
            'total_failed': self.total_failed,
            'success_rate': (self.total_sent / (self.total_sent + self.total_failed) * 100)
                            if (self.total_sent + self.total_failed) > 0 else 0
        }

    def send_plan_generated_card(self, plan: 'OptimizationPlan', evaluation: Dict) -> bool:
        """
        Envia Adaptive Card quando um plano é gerado.

        Args:
            plan: Plano gerado
            evaluation: Avaliação do approval engine

        Returns:
            True se enviou com sucesso
        """
        if not self.enabled or not self.webhook_url:
            return False

        # Construir Adaptive Card
        card = {
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": "Plano Semanal de Otimização Gerado",
                            "size": "Large",
                            "weight": "Bolder",
                            "color": "Accent"
                        },
                        {
                            "type": "TextBlock",
                            "text": f"Plano ID: {plan.plan_id}",
                            "size": "Medium",
                            "spacing": "None"
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {
                                    "title": "Gerado em:",
                                    "value": plan.generated_at.strftime('%d/%m/%Y %H:%M')
                                },
                                {
                                    "title": "Execução programada:",
                                    "value": plan.execution_scheduled_at.strftime('%d/%m/%Y %H:%M')
                                },
                                {
                                    "title": "Janela de veto expira:",
                                    "value": plan.veto_window_expires_at.strftime('%d/%m/%Y %H:%M')
                                },
                                {
                                    "title": "Total de otimizações:",
                                    "value": str(plan.total_optimizations)
                                },
                                {
                                    "title": "Auto-aprovadas (LOW/MEDIUM):",
                                    "value": str(plan.auto_approved_count)
                                },
                                {
                                    "title": "Requer revisão (HIGH):",
                                    "value": str(plan.requires_review_count)
                                },
                                {
                                    "title": "Bloqueadas (CRITICAL):",
                                    "value": str(plan.blocked_count)
                                }
                            ]
                        },
                        {
                            "type": "TextBlock",
                            "text": "Ação Necessária",
                            "size": "Medium",
                            "weight": "Bolder",
                            "separator": True
                        },
                        {
                            "type": "TextBlock",
                            "text": f"Você tem {(plan.veto_window_expires_at - datetime.now()).total_seconds() / 3600:.0f} horas para revisar e vetar otimizações se necessário.",
                            "wrap": True
                        },
                        {
                            "type": "TextBlock",
                            "text": f"[Clique aqui para visualizar e gerenciar o plano](http://seu-servidor:8080/plans/{plan.plan_id})",
                            "wrap": True,
                            "color": "Accent"
                        }
                    ]
                }
            }]
        }

        return self._send_adaptive_card(card)

    def send_plan_executed_card(self, plan: 'OptimizationPlan', results: Dict) -> bool:
        """
        Envia Adaptive Card quando um plano é executado.

        Args:
            plan: Plano executado
            results: Resultados por instância

        Returns:
            True se enviou com sucesso
        """
        if not self.enabled or not self.webhook_url:
            return False

        # Agregar resultados
        total_executed = sum(r.get('executed', 0) for r in results.values() if isinstance(r, dict))
        total_success = sum(r.get('success', 0) for r in results.values() if isinstance(r, dict))
        total_failed = sum(r.get('failed', 0) for r in results.values() if isinstance(r, dict))
        total_rolled_back = sum(r.get('rolled_back', 0) for r in results.values() if isinstance(r, dict))

        success_rate = (total_success / total_executed * 100) if total_executed > 0 else 0

        # Determinar cor baseada em taxa de sucesso
        if success_rate >= 90:
            color = "Good"
            status_emoji = "✅"
        elif success_rate >= 70:
            color = "Warning"
            status_emoji = "⚠️"
        else:
            color = "Attention"
            status_emoji = "❌"

        # Construir Adaptive Card
        card = {
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": f"{status_emoji} Plano de Otimização Executado",
                            "size": "Large",
                            "weight": "Bolder",
                            "color": color
                        },
                        {
                            "type": "TextBlock",
                            "text": f"Plano ID: {plan.plan_id}",
                            "size": "Medium",
                            "spacing": "None"
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {
                                    "title": "Executado em:",
                                    "value": datetime.now().strftime('%d/%m/%Y %H:%M')
                                },
                                {
                                    "title": "Otimizações executadas:",
                                    "value": str(total_executed)
                                },
                                {
                                    "title": "Bem-sucedidas:",
                                    "value": str(total_success)
                                },
                                {
                                    "title": "Falhadas:",
                                    "value": str(total_failed)
                                },
                                {
                                    "title": "Revertidas (Rollback):",
                                    "value": str(total_rolled_back)
                                },
                                {
                                    "title": "Taxa de sucesso:",
                                    "value": f"{success_rate:.1f}%"
                                }
                            ]
                        },
                        {
                            "type": "TextBlock",
                            "text": "Próximos Passos",
                            "size": "Medium",
                            "weight": "Bolder",
                            "separator": True
                        },
                        {
                            "type": "TextBlock",
                            "text": "Um relatório de impacto detalhado será enviado na segunda-feira às 08:00.",
                            "wrap": True
                        },
                        {
                            "type": "TextBlock",
                            "text": f"[Ver detalhes da execução](http://seu-servidor:8080/plans/{plan.plan_id})",
                            "wrap": True,
                            "color": "Accent"
                        }
                    ]
                }
            }]
        }

        return self._send_adaptive_card(card)

    def send_impact_report_card(self, report: 'ImpactReport', summary: str) -> bool:
        """
        Envia Adaptive Card com relatório de impacto.

        Args:
            report: Relatório de impacto
            summary: Resumo executivo em texto

        Returns:
            True se enviou com sucesso
        """
        if not self.enabled or not self.webhook_url:
            return False

        report_dict = report.to_dict()
        net_improvement = report_dict['impact']['net_improvement']

        # Determinar cor baseada em impacto
        if net_improvement > 20:
            color = "Good"
            status_emoji = "📈"
        elif net_improvement > 0:
            color = "Warning"
            status_emoji = "📊"
        else:
            color = "Attention"
            status_emoji = "📉"

        roi_facts = [
            {
                "title": "CPU Reduction:",
                "value": f"{report_dict['roi']['improvements']['cpu_reduction_percent']:.1f}%"
            },
            {
                "title": "Duration Reduction:",
                "value": f"{report_dict['roi']['improvements']['duration_reduction_percent']:.1f}%"
            },
            {
                "title": "Reads Reduction:",
                "value": f"{report_dict['roi']['improvements']['reads_reduction_percent']:.1f}%"
            }
        ]

        if report_dict['roi']['roi']['roi_days']:
            roi_facts.append({
                "title": "Payback Period:",
                "value": f"{report_dict['roi']['roi']['roi_days']:.0f} dias"
            })

        # Construir Adaptive Card
        card = {
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": f"{status_emoji} Relatório de Impacto Semanal",
                            "size": "Large",
                            "weight": "Bolder",
                            "color": color
                        },
                        {
                            "type": "TextBlock",
                            "text": f"Plano ID: {report.plan_id}",
                            "size": "Medium",
                            "spacing": "None"
                        },
                        {
                            "type": "TextBlock",
                            "text": "Resumo da Execução",
                            "size": "Medium",
                            "weight": "Bolder"
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {
                                    "title": "Taxa de sucesso:",
                                    "value": f"{report_dict['summary']['success_rate']:.1f}%"
                                },
                                {
                                    "title": "Bem-sucedidas:",
                                    "value": str(report.successful)
                                },
                                {
                                    "title": "Falhadas:",
                                    "value": str(report.failed)
                                },
                                {
                                    "title": "Revertidas:",
                                    "value": str(report.rolled_back)
                                }
                            ]
                        },
                        {
                            "type": "TextBlock",
                            "text": "Impacto em Performance",
                            "size": "Medium",
                            "weight": "Bolder",
                            "separator": True
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {
                                    "title": "Melhoria total:",
                                    "value": f"{report.total_improvement_percent:.1f}%"
                                },
                                {
                                    "title": "Degradação total:",
                                    "value": f"{report.total_degradation_percent:.1f}%"
                                },
                                {
                                    "title": "Impacto líquido:",
                                    "value": f"{net_improvement:.1f}%"
                                }
                            ]
                        },
                        {
                            "type": "TextBlock",
                            "text": "ROI e Economia",
                            "size": "Medium",
                            "weight": "Bolder",
                            "separator": True
                        },
                        {
                            "type": "FactSet",
                            "facts": roi_facts
                        },
                        {
                            "type": "TextBlock",
                            "text": "Recomendações",
                            "size": "Medium",
                            "weight": "Bolder",
                            "separator": True
                        },
                        {
                            "type": "TextBlock",
                            "text": "\n".join(f"• {rec}" for rec in report.recommendations[:3]),
                            "wrap": True
                        },
                        {
                            "type": "TextBlock",
                            "text": f"[Ver relatório completo](http://seu-servidor:8080/plans/{report.plan_id})",
                            "wrap": True,
                            "color": "Accent"
                        }
                    ]
                }
            }]
        }

        return self._send_adaptive_card(card)

    def _send_adaptive_card(self, card: Dict) -> bool:
        """
        Envia Adaptive Card para Teams.

        Args:
            card: Dicionário com Adaptive Card

        Returns:
            True se enviou com sucesso
        """
        try:
            response = requests.post(
                self.webhook_url,
                json=card,
                headers={'Content-Type': 'application/json'},
                timeout=self.timeout
            )

            if response.status_code in [200, 202]:
                self.total_sent += 1
                return True
            else:
                self.total_failed += 1
                print(f"Teams webhook retornou status {response.status_code}")
                return False

        except Exception as e:
            self.total_failed += 1
            print(f"Erro ao enviar Adaptive Card para Teams: {e}")
            return False
