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
