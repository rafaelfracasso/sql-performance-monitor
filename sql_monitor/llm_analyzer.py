"""
Análise de queries usando Google Gemini para sugestões inteligentes de índices.
"""
import os
import time
from collections import deque
from google import genai
from typing import Dict, Optional
from dotenv import load_dotenv


class LLMAnalyzer:
    """Analisa queries usando LLM (Google Gemini) para sugestões de otimização."""

    def __init__(self, config: dict):
        """
        Inicializa analyzer com configurações da LLM.

        Args:
            config: Dicionário com configurações (model, temperature, etc).
        """
        load_dotenv()

        self.api_key = os.getenv('GEMINI_API_KEY')
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY não encontrada. Configure no arquivo .env")

        # Inicializa o cliente com a nova API
        self.client = genai.Client(api_key=self.api_key)

        self.model_name = config.get('model', 'gemini-2.5-flash')
        self.temperature = config.get('temperature', 0.1)
        self.max_tokens = config.get('max_tokens', 2048)
        self.max_retries = config.get('max_retries', 3)
        self.retry_delays = config.get('retry_delays', [3, 8, 15])

        # Rate limiting
        rate_limit_config = config.get('rate_limit', {})
        self.max_requests_per_day = rate_limit_config.get('max_requests_per_day', 100)
        self.max_requests_per_minute = rate_limit_config.get('max_requests_per_minute', 10)
        self.max_requests_per_cycle = rate_limit_config.get('max_requests_per_cycle', 5)
        self.min_delay_between_requests = rate_limit_config.get('min_delay_between_requests', 2)

        # Tracking de requests
        self.request_timestamps = deque(maxlen=1000)  # Últimos 1000 requests (para tracking diário)
        self.cycle_request_count = 0
        self.last_request_time = 0
        self.quota_exhausted_until = 0  # Timestamp até quando quota está esgotada

    def reset_cycle_count(self):
        """Reseta contador de requests do ciclo atual."""
        self.cycle_request_count = 0

    def _get_requests_last_minute(self) -> int:
        """Conta quantos requests foram feitos no último minuto."""
        now = time.time()
        one_minute_ago = now - 60

        count = sum(1 for ts in self.request_timestamps if ts >= one_minute_ago)
        return count

    def _get_requests_last_24h(self) -> int:
        """Conta quantos requests foram feitos nas últimas 24 horas."""
        now = time.time()
        one_day_ago = now - (24 * 60 * 60)

        count = sum(1 for ts in self.request_timestamps if ts >= one_day_ago)
        return count

    def _can_make_request(self) -> tuple[bool, str]:
        """
        Verifica se pode fazer um request baseado nos limites.

        Returns:
            (pode_fazer, motivo_se_nao)
        """
        # Verifica se quota foi esgotada recentemente (429)
        now = time.time()
        if self.quota_exhausted_until > now:
            wait_time = int(self.quota_exhausted_until - now)
            return False, f"Quota da API esgotada. Aguarde {wait_time}s (erro 429 anterior)"

        # Verifica limite diário
        if self.max_requests_per_day > 0:
            requests_last_24h = self._get_requests_last_24h()
            if requests_last_24h >= self.max_requests_per_day:
                return False, f"Limite diário de {self.max_requests_per_day} requests atingido ({requests_last_24h}/24h)"

        # Verifica limite por ciclo
        if self.cycle_request_count >= self.max_requests_per_cycle:
            return False, f"Limite de {self.max_requests_per_cycle} requests por ciclo atingido"

        # Verifica limite por minuto
        requests_last_minute = self._get_requests_last_minute()
        if requests_last_minute >= self.max_requests_per_minute:
            return False, f"Limite de {self.max_requests_per_minute} requests por minuto atingido"

        return True, ""

    def _wait_for_rate_limit(self):
        """Aguarda o tempo mínimo entre requests se necessário."""
        if self.last_request_time > 0:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_delay_between_requests:
                wait_time = self.min_delay_between_requests - elapsed
                print(f"   ⏱️  Aguardando {wait_time:.1f}s (rate limit)...")
                time.sleep(wait_time)

    def _record_request(self):
        """Registra que um request foi feito."""
        now = time.time()
        self.request_timestamps.append(now)
        self.cycle_request_count += 1
        self.last_request_time = now

    def analyze_query_performance(
        self,
        sanitized_query: str,
        placeholder_map: str,
        table_ddl: str,
        existing_indexes: str,
        metrics: Dict,
        query_plan: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Analisa query e retorna sugestões de otimização.

        Implementa retry com backoff exponencial para lidar com rate limits
        e overload do Gemini API (503 errors).

        Args:
            sanitized_query: Query SQL parametrizada (sem dados sensíveis).
            placeholder_map: Mapa de placeholders formatado.
            table_ddl: DDL da tabela (CREATE TABLE).
            existing_indexes: Lista de índices existentes formatada.
            metrics: Dicionário com métricas de performance.
            query_plan: Plano de execução XML (opcional).

        Returns:
            Dict com explanation, suggestions, priority, justification.
        """
        # Verifica rate limit ANTES de fazer qualquer coisa
        can_request, reason = self._can_make_request()
        if not can_request:
            print(f"   🚫 {reason}")
            print(f"   📊 Requests nas últimas 24h: {self._get_requests_last_24h()}/{self.max_requests_per_day}")
            print(f"   📊 Requests no último minuto: {self._get_requests_last_minute()}/{self.max_requests_per_minute}")
            print(f"   📊 Requests neste ciclo: {self.cycle_request_count}/{self.max_requests_per_cycle}")
            return {
                'explanation': f"Análise pulada: {reason}",
                'suggestions': "Análise não realizada para economizar quota da API.",
                'priority': "N/A",
                'justification': "Rate limit atingido. Aumente os limites no config.json se necessário."
            }

        # Aguarda delay mínimo entre requests
        self._wait_for_rate_limit()

        prompt = self._build_analysis_prompt(
            sanitized_query,
            placeholder_map,
            table_ddl,
            existing_indexes,
            metrics,
            query_plan
        )

        # Configuração de retry (do config.json)
        max_retries = self.max_retries
        retry_delays = self.retry_delays

        last_error = None

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                    print(f"   ⏳ Aguardando {wait_time}s antes de tentar novamente...")
                    time.sleep(wait_time)
                    print(f"   🔄 Tentativa {attempt + 1}/{max_retries}...")

                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=prompt,
                    config={
                        'temperature': self.temperature,
                        'max_output_tokens': self.max_tokens,
                    }
                )

                # Registra request bem-sucedido
                self._record_request()

                # Debug: verifica tamanho da resposta
                response_length = len(response.text)
                print(f"   📊 Resposta LLM: {response_length} caracteres")

                # Debug: salva resposta completa se muito curta (possível truncamento)
                if response_length < 500:
                    print(f"   ⚠️  Resposta suspeita (muito curta): {response.text[:200]}...")

                result = self._parse_llm_response(response.text)

                # Debug: verifica se sugestões foram encontradas
                if not result['suggestions'] or result['suggestions'] == '':
                    print(f"   ⚠️  AVISO: Nenhuma sugestão de índice encontrada na resposta!")
                    print(f"   📝 Primeiros 300 chars da resposta: {response.text[:300]}")

                # Sucesso! Retorna resultado
                if attempt > 0:
                    print(f"   ✓ Análise concluída com sucesso após {attempt + 1} tentativa(s)")
                return result

            except Exception as e:
                last_error = e
                error_str = str(e)

                # Verifica se é erro 429 (quota exhausted)
                is_quota_error = '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str or 'quota exceeded' in error_str.lower()

                if is_quota_error:
                    print(f"   🚫 QUOTA DA API ESGOTADA!")
                    print(f"   📊 Erro: {error_str[:200]}...")

                    # Tenta extrair o retry delay da mensagem
                    import re
                    retry_match = re.search(r'retry in (\d+(?:\.\d+)?)\s*s', error_str, re.IGNORECASE)
                    if retry_match:
                        retry_seconds = float(retry_match.group(1))
                        self.quota_exhausted_until = time.time() + retry_seconds
                        print(f"   ⏰ API bloqueada por {int(retry_seconds)}s")
                    else:
                        # Se não encontrou, assume 1 minuto
                        self.quota_exhausted_until = time.time() + 60
                        print(f"   ⏰ API bloqueada por ~60s")

                    print(f"   💡 Dica: Considere usar gemini-1.5-flash (1500 RPD) ao invés de gemini-2.5-flash (20 RPD)")
                    break  # NÃO tenta novamente em caso de quota

                # Verifica se é erro 503 (overloaded) - esse pode tentar novamente
                is_retryable_503 = ('503' in error_str or 'overloaded' in error_str.lower() or
                                   'UNAVAILABLE' in error_str)

                if is_retryable_503 and attempt < max_retries - 1:
                    print(f"   ⚠️  API temporariamente indisponível: {error_str}")
                    continue  # Tenta novamente
                elif attempt == max_retries - 1:
                    print(f"   ✗ Falhou após {max_retries} tentativas: {error_str}")
                else:
                    # Erro não-retryable (404, auth error, etc)
                    print(f"   ✗ Erro ao chamar Gemini API: {error_str}")
                    break

        # Se chegou aqui, todas as tentativas falharam
        return {
            'explanation': f"Erro na análise LLM após {max_retries} tentativas: {str(last_error)}",
            'suggestions': "Não foi possível gerar sugestões devido a problemas com a API.",
            'priority': "DESCONHECIDO",
            'justification': "API Gemini temporariamente indisponível ou limite de taxa excedido."
        }

    def _build_analysis_prompt(
        self,
        query: str,
        placeholders: str,
        ddl: str,
        indexes: str,
        metrics: Dict,
        plan: Optional[str]
    ) -> str:
        """
        Constrói prompt estruturado para o Gemini.

        Args:
            query: Query parametrizada.
            placeholders: Mapa de placeholders.
            ddl: DDL da tabela.
            indexes: Índices existentes.
            metrics: Métricas de performance.
            plan: Plano de execução (opcional).

        Returns:
            Prompt formatado.
        """
        prompt = f"""Você é um especialista em otimização de performance de SQL Server.

Analise a seguinte query problemática e forneça sugestões de índices otimizados.

**IMPORTANTE**: A query foi parametrizada por segurança. Os valores reais foram substituídos por placeholders tipados.

## QUERY PARAMETRIZADA (Dados Sensíveis Removidos)
```sql
{query}
```

## {placeholders}

## DDL DA TABELA
```sql
{ddl}
```

## {indexes}

## MÉTRICAS DE PERFORMANCE
- Tempo de execução: {metrics.get('duration_seconds', 'N/A')}s
- CPU Time: {metrics.get('cpu_time_ms', 'N/A')}ms
- Logical Reads: {metrics.get('logical_reads', 'N/A')}
- Physical Reads: {metrics.get('physical_reads', 'N/A')}
- Writes: {metrics.get('writes', 'N/A')}
"""

        if plan and len(plan) < 5000:  # Limita tamanho do plano
            prompt += f"\n## PLANO DE EXECUÇÃO (Resumo)\n{plan[:5000]}\n"

        prompt += """
## TAREFA
Seja OBJETIVO e CONCISO. Forneça:

1. **EXPLICAÇÃO** (máximo 2-3 frases): Principal causa do problema de performance.

2. **SUGESTÕES DE ÍNDICES** (foco principal): Comandos CREATE INDEX completos e prontos para executar.
   - Priorize colunas em WHERE, JOIN, ORDER BY
   - Use INCLUDE para colunas no SELECT
   - Evite duplicar índices existentes

3. **PRIORIDADE**: CRÍTICO, ALTO, MÉDIO ou BAIXO

4. **JUSTIFICATIVA** (1-2 frases por índice): Como cada índice resolve o problema.

**IMPORTANTE:** Foque nas SUGESTÕES. Seja breve na explicação e justificativa.

**FORMATO DA RESPOSTA:**
Retorne EXATAMENTE neste formato:

[EXPLICAÇÃO]
<1-3 frases resumindo o problema principal>

[SUGESTÕES]
```sql
CREATE NONCLUSTERED INDEX IX_nome ON schema.tabela(colunas_chave) INCLUDE (colunas_include);
-- Mais índices se necessário
```

[PRIORIDADE]
<CRÍTICO|ALTO|MÉDIO|BAIXO>

[JUSTIFICATIVA]
<1-2 frases explicando como cada índice melhora a performance>
"""

        return prompt

    def _parse_llm_response(self, response_text: str) -> Dict[str, str]:
        """
        Parse da resposta do Gemini extraindo seções.

        Args:
            response_text: Texto da resposta da LLM.

        Returns:
            Dict com seções parseadas.
        """
        result = {
            'explanation': '',
            'suggestions': '',
            'priority': 'MÉDIO',
            'justification': ''
        }

        # Parse usando marcadores
        sections = {
            'explanation': r'\[EXPLICAÇÃO\](.*?)(?=\[SUGESTÕES\]|\[PRIORIDADE\]|\[JUSTIFICATIVA\]|$)',
            'suggestions': r'\[SUGESTÕES\](.*?)(?=\[PRIORIDADE\]|\[JUSTIFICATIVA\]|$)',
            'priority': r'\[PRIORIDADE\](.*?)(?=\[JUSTIFICATIVA\]|$)',
            'justification': r'\[JUSTIFICATIVA\](.*?)$'
        }

        import re
        for key, pattern in sections.items():
            match = re.search(pattern, response_text, re.DOTALL | re.IGNORECASE)
            if match:
                result[key] = match.group(1).strip()

        # Se não encontrou marcadores, tenta usar a resposta inteira
        if not result['explanation'] and not result['suggestions']:
            result['explanation'] = response_text.strip()

        return result

    def test_connection(self) -> bool:
        """
        Testa conexão com Gemini API com retry.

        Returns:
            bool: True se conectado com sucesso.
        """
        max_retries = 2  # Testa 2 vezes apenas

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    print(f"⏳ Aguardando 3s antes de testar novamente...")
                    time.sleep(3)

                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents="Responda apenas: OK"
                )
                print(f"✓ Gemini API conectada: {self.model_name}")
                return True

            except Exception as e:
                error_str = str(e)
                is_retryable = ('503' in error_str or 'overloaded' in error_str.lower() or
                               '429' in error_str or 'UNAVAILABLE' in error_str)

                if is_retryable and attempt < max_retries - 1:
                    print(f"⚠️  API temporariamente indisponível, tentando novamente...")
                    continue
                else:
                    print(f"✗ Erro ao testar Gemini API: {e}")
                    if is_retryable:
                        print(f"   ℹ️  Não se preocupe: o monitor vai usar retry automático durante as análises")
                    return False

        return False
