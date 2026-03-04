"""
Análise de queries usando LLM para sugestões inteligentes de índices.
"""
import os
import json
import time
from collections import deque
from typing import Dict, Optional
from enum import Enum
from dotenv import load_dotenv
from pathlib import Path
from .llm_providers import create_client, get_api_key_env


class CircuitState(Enum):
    """Estados do circuit breaker."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Circuit aberto (não faz requests)
    HALF_OPEN = "half_open"  # Testando se API voltou


class LLMAnalyzer:
    """Analisa queries usando LLM para sugestões de otimização."""

    PROMPT_RELOAD_INTERVAL_SECONDS = 300  # 5 minutos

    def __init__(self, config: dict, metrics_store=None):
        """
        Inicializa analyzer com configurações da LLM.

        Args:
            config: Dicionário com configurações (model, temperature, etc).
            metrics_store: Instância de MetricsStore para buscar prompts do DuckDB.
                Se None, prompts editados na UI não terão efeito (usa defaults hardcoded).
        """
        load_dotenv()

        # Suporta receber config inteiro ou apenas secao 'llm'
        llm_config = config.get('llm', config) if 'llm' in config else config

        self.provider = llm_config.get('provider', 'groq')
        self.model_name = llm_config.get('model', 'llama-3.3-70b-versatile')
        self.temperature = llm_config.get('temperature', 0.1)
        self.max_tokens = llm_config.get('max_tokens', 2048)
        self.max_retries = llm_config.get('max_retries', 3)
        self.retry_delays = llm_config.get('retry_delays', [3, 8, 15])

        # metrics_store precisa ser definido antes de _load_config_from_db
        self.metrics_store = metrics_store

        # DuckDB é a fonte de verdade para configuração de LLM — sobrescreve config.json
        self._load_config_from_db()

        env_var = get_api_key_env(self.provider)
        self.api_key = os.getenv(env_var)
        if not self.api_key:
            raise ValueError(f"{env_var} não encontrada. Configure no arquivo .env")

        # Inicializa o cliente LLM via factory
        self.client = create_client(self.provider, self.api_key)

        # Rate limiting
        rate_limit_config = llm_config.get('rate_limit', {})
        self.max_requests_per_day = rate_limit_config.get('max_requests_per_day', 100)
        self.max_requests_per_minute = rate_limit_config.get('max_requests_per_minute', 10)
        self.max_requests_per_cycle = rate_limit_config.get('max_requests_per_cycle', 5)
        self.min_delay_between_requests = rate_limit_config.get('min_delay_between_requests', 2)

        # Tracking de requests
        self.request_timestamps = deque(maxlen=1000)  # Últimos 1000 requests (para tracking diário)
        self.cycle_request_count = 0
        self.last_request_time = 0
        self.quota_exhausted_until = 0  # Timestamp até quando quota está esgotada

        # Circuit Breaker
        self.circuit_state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.failure_threshold = 5  # Abre circuito após 5 falhas consecutivas
        # Obter timeout de recuperação do circuit breaker do config (busca na raiz)
        timeouts_config = config.get('timeouts', {})
        self.recovery_timeout = timeouts_config.get('circuit_breaker_recovery', 60)
        self.half_open_success_threshold = 2  # Precisa 2 sucessos consecutivos para fechar
        self.circuit_opened_at = 0  # Timestamp quando circuito foi aberto

        # Carregar prompts do DuckDB (ou fallback para defaults)
        if not metrics_store:
            import traceback
            print(f"[WARN] LLMAnalyzer inicializado SEM metrics_store - prompts da UI serão ignorados!")
            print(f"[WARN] Caller: {''.join(traceback.format_stack()[-3:-1]).strip()}")
        self.prompts = self._load_prompts()
        self.prompts_last_loaded = time.time()
        self.config_last_loaded = time.time()

    def _reload_config_if_changed(self):
        """
        Recarrega model/provider/temperatura do DuckDB periodicamente (a cada 5 minutos).
        Se o provider mudou, recria o client. Permite trocar modelo sem reiniciar.
        """
        if not self.metrics_store:
            return
        now = time.time()
        if now - self.config_last_loaded < self.PROMPT_RELOAD_INTERVAL_SECONDS:
            return
        try:
            result = self.metrics_store.execute_query("""
                SELECT provider, model, temperature, max_tokens, max_retries, retry_delays
                FROM llm_config WHERE id = 1
            """)
            if not result or not result[0]:
                return
            row = result[0]
            new_provider = row[0] or self.provider
            new_model    = row[1] or self.model_name

            if new_provider != self.provider:
                env_var = get_api_key_env(new_provider)
                new_key = os.getenv(env_var)
                if not new_key:
                    print(f"[WARN] Nao foi possivel trocar para provider '{new_provider}': {env_var} nao configurada")
                    return
                self.provider = new_provider
                self.api_key  = new_key
                self.client   = create_client(self.provider, self.api_key)
                print(f"[INFO] Provider LLM atualizado para: {self.provider}")

            if new_model != self.model_name:
                print(f"[INFO] Modelo LLM atualizado: {self.model_name} -> {new_model}")
                self.model_name = new_model

            if row[2] is not None:
                self.temperature = row[2]
            if row[3] is not None:
                self.max_tokens = row[3]
            if row[4] is not None:
                self.max_retries = row[4]
            if row[5]:
                import json as _json
                self.retry_delays = _json.loads(row[5])

            self.config_last_loaded = now
        except Exception as e:
            print(f"[WARN] Erro ao recarregar config LLM: {e}")

    def _load_config_from_db(self):
        """Sobrescreve configurações com valores do DuckDB (fonte de verdade do /settings)."""
        if not self.metrics_store:
            return
        try:
            result = self.metrics_store.execute_query("""
                SELECT provider, model, temperature, max_tokens, max_retries, retry_delays
                FROM llm_config WHERE id = 1
            """)
            if result and result[0]:
                row = result[0]
                if row[0]:
                    self.provider = row[0]
                if row[1]:
                    self.model_name = row[1]
                if row[2] is not None:
                    self.temperature = row[2]
                if row[3] is not None:
                    self.max_tokens = row[3]
                if row[4] is not None:
                    self.max_retries = row[4]
                if row[5]:
                    import json as _json
                    self.retry_delays = _json.loads(row[5])
        except Exception as e:
            print(f"[WARN] Nao foi possivel carregar config LLM do DuckDB: {e}")

    def _load_prompts(self) -> Dict:
        """
        Carrega prompts do DuckDB.

        Returns:
            Dicionário com prompts carregados ou prompts default se DuckDB não disponível.
        """
        if not self.metrics_store:
            print(f"[WARN] MetricsStore não disponível, usando prompts default")
            return self._get_default_prompts()

        try:
            # Buscar todos os prompts ativos do DuckDB
            results = self.metrics_store.execute_query("""
                SELECT db_type, prompt_type, name, content
                FROM llm_prompts
                WHERE is_active = TRUE
            """)

            if not results:
                print(f"[WARN] Nenhum prompt encontrado no DuckDB, usando defaults")
                return self._get_default_prompts()

            # Reconstruir estrutura do prompts.json
            prompts = {
                'database_prompts': {},
                'base_prompt_template': '',
                'task_instructions': ''
            }

            for db_type, prompt_type, name, content in results:
                if db_type not in prompts['database_prompts']:
                    prompts['database_prompts'][db_type] = {
                        'name': db_type.upper(),
                        'features': [],
                        'index_syntax': '',
                        'metrics_note': ''
                    }

                db_config = prompts['database_prompts'][db_type]

                # base_template e task_instructions sao globais - usar o primeiro encontrado
                if prompt_type == 'base_template':
                    if not prompts['base_prompt_template']:
                        prompts['base_prompt_template'] = content
                elif prompt_type == 'task_instructions':
                    if not prompts['task_instructions']:
                        prompts['task_instructions'] = content
                elif prompt_type == 'features':
                    db_config['features'] = [f.strip() for f in content.split('\n') if f.strip()]
                elif prompt_type == 'index_syntax':
                    db_config['index_syntax'] = content

            print(f"[INFO] Prompts carregados do DuckDB: {len(results)} registros")
            return prompts

        except Exception as e:
            print(f"[ERROR] Erro ao carregar prompts do DuckDB: {e}")
            print(f"[WARN] Usando prompts default hardcoded")
            return self._get_default_prompts()

    def _get_default_prompts(self) -> Dict:
        """Retorna prompts default (fallback) caso arquivo JSON não esteja disponível."""
        return {
            'database_prompts': {
                'sqlserver': {
                    'name': 'SQL Server',
                    'index_syntax': 'CREATE NONCLUSTERED INDEX IX_nome ON schema.tabela(colunas_chave) INCLUDE (colunas_include);',
                    'features': ['Use INCLUDE para colunas no SELECT (covering index)'],
                    'metrics_note': ''
                },
                'hana': {
                    'name': 'SAP HANA',
                    'index_syntax': 'CREATE INDEX idx_nome ON "SCHEMA"."TABELA" (coluna1, coluna2);',
                    'features': ['HANA usa COLUMN STORE por padrão'],
                    'metrics_note': ''
                },
                'postgresql': {
                    'name': 'PostgreSQL',
                    'index_syntax': 'CREATE INDEX idx_nome ON schema.tabela (coluna1, coluna2);',
                    'features': ['Use INCLUDE para covering indexes (PostgreSQL 11+)'],
                    'metrics_note': ''
                }
            },
            'base_prompt_template': 'Voce e um especialista em otimizacao de performance de {db_name}.',
            'task_instructions': 'Forneca sugestoes de otimizacao.'
        }

    def reload_prompts_if_changed(self):
        """
        Recarrega prompts do DuckDB periodicamente (a cada 5 minutos).

        Returns:
            bool: True se prompts foram recarregados, False caso contrário.
        """
        try:
            now = time.time()
            if now - self.prompts_last_loaded < self.PROMPT_RELOAD_INTERVAL_SECONDS:
                return False

            print(f"[INFO] Recarregando prompts do DuckDB...")
            self.prompts = self._load_prompts()
            self.prompts_last_loaded = now
            print(f"[INFO] Prompts recarregados com sucesso")
            return True

        except Exception as e:
            print(f"[ERROR] Erro ao recarregar prompts: {e}")
            return False

    def reset_cycle_count(self):
        """Reseta contador de requests do ciclo atual."""
        self.cycle_request_count = 0

    def _check_circuit_breaker(self) -> tuple[bool, str]:
        """
        Verifica estado do circuit breaker.

        Returns:
            (pode_fazer_request, motivo_se_nao)
        """
        now = time.time()

        if self.circuit_state == CircuitState.OPEN:
            # Verifica se pode tentar novamente (recovery timeout)
            if now - self.circuit_opened_at >= self.recovery_timeout:
                print(f"   🔧 Circuit breaker: HALF-OPEN (testando API...)")
                self.circuit_state = CircuitState.HALF_OPEN
                self.success_count = 0
                return True, ""
            else:
                wait_time = int(self.recovery_timeout - (now - self.circuit_opened_at))
                return False, f"Circuit breaker OPEN (API com falhas sistêmicas). Aguarde {wait_time}s"

        elif self.circuit_state == CircuitState.HALF_OPEN:
            # Em half-open, permite apenas 1 request de teste por vez
            return True, ""

        else:  # CLOSED
            return True, ""

    def _record_success(self):
        """Registra sucesso de API call (para circuit breaker)."""
        self.failure_count = 0

        if self.circuit_state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.half_open_success_threshold:
                print(f"   ✅ Circuit breaker: CLOSED (API recuperada)")
                self.circuit_state = CircuitState.CLOSED
                self.success_count = 0

    def _record_failure(self, is_retryable_error: bool = False):
        """
        Registra falha de API call (para circuit breaker).

        Args:
            is_retryable_error: Se True, é um erro temporário (503). Se False, é erro sistemático.
        """
        # Erros 503 (overload) não contam para circuit breaker - são temporários
        if is_retryable_error:
            return

        self.failure_count += 1
        self.success_count = 0

        # Abre circuit se ultrapassou threshold
        if self.failure_count >= self.failure_threshold:
            if self.circuit_state != CircuitState.OPEN:
                print(f"   🔴 Circuit breaker: OPEN (API falhando sistematicamente)")
                print(f"   📊 {self.failure_count} falhas consecutivas detectadas")
                print(f"   ⏰ API requests bloqueados por {self.recovery_timeout}s")
                self.circuit_state = CircuitState.OPEN
                self.circuit_opened_at = time.time()
                self.failure_count = 0

    def get_circuit_state(self) -> Dict[str, any]:
        """
        Retorna estado atual do circuit breaker.

        Returns:
            Dict com informações do circuito.
        """
        return {
            'state': self.circuit_state.value,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'failures_until_open': max(0, self.failure_threshold - self.failure_count),
            'successes_until_closed': max(0, self.half_open_success_threshold - self.success_count) if self.circuit_state == CircuitState.HALF_OPEN else 0
        }

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
        query_plan: Optional[str] = None,
        db_type: str = "sqlserver"
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
        # Verifica circuit breaker PRIMEIRO
        can_circuit, circuit_reason = self._check_circuit_breaker()
        if not can_circuit:
            print(f"   🚫 {circuit_reason}")
            return {
                'explanation': f"Análise pulada: {circuit_reason}",
                'suggestions': "API LLM está com falhas sistêmicas. Aguardando recuperação.",
                'priority': "N/A",
                'justification': "Circuit breaker ativo para proteger contra falhas em cascata.",
                'tokens_used': 0,
                'prompt_tokens': 0,
                'completion_tokens': 0,
                'model_used': self.model_name
            }

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
                'justification': "Rate limit atingido. Aumente os limites no config.json se necessário.",
                'tokens_used': 0,
                'prompt_tokens': 0,
                'completion_tokens': 0,
                'model_used': self.model_name
            }

        # Aguarda delay mínimo entre requests
        self._wait_for_rate_limit()

        # Recarrega config do DB se necessário (permite trocar modelo sem reiniciar)
        self._reload_config_if_changed()

        prompt = self._build_analysis_prompt(
            sanitized_query,
            placeholder_map,
            table_ddl,
            existing_indexes,
            metrics,
            query_plan,
            db_type
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

                response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=self.temperature,
                    max_tokens=self.max_tokens,
                )

                # Registra request bem-sucedido
                self._record_request()
                self._record_success()  # Circuit breaker: registra sucesso

                response_text = response.choices[0].message.content
                response_length = len(response_text)

                if response_length < 500:
                    print(f"   Resposta LLM suspeita (muito curta: {response_length} chars): {response_text[:200]}...")

                result = self._parse_llm_response(response_text)

                # Extrair uso de tokens se disponível
                if hasattr(response, 'usage') and response.usage:
                    result['tokens_used'] = response.usage.total_tokens or 0
                    result['prompt_tokens'] = response.usage.prompt_tokens or 0
                    result['completion_tokens'] = response.usage.completion_tokens or 0
                else:
                    result['tokens_used'] = 0
                    result['prompt_tokens'] = 0
                    result['completion_tokens'] = 0

                # Debug: verifica se sugestões foram encontradas
                if not result['suggestions'] or result['suggestions'] == '':
                    print(f"   AVISO: Nenhuma sugestao de indice encontrada na resposta!")
                    print(f"   Primeiros 300 chars da resposta: {response_text[:300]}")

                result['model_used'] = self.model_name

                # Sucesso! Retorna resultado
                if attempt > 0:
                    print(f"   ✓ Análise concluída com sucesso após {attempt + 1} tentativa(s)")
                return result

            except Exception as e:
                last_error = e
                error_str = str(e)

                # Verifica se é erro de tokens excedidos (prompt muito grande) - nao conta para circuit breaker
                is_token_limit_error = ('maximum number of tokens exceeded' in error_str.lower() or
                                        'context_length_exceeded' in error_str.lower() or
                                        'maximum context length' in error_str.lower())
                if is_token_limit_error:
                    print(f"   ✗ Prompt excede limite de tokens do provider: {error_str[:200]}")
                    break  # Nao tenta novamente - o prompt nao vai diminuir sozinho

                # Verifica se é erro 429 ou 413 (quota/payload too large)
                is_quota_error = ('429' in error_str or '413' in error_str or
                                  'RESOURCE_EXHAUSTED' in error_str or 'quota exceeded' in error_str.lower() or
                                  'rate_limit_exceeded' in error_str)

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

                    self._record_failure(is_retryable_error=False)  # Circuit breaker: falha sistematica
                    break  # NÃO tenta novamente em caso de quota

                # Verifica se é erro 503 (overloaded) - esse pode tentar novamente
                is_retryable_503 = ('503' in error_str or 'overloaded' in error_str.lower() or
                                   'UNAVAILABLE' in error_str)

                if is_retryable_503 and attempt < max_retries - 1:
                    print(f"   ⚠️  API temporariamente indisponível: {error_str}")
                    # Erro 503 é temporário, não conta para circuit breaker
                    continue  # Tenta novamente
                elif attempt == max_retries - 1:
                    print(f"   ✗ Falhou após {max_retries} tentativas: {error_str}")
                    # Se falhou todas as tentativas, registra falha no circuit breaker
                    self._record_failure(is_retryable_error=is_retryable_503)
                else:
                    # Erro não-retryable (404, auth error, etc)
                    print(f"   ✗ Erro ao chamar LLM API: {error_str}")
                    self._record_failure(is_retryable_error=False)  # Circuit breaker: falha sistematica
                    break

        # Se chegou aqui, todas as tentativas falharam
        return {
            'explanation': f"Erro na análise LLM após {max_retries} tentativas: {str(last_error)}",
            'suggestions': "Não foi possível gerar sugestões devido a problemas com a API.",
            'priority': "DESCONHECIDO",
            'justification': "API LLM temporariamente indisponivel ou limite de taxa excedido.",
            'tokens_used': 0,
            'prompt_tokens': 0,
            'completion_tokens': 0,
            'model_used': self.model_name
        }

    def _build_analysis_prompt(
        self,
        query: str,
        placeholders: str,
        ddl: str,
        indexes: str,
        metrics: Dict,
        plan: Optional[str],
        db_type: str = "sqlserver"
    ) -> str:
        """
        Constroi prompt estruturado para o Gemini baseado no tipo de banco.

        Args:
            query: Query parametrizada.
            placeholders: Mapa de placeholders.
            ddl: DDL da tabela.
            indexes: Indices existentes.
            metrics: Metricas de performance.
            plan: Plano de execucao (opcional).
            db_type: Tipo do banco (sqlserver, hana, postgresql).

        Returns:
            Prompt formatado.
        """
        # Verificar se prompts foram modificados e recarregar
        self.reload_prompts_if_changed()

        # Obter configuração do banco do JSON
        db_prompts = self.prompts.get('database_prompts', {})
        db_config = db_prompts.get(db_type, db_prompts.get('sqlserver', {}))

        # Extrair dados da configuração
        db_name = db_config.get('name', db_type.upper())
        index_syntax = db_config.get('index_syntax', '')
        features_list = db_config.get('features', [])
        metrics_note = db_config.get('metrics_note', '')

        # Converter lista de features para string formatada
        features_str = '\n'.join(f'   {feature}' for feature in features_list)

        config = {
            'name': db_name,
            'index_syntax': index_syntax,
            'features': features_str,
            'metrics_note': metrics_note
        }

        # Formatar métricas de forma mais amigável
        duration_s = metrics.get('duration_seconds', metrics.get('elapsed_time_ms', 0) / 1000 if metrics.get('elapsed_time_ms') else 'N/A')
        cpu_ms = metrics.get('cpu_time_ms', 'N/A')
        logical_reads = metrics.get('logical_reads', 'N/A')
        physical_reads = metrics.get('physical_reads', 'N/A')
        writes = metrics.get('writes', 'N/A')
        exec_count = metrics.get('execution_count', 'N/A')

        # HANA: substituir métricas indisponíveis e incluir métricas relevantes
        if db_type == "hana":
            if cpu_ms == 0 or cpu_ms == 0.0:
                cpu_ms = "N/A (não disponível no HANA)"
            if logical_reads == 0:
                logical_reads = "N/A (HANA é in-memory)"
            if physical_reads == 0:
                physical_reads = "N/A (HANA é in-memory)"

            # Incluir métricas HANA relevantes na nota
            hana_extra = []
            memory_mb = metrics.get('memory_mb', 0)
            if memory_mb and memory_mb > 0:
                hana_extra.append(f"Memória utilizada: {memory_mb:.1f} MB")
            total_rows = metrics.get('total_rows', 0)
            if total_rows and total_rows > 0:
                hana_extra.append(f"Total de linhas retornadas: {total_rows:,}")
            if exec_count and exec_count != 'N/A' and exec_count > 0:
                hana_extra.append(f"Contagem de execuções: {exec_count:,}")
            if hana_extra:
                extra_str = "\n".join(hana_extra)
                note = config['metrics_note']
                config['metrics_note'] = f"{note}\nMétricas HANA disponíveis:\n{extra_str}" if note else f"Métricas HANA disponíveis:\n{extra_str}"

        # Obter templates do JSON
        base_template = self.prompts.get('base_prompt_template', '')
        task_instructions = self.prompts.get('task_instructions', '')

        # Truncar DDL e indexes para nao exceder o limite de tokens do provider.
        # DDL grande demais gera HTTP 413 no Groq (limite de 12k TPM no on-demand).
        # Preferimos analise parcial a nenhuma analise.
        MAX_DDL_CHARS = 3000
        MAX_INDEXES_CHARS = 1500
        if len(ddl) > MAX_DDL_CHARS:
            ddl = ddl[:MAX_DDL_CHARS] + "\n-- [DDL truncado]"
        if len(indexes) > MAX_INDEXES_CHARS:
            indexes = indexes[:MAX_INDEXES_CHARS] + "\n-- [truncado]"

        # Construir prompt base
        prompt = base_template.format(
            db_name=config['name'],
            query=query,
            placeholders=placeholders,
            ddl=ddl,
            indexes=indexes,
            duration_s=duration_s,
            cpu_ms=cpu_ms,
            logical_reads=logical_reads,
            physical_reads=physical_reads,
            writes=writes,
            exec_count=exec_count,
            metrics_note=config['metrics_note']
        )

        # Adicionar plano de execução se disponível
        if plan and len(plan) < 5000:
            prompt += f"\n## PLANO DE EXECUCAO (Resumo)\n{plan[:5000]}\n"

        # Adicionar instruções da tarefa
        prompt += task_instructions.format(
            db_name=config['name'],
            features=config['features'],
            index_syntax=config['index_syntax']
        )

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

                response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=[{"role": "user", "content": "Responda apenas: OK"}],
                    max_tokens=10,
                )
                print(f"✓ LLM API conectada ({self.provider}): {self.model_name}")
                return True

            except Exception as e:
                error_str = str(e)
                is_retryable = ('503' in error_str or 'overloaded' in error_str.lower() or
                               '429' in error_str or 'UNAVAILABLE' in error_str)

                if is_retryable and attempt < max_retries - 1:
                    print(f"API temporariamente indisponivel, tentando novamente...")
                    continue
                else:
                    print(f"✗ Erro ao testar LLM API ({self.provider}): {e}")
                    if is_retryable:
                        print(f"   O monitor vai usar retry automatico durante as analises")
                    return False

        return False
