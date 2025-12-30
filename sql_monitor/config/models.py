"""
Modelos Pydantic para validação de configuração.

Valida config.json e databases.json garantindo que todos os campos
necessários estão presentes e têm tipos/valores válidos.
"""
from typing import List, Dict, Optional, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
from pathlib import Path


# ============================================================================
# Modelos para config.json
# ============================================================================

class MonitorConfig(BaseModel):
    """Configuração de monitoramento."""
    interval_seconds: int = Field(
        default=60,
        ge=10,
        le=3600,
        description="Intervalo entre verificações (10s-1h)"
    )


class PerformanceThresholds(BaseModel):
    """Thresholds de performance para identificar queries problemáticas."""
    execution_time_seconds: float = Field(
        default=30,
        ge=0,
        description="Tempo de execução em segundos"
    )
    cpu_time_ms: int = Field(
        default=10000,
        ge=0,
        description="Tempo de CPU em milissegundos"
    )
    logical_reads: int = Field(
        default=50000,
        ge=0,
        description="Número de leituras lógicas"
    )
    physical_reads: int = Field(
        default=10000,
        ge=0,
        description="Número de leituras físicas"
    )
    writes: int = Field(
        default=5000,
        ge=0,
        description="Número de escritas"
    )


class LLMRateLimit(BaseModel):
    """Configuração de rate limiting para LLM."""
    max_requests_per_day: int = Field(
        default=1500,
        ge=0,
        description="Máximo de requests por dia (0 = ilimitado)"
    )
    max_requests_per_minute: int = Field(
        default=60,
        ge=1,
        le=1000,
        description="Máximo de requests por minuto"
    )
    max_requests_per_cycle: int = Field(
        default=5,
        ge=1,
        le=100,
        description="Máximo de requests por ciclo"
    )
    min_delay_between_requests: float = Field(
        default=2,
        ge=0,
        le=60,
        description="Delay mínimo entre requests (segundos)"
    )


class LLMConfig(BaseModel):
    """Configuração do LLM (Google Gemini)."""
    provider: Literal["gemini"] = Field(
        default="gemini",
        description="Provider de LLM (atualmente apenas gemini)"
    )
    model: str = Field(
        default="gemini-2.0-flash-exp",
        description="Modelo do Gemini a usar"
    )
    temperature: float = Field(
        default=0.1,
        ge=0.0,
        le=2.0,
        description="Temperatura do modelo (0-2)"
    )
    max_tokens: int = Field(
        default=8192,
        ge=256,
        le=32768,
        description="Máximo de tokens na resposta"
    )
    max_retries: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Número de tentativas em caso de erro"
    )
    retry_delays: List[int] = Field(
        default=[3, 8, 15],
        description="Delays entre retries (segundos)"
    )
    rate_limit: LLMRateLimit = Field(
        default_factory=LLMRateLimit,
        description="Configuração de rate limiting"
    )


class LoggingConfig(BaseModel):
    """Configuração de logging."""
    log_directory: str = Field(
        default="logs",
        description="Diretório para arquivos de log"
    )
    include_execution_plan: bool = Field(
        default=True,
        description="Incluir plano de execução nos logs"
    )
    max_query_length: int = Field(
        default=10000,
        ge=100,
        le=100000,
        description="Tamanho máximo de query nos logs"
    )


class SecurityConfig(BaseModel):
    """Configuração de segurança."""
    sanitize_queries: bool = Field(
        default=True,
        description="Sanitizar queries antes de enviar ao LLM"
    )
    placeholder_prefix: str = Field(
        default="@p",
        min_length=1,
        max_length=10,
        description="Prefixo para placeholders"
    )
    show_example_values: bool = Field(
        default=True,
        description="Mostrar exemplos de valores nos placeholders"
    )


class QueryCacheConfig(BaseModel):
    """Configuração do cache de queries."""
    enabled: bool = Field(
        default=True,
        description="Habilitar cache de queries"
    )
    ttl_hours: int = Field(
        default=24,
        ge=1,
        le=168,  # 1 semana
        description="Time to live em horas"
    )
    cache_file: str = Field(
        default="logs/query_cache.json",
        description="Caminho do arquivo de cache"
    )
    auto_save_interval: int = Field(
        default=300,
        ge=60,
        le=3600,
        description="Intervalo de auto-save em segundos"
    )


class TeamsConfig(BaseModel):
    """Configuração de integração com Microsoft Teams."""
    enabled: bool = Field(
        default=False,
        description="Habilitar integração com Teams"
    )
    webhook_url: Optional[str] = Field(
        default=None,
        description="URL do webhook do Power Automate"
    )
    notify_on_cache_hit: bool = Field(
        default=True,
        description="Notificar também em cache hits"
    )
    priority_filter: List[str] = Field(
        default_factory=list,
        description="Filtrar por prioridade (vazio = todas)"
    )
    timeout: int = Field(
        default=10,
        ge=1,
        le=60,
        description="Timeout de requisição em segundos"
    )

    @field_validator('webhook_url')
    @classmethod
    def validate_webhook_url(cls, v: Optional[str], info) -> Optional[str]:
        """Valida que webhook_url está presente se enabled=True."""
        # Acessa o valor de 'enabled' através de info.data
        if info.data.get('enabled') and not v:
            raise ValueError("webhook_url é obrigatório quando Teams está habilitado")
        return v


class TimeoutsConfig(BaseModel):
    """Configuração de timeouts."""
    database_connect: int = Field(
        default=10,
        ge=1,
        le=60,
        description="Timeout de conexão ao database (segundos)"
    )
    database_query: int = Field(
        default=60,
        ge=5,
        le=300,
        description="Timeout de execução de query (segundos)"
    )
    llm_analysis: int = Field(
        default=30,
        ge=10,
        le=120,
        description="Timeout de análise LLM (segundos)"
    )
    thread_shutdown: int = Field(
        default=90,
        ge=30,
        le=300,
        description="Timeout de shutdown de threads (segundos)"
    )
    circuit_breaker_recovery: int = Field(
        default=60,
        ge=10,
        le=300,
        description="Timeout de recuperação do circuit breaker (segundos)"
    )


class StructuredLoggingConfig(BaseModel):
    """Configuração de logging estruturado."""
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Nível de log"
    )
    format: Literal["colored", "json", "simple"] = Field(
        default="colored",
        description="Formato de saída"
    )
    log_file: str = Field(
        default="logs/monitor.log",
        description="Arquivo de log"
    )
    enable_console: bool = Field(
        default=True,
        description="Habilitar saída no console"
    )


class Config(BaseModel):
    """Modelo completo de config.json."""
    monitor: MonitorConfig = Field(default_factory=MonitorConfig)
    performance_thresholds: PerformanceThresholds = Field(default_factory=PerformanceThresholds)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    query_cache: QueryCacheConfig = Field(default_factory=QueryCacheConfig)
    teams: TeamsConfig = Field(default_factory=TeamsConfig)
    timeouts: TimeoutsConfig = Field(default_factory=TimeoutsConfig)
    structured_logging: StructuredLoggingConfig = Field(
        default_factory=StructuredLoggingConfig,
        alias="logging"  # Pode usar tanto 'logging' quanto 'structured_logging'
    )

    class Config:
        """Configuração do Pydantic."""
        extra = "allow"  # Permite campos extras (para _comments)
        populate_by_name = True  # Permite usar alias


# ============================================================================
# Modelos para databases.json
# ============================================================================

class DatabaseCredentials(BaseModel):
    """Credenciais de conexão ao database."""
    server: str = Field(
        min_length=1,
        description="Endereço do servidor"
    )
    port: str = Field(
        min_length=1,
        description="Porta do servidor"
    )
    database: str = Field(
        min_length=1,
        description="Nome do database"
    )
    username: str = Field(
        min_length=1,
        description="Usuário"
    )
    password: str = Field(
        min_length=1,
        description="Senha (pode ser ${VAR_NAME})"
    )
    driver: Optional[str] = Field(
        default=None,
        description="Driver ODBC (apenas SQL Server)"
    )

    @field_validator('port')
    @classmethod
    def validate_port(cls, v: str) -> str:
        """Valida que porta é um número válido."""
        try:
            port = int(v)
            if port < 1 or port > 65535:
                raise ValueError(f"Porta deve estar entre 1-65535, recebido: {port}")
        except ValueError as e:
            if "invalid literal" in str(e):
                raise ValueError(f"Porta deve ser um número, recebido: {v}")
            raise
        return v


class DatabaseEntry(BaseModel):
    """Entrada de configuração de database."""
    name: str = Field(
        min_length=1,
        description="Nome identificador do database"
    )
    type: Literal["SQLSERVER", "POSTGRESQL", "HANA"] = Field(
        description="Tipo de database"
    )
    enabled: bool = Field(
        default=True,
        description="Se deve monitorar este database"
    )
    credentials: DatabaseCredentials = Field(
        description="Credenciais de conexão"
    )


class DatabasesConfig(BaseModel):
    """Modelo completo de databases.json."""
    databases: List[DatabaseEntry] = Field(
        min_length=1,
        description="Lista de databases a monitorar"
    )

    class Config:
        """Configuração do Pydantic."""
        extra = "allow"  # Permite campos extras (para _comments)

    @model_validator(mode='after')
    def validate_at_least_one_enabled(self) -> 'DatabasesConfig':
        """Valida que pelo menos um database está habilitado."""
        enabled_count = sum(1 for db in self.databases if db.enabled)
        if enabled_count == 0:
            raise ValueError(
                "Pelo menos um database deve estar habilitado (enabled: true)"
            )
        return self


# ============================================================================
# Funções auxiliares de validação
# ============================================================================

def validate_config_file(config_path: str) -> Config:
    """
    Valida arquivo config.json.

    Args:
        config_path: Caminho para config.json.

    Returns:
        Objeto Config validado.

    Raises:
        FileNotFoundError: Se arquivo não existir.
        ValueError: Se configuração for inválida.
    """
    import json

    if not Path(config_path).exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    try:
        return Config(**data)
    except Exception as e:
        raise ValueError(f"Erro ao validar config.json: {e}")


def validate_databases_file(databases_path: str) -> DatabasesConfig:
    """
    Valida arquivo databases.json.

    Args:
        databases_path: Caminho para databases.json.

    Returns:
        Objeto DatabasesConfig validado.

    Raises:
        FileNotFoundError: Se arquivo não existir.
        ValueError: Se configuração for inválida.
    """
    import json

    if not Path(databases_path).exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {databases_path}")

    with open(databases_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    try:
        return DatabasesConfig(**data)
    except Exception as e:
        raise ValueError(f"Erro ao validar databases.json: {e}")
