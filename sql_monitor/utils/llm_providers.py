"""
Abstração de providers LLM: Groq e Gemini (via endpoint OpenAI-compatible).
"""

# Preços por 1M tokens (input, output) em USD
MODEL_PRICING = {
    # Groq
    'llama-3.3-70b-versatile':  (0.59, 0.79),
    'llama-3.1-8b-instant':     (0.05, 0.08),
    'llama-3.1-70b-versatile':  (0.59, 0.79),
    'mixtral-8x7b-32768':       (0.24, 0.24),
    'gemma2-9b-it':             (0.20, 0.20),
    # Gemini
    'gemini-2.0-flash':         (0.10, 0.40),
    'gemini-2.0-flash-lite':    (0.075, 0.30),
    'gemini-1.5-flash':         (0.075, 0.30),
    'gemini-1.5-pro':           (3.50, 10.50),
}

PROVIDER_DEFAULTS = {
    'groq':   {'env_key': 'GROQ_API_KEY',   'default_model': 'llama-3.3-70b-versatile'},
    'gemini': {'env_key': 'GEMINI_API_KEY',  'default_model': 'gemini-2.0-flash'},
}


def get_api_key_env(provider: str) -> str:
    """Retorna o nome da variável de ambiente da API key para o provider."""
    cfg = PROVIDER_DEFAULTS.get(provider)
    if not cfg:
        raise ValueError(f"Provider desconhecido: {provider}")
    return cfg['env_key']


def get_default_model(provider: str) -> str:
    """Retorna o modelo padrão para o provider."""
    cfg = PROVIDER_DEFAULTS.get(provider)
    if not cfg:
        raise ValueError(f"Provider desconhecido: {provider}")
    return cfg['default_model']


def get_model_pricing(model: str) -> tuple:
    """
    Retorna (input_price, output_price) por 1M tokens em USD para o modelo.
    Retorna (0.0, 0.0) se o modelo não estiver na tabela de preços.
    """
    return MODEL_PRICING.get(model, (0.0, 0.0))


def create_client(provider: str, api_key: str):
    """
    Factory que cria o cliente LLM para o provider.
    Groq usa o SDK groq; Gemini usa o SDK openai com endpoint compatível.
    A interface client.chat.completions.create() é idêntica nos dois casos.
    """
    if provider == 'groq':
        from groq import Groq
        return Groq(api_key=api_key)
    elif provider == 'gemini':
        from openai import OpenAI
        return OpenAI(
            api_key=api_key,
            base_url='https://generativelanguage.googleapis.com/v1beta/openai/'
        )
    raise ValueError(f"Provider desconhecido: {provider}")


def list_models(provider: str, api_key: str) -> list:
    """Lista modelos disponíveis via API do provider (funciona para Groq e Gemini)."""
    client = create_client(provider, api_key)
    models = client.models.list()
    return sorted([m.id for m in models.data])
