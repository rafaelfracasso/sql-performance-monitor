"""
Aplicação FastAPI principal para dashboard e API REST.
"""
import os
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

# Diretórios
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

# Garantir que diretórios existem
TEMPLATES_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

# Criar aplicação FastAPI
app = FastAPI(
    title="SQL Monitor Dashboard",
    description="Dashboard de observabilidade e gerenciamento de otimizações",
    version="1.0.0"
)

# CORS (permitir acesso de qualquer origem - ajustar em produção)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Servir arquivos estáticos (CSS, JS, imagens)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Templates Jinja2
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Adicionar filtros customizados ao Jinja2
def format_number(value):
    """Formata número com separadores de milhar."""
    try:
        return "{:,}".format(int(value))
    except (ValueError, TypeError):
        return value

templates.env.filters['format_number'] = format_number


# Health check endpoint
@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "sql-monitor-api"}


# Importar e registrar rotas
from .routes import router
app.include_router(router)


def create_app(config: dict = None):
    """
    Factory function para criar aplicação FastAPI.

    Args:
        config: Dicionário de configuração

    Returns:
        FastAPI app instance
    """
    # Configurações podem ser aplicadas aqui
    if config:
        # Aplicar configurações personalizadas se necessário
        pass

    return app


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
