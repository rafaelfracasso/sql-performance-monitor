#!/usr/bin/env python3
"""
Script para inicializar a API REST do SQL Monitor.
"""
import sys
import os
from pathlib import Path

# Adicionar diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent))

def main():
    """Inicia servidor FastAPI."""
    import uvicorn
    from sql_monitor.api.app import app
    from sql_monitor.utils.metrics_store import MetricsStore
    from sql_monitor.api.routes import init_dependencies

    # Configurações
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8080"))
    db_path = os.getenv("DUCKDB_PATH", "sql_monitor_data/metrics.duckdb")

    print(f"Inicializando SQL Monitor API...")
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"DuckDB: {db_path}")

    # Inicializar MetricsStore
    try:
        metrics_store = MetricsStore(db_path)
        print(f"MetricsStore inicializado com sucesso")

        # Injetar dependências nas rotas
        init_dependencies(metrics_store)
        print("Dependências inicializadas")

    except Exception as e:
        print(f"Erro ao inicializar MetricsStore: {e}")
        sys.exit(1)

    # Iniciar servidor
    print(f"\nServidor iniciando em http://{host}:{port}")
    print("Pressione Ctrl+C para parar\n")

    try:
        uvicorn.run(
            app,
            host=host,
            port=port,
            log_level="info",
            access_log=True
        )
    except KeyboardInterrupt:
        print("\n\nServidor parado pelo usuário")
    except Exception as e:
        print(f"\nErro ao iniciar servidor: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
