#!/usr/bin/env python3
"""
Multi-Database Performance Monitor
Monitor periódico para SQL Server, PostgreSQL e SAP HANA com análise via LLM.
"""
import sys
import json
import threading
from pathlib import Path
from dotenv import load_dotenv

from sql_monitor.monitor.multi_monitor import MultiDatabaseMonitor
from sql_monitor.utils.structured_logger import setup_logging, create_logger
from sql_monitor.config import validate_config_file, validate_databases_file

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()


def start_api_server(config: dict, metrics_store, logger):
    """
    Inicia servidor FastAPI em thread separada.

    Args:
        config: Configuração completa
        metrics_store: MetricsStore instance
        logger: Logger instance

    Returns:
        Thread do servidor ou None se desabilitado
    """
    api_config = config.get('weekly_optimizer', {}).get('api', {})

    if not api_config.get('enabled', False):
        logger.info("API REST desabilitada (weekly_optimizer.api.enabled = false)")
        return None

    try:
        import uvicorn
        from sql_monitor.api.app import app
        from sql_monitor.api.routes import init_dependencies

        # Inicializar dependências da API
        init_dependencies(metrics_store, config)

        host = api_config.get('host', '0.0.0.0')
        port = api_config.get('port', 8080)

        logger.info(f"Iniciando API REST em http://{host}:{port}")

        # Executar em thread separada
        def run_server():
            uvicorn.run(
                app,
                host=host,
                port=port,
                log_level="warning",  # Menos verboso
                access_log=False
            )

        api_thread = threading.Thread(
            target=run_server,
            daemon=True,
            name="FastAPI-Server"
        )
        api_thread.start()

        logger.info(f"API REST disponível em http://{host}:{port}")
        logger.info(f"  Dashboard: http://{host}:{port}/")
        logger.info(f"  Planos: http://{host}:{port}/plans")

        return api_thread

    except ImportError as e:
        logger.error(f"Erro ao importar FastAPI: {e}")
        logger.error("Execute: pip install fastapi uvicorn jinja2 python-multipart")
        return None
    except Exception as e:
        logger.error(f"Erro ao iniciar API: {e}", exc_info=True)
        return None


def start_weekly_optimizer(config: dict, metrics_store, connectors: dict, teams_notifier, logger):
    """
    Inicia WeeklyOptimizerScheduler.

    Args:
        config: Configuração completa
        metrics_store: MetricsStore instance
        connectors: Dict de conectores {instance_name: connector}
        teams_notifier: TeamsNotifier instance
        logger: Logger instance

    Returns:
        WeeklyOptimizerScheduler instance ou None se desabilitado
    """
    optimizer_config = config.get('weekly_optimizer', {})

    if not optimizer_config.get('enabled', False):
        logger.info("Weekly Optimizer desabilitado (weekly_optimizer.enabled = false)")
        return None

    try:
        from sql_monitor.optimization.scheduler import WeeklyOptimizerScheduler

        logger.info("Iniciando Weekly Optimizer Scheduler")

        scheduler = WeeklyOptimizerScheduler(
            metrics_store=metrics_store,
            connectors=connectors,
            teams_notifier=teams_notifier,
            config=optimizer_config
        )

        scheduler.start()

        return scheduler

    except ImportError as e:
        logger.error(f"Erro ao importar WeeklyOptimizerScheduler: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro ao iniciar Weekly Optimizer: {e}", exc_info=True)
        return None


def main():
    """Função principal."""
    # Validar e carregar configurações
    try:
        # Validar config.json com Pydantic
        config_model = validate_config_file('config.json')
        config = config_model.model_dump(by_alias=True, exclude_none=True)

        # Configurar sistema de logging
        logging_config = config.get('logging', {})
        setup_logging(
            log_level=logging_config.get('level', 'INFO'),
            log_format=logging_config.get('format', 'colored'),
            log_file=logging_config.get('log_file', 'logs/monitor.log'),
            enable_console=logging_config.get('enable_console', True)
        )

        logger = create_logger(__name__)
        logger.info("Sistema de logging inicializado",
                   level=logging_config.get('level', 'INFO'),
                   format=logging_config.get('format', 'colored'))

    except FileNotFoundError as e:
        print(f"\n✗ {e}")
        print("  Certifique-se de que config.json existe no diretório atual.")
        return 1
    except ValueError as e:
        print(f"\n✗ Erro de validação em config.json:")
        print(f"  {e}")
        print("\n  Corrija os erros e tente novamente.")
        return 1
    except Exception as e:
        print(f"⚠️  Erro inesperado ao carregar config.json: {e}")
        print("   Usando configuração padrão")
        logger = None
        config = {}

    # Validar databases.json
    try:
        databases_model = validate_databases_file('config/databases.json')
        print(f"✓ Configuração validada: {len(databases_model.databases)} database(s) configurado(s)")
    except FileNotFoundError as e:
        print(f"\n✗ {e}")
        print("  Copie config/databases.json.example para config/databases.json")
        print("  e configure as credenciais dos seus bancos de dados.")
        return 1
    except ValueError as e:
        print(f"\n✗ Erro de validação em databases.json:")
        print(f"  {e}")
        print("\n  Corrija os erros e tente novamente.")
        return 1

    print("\n" + "=" * 80)
    print("MULTI-DATABASE PERFORMANCE MONITOR")
    print("=" * 80)
    print("Monitora SQL Server, PostgreSQL e SAP HANA simultaneamente")
    print("Análise inteligente via Google Gemini LLM")
    print("=" * 80 + "\n")

    # Inicializa monitor
    monitor = MultiDatabaseMonitor(
        config_path="config.json",
        db_config_path="config/databases.json"
    )

    # Inicializa todos os monitors
    if not monitor.initialize():
        print("\n✗ Falha na inicialização. Nenhum monitor foi criado.")
        print("  Verifique se há bancos habilitados em config/databases.json")
        return

    # Inicializar componentes opcionais
    api_thread = None
    weekly_optimizer = None

    try:
        # Inicia monitoramento em background (threads)
        monitor.start()

        print("\n✓ Monitoramento ativo!")

        # Inicializar API REST (se habilitada)
        api_thread = start_api_server(
            config=config,
            metrics_store=monitor.metrics_store,
            logger=logger
        )

        # Inicializar Weekly Optimizer (se habilitado)
        # Obter conectores dos monitors ativos
        connectors = {}
        for db_type, db_monitors_list in monitor.monitors_by_type.items():
            for db_monitor in db_monitors_list:
                if hasattr(db_monitor, 'connector') and hasattr(db_monitor, 'instance_name'):
                    connectors[db_monitor.instance_name] = db_monitor.connector

        # Criar TeamsNotifier para o Weekly Optimizer
        teams_notifier = None
        if config.get('teams', {}).get('enabled', False):
            try:
                from sql_monitor.utils.teams_notifier import TeamsNotifier
                teams_notifier = TeamsNotifier(config)
                logger.info("TeamsNotifier inicializado para Weekly Optimizer")
            except Exception as e:
                logger.warning(f"Erro ao inicializar TeamsNotifier: {e}")

        weekly_optimizer = start_weekly_optimizer(
            config=config,
            metrics_store=monitor.metrics_store,
            connectors=connectors,
            teams_notifier=teams_notifier,
            logger=logger
        )

        print("\n  Pressione Ctrl+C para parar e ver estatísticas.\n")

        # Loop principal - aguarda threads de monitoramento
        for thread in monitor.threads:
            thread.join()

    except KeyboardInterrupt:
        print("\n\n✓ Interrompido pelo usuário")

    finally:
        # Parar componentes opcionais
        if weekly_optimizer:
            logger.info("Parando Weekly Optimizer...")
            weekly_optimizer.stop()

        # API thread é daemon, vai parar automaticamente

        # Para monitor e mostra estatísticas
        monitor.stop()
        monitor.print_stats()

        print("\n✓ Encerrado com sucesso\n")


if __name__ == "__main__":
    main()
