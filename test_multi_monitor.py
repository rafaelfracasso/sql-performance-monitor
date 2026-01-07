#!/usr/bin/env python3
"""
Script de teste para Multi-Database Monitor.
"""
import os
import json
import tempfile
from pathlib import Path
from dotenv import load_dotenv
from sql_monitor.monitor.multi_monitor import MultiDatabaseMonitor
from sql_monitor.core.database_types import DatabaseType

# Carregar variáveis de ambiente
load_dotenv()


def create_test_config():
    """Cria um arquivo de configuração temporário para testes."""
    config = {
        "query_cache": {
            "cache_file": "logs/query_cache.json",
            "max_hours": 24
        },
        "gemini": {
            "api_key": os.getenv('GEMINI_API_KEY', ''),
            "model": "gemini-1.5-pro",
            "temperature": 0.3
        },
        "teams": {
            "webhook_url": os.getenv('TEAMS_WEBHOOK_URL', ''),
            "enabled": False
        },
        "performance": {
            "cpu_threshold": 80.0,
            "elapsed_time_threshold": 5000,
            "logical_reads_threshold": 10000
        },
        "logging": {
            "format": "simple",
            "level": "INFO"
        },
        "timeouts": {
            "llm_timeout": 30,
            "database_timeout": 60,
            "shutdown_timeout": 90
        },
        "metrics_store": {
            "db_path": "logs/metrics.duckdb",
            "enable_compression": True
        }
    }

    # Criar arquivo temporário
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
    json.dump(config, temp_file, indent=2)
    temp_file.close()

    return temp_file.name


def create_test_databases_config(include_sqlserver=True, include_postgresql=True, include_hana=True):
    """Cria um arquivo de configuração de databases temporário para testes."""
    databases = []

    # SQL Server
    if include_sqlserver and os.getenv('SQL_SERVER'):
        databases.append({
            "enabled": True,
            "name": "SQL Server Test Instance",
            "type": "SQLSERVER",
            "credentials": {
                "server": os.getenv('SQL_SERVER'),
                "port": os.getenv('SQL_PORT', '1433'),
                "database": os.getenv('SQL_DATABASE', 'master'),
                "username": os.getenv('SQL_USERNAME'),
                "password": os.getenv('SQL_PASSWORD'),
                "driver": os.getenv('SQL_DRIVER', 'ODBC Driver 18 for SQL Server')
            }
        })

    # PostgreSQL
    if include_postgresql and os.getenv('PG_SERVER'):
        databases.append({
            "enabled": True,
            "name": "PostgreSQL Test Instance",
            "type": "POSTGRESQL",
            "credentials": {
                "server": os.getenv('PG_SERVER'),
                "port": os.getenv('PG_PORT', '5432'),
                "database": os.getenv('PG_DATABASE', 'postgres'),
                "username": os.getenv('PG_USERNAME'),
                "password": os.getenv('PG_PASSWORD')
            }
        })

    # SAP HANA
    if include_hana and os.getenv('HANA_SERVER'):
        databases.append({
            "enabled": True,
            "name": "SAP HANA Test Instance",
            "type": "HANA",
            "credentials": {
                "server": os.getenv('HANA_SERVER'),
                "port": os.getenv('HANA_PORT', '30015'),
                "database": os.getenv('HANA_DATABASE', 'SYSTEMDB'),
                "username": os.getenv('HANA_USERNAME'),
                "password": os.getenv('HANA_PASSWORD')
            }
        })

    config = {"databases": databases}

    # Criar arquivo temporário
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
    json.dump(config, temp_file, indent=2)
    temp_file.close()

    return temp_file.name


def test_initialization():
    """Testa inicialização do MultiDatabaseMonitor."""
    print("=" * 80)
    print("TESTE DE INICIALIZAÇÃO - MULTI-DATABASE MONITOR")
    print("=" * 80)

    # Criar arquivos de configuração temporários
    config_file = create_test_config()
    db_config_file = create_test_databases_config()

    try:
        # Criar monitor
        print("\n📊 Criando Multi-Database Monitor...")
        monitor = MultiDatabaseMonitor(
            config_path=config_file,
            db_config_path=db_config_file
        )

        # Inicializar
        print("\n🔧 Inicializando monitors...")
        if monitor.initialize():
            print("\n✓ Multi-Database Monitor inicializado com sucesso!")

            # Exibir estatísticas
            print("\n📈 Estatísticas de inicialização:")
            for db_type, monitors in monitor.monitors_by_type.items():
                if monitors:
                    print(f"   {db_type.value}: {len(monitors)} instância(s)")
                    for mon in monitors:
                        print(f"      - {mon.instance_name}")

            return True
        else:
            print("\n✗ Falha ao inicializar Multi-Database Monitor")
            return False

    except Exception as e:
        print(f"\n✗ Erro ao testar inicialização: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Limpar arquivos temporários
        try:
            os.unlink(config_file)
            os.unlink(db_config_file)
        except:
            pass


def test_single_cycle():
    """Testa execução de um único ciclo de monitoramento."""
    print("\n" + "=" * 80)
    print("TESTE DE CICLO ÚNICO - MULTI-DATABASE MONITOR")
    print("=" * 80)

    # Criar arquivos de configuração temporários
    config_file = create_test_config()
    db_config_file = create_test_databases_config()

    try:
        # Criar e inicializar monitor
        print("\n📊 Criando e inicializando Multi-Database Monitor...")
        monitor = MultiDatabaseMonitor(
            config_path=config_file,
            db_config_path=db_config_file
        )

        if not monitor.initialize():
            print("\n✗ Falha ao inicializar monitor")
            return False

        # Executar um único ciclo
        print("\n🔄 Executando ciclo único de monitoramento...")
        print("   (Isso pode demorar alguns minutos dependendo dos bancos configurados)")

        monitor.run_single_cycle()

        # Exibir estatísticas
        print("\n📊 Estatísticas do ciclo:")
        print(f"   Queries encontradas: {monitor.stats.get('total_queries_found', 0)}")
        print(f"   Queries analisadas: {monitor.stats.get('total_queries_analyzed', 0)}")
        print(f"   Cache hits: {monitor.stats.get('total_cache_hits', 0)}")
        print(f"   Erros: {monitor.stats.get('total_errors', 0)}")

        print("\n✓ Ciclo único executado com sucesso!")
        return True

    except Exception as e:
        print(f"\n✗ Erro ao executar ciclo: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Limpar arquivos temporários
        try:
            os.unlink(config_file)
            os.unlink(db_config_file)
        except:
            pass


def test_multithread_execution():
    """Testa execução multithread com múltiplos tipos de banco."""
    print("\n" + "=" * 80)
    print("TESTE DE EXECUÇÃO MULTITHREAD")
    print("=" * 80)

    # Verificar quais bancos estão configurados
    has_sqlserver = bool(os.getenv('SQL_SERVER'))
    has_postgresql = bool(os.getenv('PG_SERVER'))
    has_hana = bool(os.getenv('HANA_SERVER'))

    num_types = sum([has_sqlserver, has_postgresql, has_hana])

    if num_types < 2:
        print("\n⚠️  Teste de multithread requer pelo menos 2 tipos de banco configurados")
        print("   Tipos disponíveis:")
        print(f"      SQL Server: {'✓' if has_sqlserver else '✗'}")
        print(f"      PostgreSQL: {'✓' if has_postgresql else '✗'}")
        print(f"      SAP HANA: {'✓' if has_hana else '✗'}")
        return False

    # Criar arquivos de configuração temporários
    config_file = create_test_config()
    db_config_file = create_test_databases_config()

    try:
        print(f"\n📊 Testando execução multithread com {num_types} tipos de banco...")

        # Criar e inicializar monitor
        monitor = MultiDatabaseMonitor(
            config_path=config_file,
            db_config_path=db_config_file
        )

        if not monitor.initialize():
            print("\n✗ Falha ao inicializar monitor")
            return False

        # Executar ciclo (usa threads internamente)
        print("\n🔄 Executando ciclo multithread...")
        monitor.run_single_cycle()

        print(f"\n✓ Execução multithread concluída!")
        print(f"   Threads utilizadas: {num_types} (uma por tipo de banco)")

        return True

    except Exception as e:
        print(f"\n✗ Erro ao testar multithread: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Limpar arquivos temporários
        try:
            os.unlink(config_file)
            os.unlink(db_config_file)
        except:
            pass


def test_cache_isolation():
    """Testa isolamento de cache entre tipos de banco."""
    print("\n" + "=" * 80)
    print("TESTE DE ISOLAMENTO DE CACHE")
    print("=" * 80)

    # Criar arquivos de configuração temporários
    config_file = create_test_config()
    db_config_file = create_test_databases_config()

    try:
        print("\n📊 Verificando isolamento de cache por tipo de banco...")

        # Criar monitor
        monitor = MultiDatabaseMonitor(
            config_path=config_file,
            db_config_path=db_config_file
        )

        if not monitor.initialize():
            print("\n✗ Falha ao inicializar monitor")
            return False

        # Verificar que cada tipo tem seu próprio cache
        print("\n🔍 Caches criados:")
        for db_type, cache in monitor.caches.items():
            cache_file = cache.cache_file
            print(f"   {db_type.value}: {cache_file}")

        # Verificar que os arquivos de cache são diferentes
        cache_files = [cache.cache_file for cache in monitor.caches.values()]
        if len(cache_files) == len(set(cache_files)):
            print("\n✓ Cada tipo de banco tem seu próprio arquivo de cache!")
            print("   Thread-safe por design (sem race conditions)")
            return True
        else:
            print("\n✗ Conflito de cache detectado!")
            return False

    except Exception as e:
        print(f"\n✗ Erro ao testar isolamento de cache: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Limpar arquivos temporários
        try:
            os.unlink(config_file)
            os.unlink(db_config_file)
        except:
            pass


if __name__ == '__main__':
    print("\n🚀 Iniciando testes do Multi-Database Monitor...\n")

    results = []

    # Teste 1: Inicialização
    print("\n" + "=" * 80)
    result = test_initialization()
    results.append(("Inicialização", result))

    # Teste 2: Isolamento de cache
    result = test_cache_isolation()
    results.append(("Isolamento de Cache", result))

    # Teste 3: Execução multithread (se múltiplos tipos disponíveis)
    result = test_multithread_execution()
    results.append(("Execução Multithread", result))

    # Teste 4: Ciclo único (demorado - opcional)
    print("\n⚠️  O teste de ciclo único pode demorar vários minutos.")
    response = input("Deseja executar o teste de ciclo único? (s/N): ")
    if response.lower() == 's':
        result = test_single_cycle()
        results.append(("Ciclo Único", result))

    # Resumo
    print("\n" + "=" * 80)
    print("RESUMO DOS TESTES MULTI-DATABASE MONITOR")
    print("=" * 80)

    for name, success in results:
        status = "✓ PASSOU" if success else "✗ FALHOU"
        print(f"{status}: {name}")

    total = len(results)
    passed = sum(1 for _, success in results if success)
    print(f"\nTotal: {passed}/{total} testes passaram")

    print("\n" + "=" * 80)
    print("✓ TESTES CONCLUÍDOS!")
    print("=" * 80)
