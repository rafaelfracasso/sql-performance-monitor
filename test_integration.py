#!/usr/bin/env python3
"""
Script de teste de integração completa.
Testa o fluxo end-to-end do sistema de monitoramento.
"""
import os
import json
import tempfile
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from sql_monitor.monitor.multi_monitor import MultiDatabaseMonitor
from sql_monitor.core.database_types import DatabaseType
from sql_monitor.factories.database_factory import DatabaseFactory

# Carregar variáveis de ambiente
load_dotenv()


def test_full_pipeline():
    """Testa o pipeline completo: conexão -> coleta -> análise -> cache."""
    print("=" * 80)
    print("TESTE DE INTEGRAÇÃO - PIPELINE COMPLETO")
    print("=" * 80)

    # Verificar quais bancos estão disponíveis
    has_sqlserver = bool(os.getenv('SQL_SERVER'))
    has_postgresql = bool(os.getenv('PG_SERVER'))
    has_hana = bool(os.getenv('HANA_SERVER'))

    if not (has_sqlserver or has_postgresql or has_hana):
        print("\n⚠️  Nenhum banco de dados configurado no .env")
        print("   Configure pelo menos um banco para executar este teste")
        return False

    print("\n📊 Bancos disponíveis para teste:")
    print(f"   SQL Server: {'✓' if has_sqlserver else '✗'}")
    print(f"   PostgreSQL: {'✓' if has_postgresql else '✗'}")
    print(f"   SAP HANA: {'✓' if has_hana else '✗'}")

    # Testar cada banco disponível
    results = []

    if has_sqlserver:
        print("\n" + "=" * 80)
        print("Testando SQL Server...")
        result = test_database_pipeline(DatabaseType.SQLSERVER, {
            'server': os.getenv('SQL_SERVER'),
            'port': os.getenv('SQL_PORT', '1433'),
            'database': os.getenv('SQL_DATABASE', 'master'),
            'username': os.getenv('SQL_USERNAME'),
            'password': os.getenv('SQL_PASSWORD'),
            'driver': os.getenv('SQL_DRIVER', 'ODBC Driver 18 for SQL Server')
        })
        results.append(("SQL Server Pipeline", result))

    if has_postgresql:
        print("\n" + "=" * 80)
        print("Testando PostgreSQL...")
        result = test_database_pipeline(DatabaseType.POSTGRESQL, {
            'server': os.getenv('PG_SERVER'),
            'port': os.getenv('PG_PORT', '5432'),
            'database': os.getenv('PG_DATABASE', 'postgres'),
            'username': os.getenv('PG_USERNAME'),
            'password': os.getenv('PG_PASSWORD')
        })
        results.append(("PostgreSQL Pipeline", result))

    if has_hana:
        print("\n" + "=" * 80)
        print("Testando SAP HANA...")
        result = test_database_pipeline(DatabaseType.HANA, {
            'server': os.getenv('HANA_SERVER'),
            'port': os.getenv('HANA_PORT', '30015'),
            'database': os.getenv('HANA_DATABASE', 'SYSTEMDB'),
            'username': os.getenv('HANA_USERNAME'),
            'password': os.getenv('HANA_PASSWORD')
        })
        results.append(("SAP HANA Pipeline", result))

    return results


def test_database_pipeline(db_type: DatabaseType, credentials: dict):
    """
    Testa o pipeline completo para um tipo de banco.

    Pipeline:
    1. Criar componentes via Factory
    2. Conectar ao banco
    3. Coletar queries ativas
    4. Coletar expensive queries
    5. Coletar table scans
    6. Verificar funcionamento

    Args:
        db_type: Tipo de banco
        credentials: Credenciais de conexão

    Returns:
        True se o pipeline funcionou, False caso contrário
    """
    try:
        print(f"\n1️⃣  Criando componentes via Factory...")
        conn, collector, extractor = DatabaseFactory.create_components(db_type, credentials)
        print(f"   ✓ Componentes criados")

        print(f"\n2️⃣  Conectando ao banco...")
        if not conn.connect():
            print(f"   ✗ Falha ao conectar")
            return False
        print(f"   ✓ Conectado com sucesso")

        print(f"\n3️⃣  Testando conexão...")
        if not conn.test_connection():
            print(f"   ✗ Teste de conexão falhou")
            conn.disconnect()
            return False
        print(f"   ✓ Conexão funcionando")

        print(f"\n4️⃣  Coletando queries ativas...")
        active_queries = collector.collect_active_queries()
        print(f"   ✓ Encontradas {len(active_queries)} queries ativas")

        print(f"\n5️⃣  Coletando expensive queries...")
        expensive_queries = collector.collect_recent_expensive_queries()
        print(f"   ✓ Encontradas {len(expensive_queries)} expensive queries")

        print(f"\n6️⃣  Coletando table scans...")
        table_scans = collector.get_table_scan_queries()
        print(f"   ✓ Encontradas {len(table_scans)} queries/tabelas com table scans")

        print(f"\n7️⃣  Desconectando...")
        conn.disconnect()
        print(f"   ✓ Desconectado com sucesso")

        print(f"\n✅ Pipeline {db_type.value} completado com sucesso!")
        return True

    except Exception as e:
        print(f"\n❌ Erro no pipeline {db_type.value}: {e}")
        import traceback
        traceback.print_exc()
        try:
            conn.disconnect()
        except:
            pass
        return False


def test_cache_persistence():
    """Testa persistência do cache entre execuções."""
    print("\n" + "=" * 80)
    print("TESTE DE INTEGRAÇÃO - PERSISTÊNCIA DE CACHE")
    print("=" * 80)

    from sql_monitor.utils.query_cache import QueryCache

    # Criar cache temporário
    temp_cache_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
    temp_cache_file.close()

    try:
        print("\n1️⃣  Criando cache e adicionando query...")
        cache1 = QueryCache(cache_file=temp_cache_file.name, config={})

        test_query_hash = "test_hash_123"
        test_analysis = {
            "query_text": "SELECT * FROM test",
            "performance_issues": ["Missing index"],
            "timestamp": datetime.now().isoformat()
        }

        cache1.add(test_query_hash, test_analysis)
        print(f"   ✓ Query adicionada ao cache")

        print("\n2️⃣  Salvando cache em disco...")
        cache1.save()
        print(f"   ✓ Cache salvo em {temp_cache_file.name}")

        print("\n3️⃣  Criando nova instância de cache (simulando reinício)...")
        cache2 = QueryCache(cache_file=temp_cache_file.name, config={})
        print(f"   ✓ Cache recarregado")

        print("\n4️⃣  Verificando se query está no cache...")
        if cache2.get(test_query_hash):
            print(f"   ✓ Query encontrada no cache!")
            print(f"   ✓ Persistência funcionando corretamente!")
            return True
        else:
            print(f"   ✗ Query não encontrada no cache")
            return False

    except Exception as e:
        print(f"\n✗ Erro ao testar persistência: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Limpar arquivo temporário
        try:
            os.unlink(temp_cache_file.name)
        except:
            pass


def test_query_sanitization():
    """Testa sanitização de queries."""
    print("\n" + "=" * 80)
    print("TESTE DE INTEGRAÇÃO - SANITIZAÇÃO DE QUERIES")
    print("=" * 80)

    from sql_monitor.utils.query_sanitizer import QuerySanitizer

    sanitizer = QuerySanitizer({})

    test_cases = [
        {
            "name": "Query com valores literais",
            "query": "SELECT * FROM users WHERE id = 123 AND name = 'John'",
            "expected_contains": ["users", "WHERE"]
        },
        {
            "name": "Query com múltiplos espaços",
            "query": "SELECT    *    FROM    products",
            "expected_contains": ["SELECT", "FROM", "products"]
        },
        {
            "name": "Query com comentários",
            "query": "SELECT * FROM orders -- comentário",
            "expected_contains": ["SELECT", "FROM", "orders"]
        }
    ]

    all_passed = True

    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{i}️⃣  Testando: {test_case['name']}")
        print(f"   Query original: {test_case['query'][:60]}...")

        try:
            sanitized = sanitizer.sanitize(test_case['query'])
            print(f"   Query sanitizada: {sanitized[:60]}...")

            # Verificar se contém palavras esperadas
            passed = all(word in sanitized for word in test_case['expected_contains'])

            if passed:
                print(f"   ✓ Sanitização correta")
            else:
                print(f"   ✗ Sanitização incorreta")
                all_passed = False

        except Exception as e:
            print(f"   ✗ Erro: {e}")
            all_passed = False

    return all_passed


def test_performance_checker():
    """Testa verificação de performance."""
    print("\n" + "=" * 80)
    print("TESTE DE INTEGRAÇÃO - PERFORMANCE CHECKER")
    print("=" * 80)

    from sql_monitor.utils.performance_checker import PerformanceChecker

    config = {
        "performance": {
            "cpu_threshold": 80.0,
            "elapsed_time_threshold": 5000,
            "logical_reads_threshold": 10000
        }
    }

    checker = PerformanceChecker(config)

    test_cases = [
        {
            "name": "Query com alto CPU",
            "query": {
                "cpu_time": 10000,
                "elapsed_time": 1000,
                "logical_reads": 1000
            },
            "should_flag": True
        },
        {
            "name": "Query com alto elapsed time",
            "query": {
                "cpu_time": 1000,
                "elapsed_time": 10000,
                "logical_reads": 1000
            },
            "should_flag": True
        },
        {
            "name": "Query normal",
            "query": {
                "cpu_time": 100,
                "elapsed_time": 100,
                "logical_reads": 100
            },
            "should_flag": False
        }
    ]

    all_passed = True

    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{i}️⃣  Testando: {test_case['name']}")

        try:
            has_issues = checker.has_performance_issues(test_case['query'])
            expected = test_case['should_flag']

            if has_issues == expected:
                print(f"   ✓ Detecção correta (flagged: {has_issues})")
            else:
                print(f"   ✗ Detecção incorreta (esperado: {expected}, obtido: {has_issues})")
                all_passed = False

        except Exception as e:
            print(f"   ✗ Erro: {e}")
            all_passed = False

    return all_passed


if __name__ == '__main__':
    print("\n🚀 Iniciando testes de integração...\n")

    all_results = []

    # Teste 1: Pipeline completo por banco
    print("\n" + "=" * 80)
    print("FASE 1: TESTES DE PIPELINE POR BANCO")
    print("=" * 80)
    pipeline_results = test_full_pipeline()
    if pipeline_results:
        all_results.extend(pipeline_results)

    # Teste 2: Persistência de cache
    print("\n" + "=" * 80)
    print("FASE 2: TESTES DE UTILIDADES")
    print("=" * 80)
    result = test_cache_persistence()
    all_results.append(("Persistência de Cache", result))

    # Teste 3: Sanitização de queries
    result = test_query_sanitization()
    all_results.append(("Sanitização de Queries", result))

    # Teste 4: Performance checker
    result = test_performance_checker()
    all_results.append(("Performance Checker", result))

    # Resumo Final
    print("\n" + "=" * 80)
    print("RESUMO FINAL DOS TESTES DE INTEGRAÇÃO")
    print("=" * 80)

    for name, success in all_results:
        status = "✓ PASSOU" if success else "✗ FALHOU"
        print(f"{status}: {name}")

    total = len(all_results)
    passed = sum(1 for _, success in all_results if success)
    print(f"\nTotal: {passed}/{total} testes passaram")

    if passed == total:
        print("\n🎉 TODOS OS TESTES DE INTEGRAÇÃO PASSARAM!")
    else:
        print(f"\n⚠️  {total - passed} teste(s) falharam")

    print("\n" + "=" * 80)
    print("✓ TESTES DE INTEGRAÇÃO CONCLUÍDOS!")
    print("=" * 80)
