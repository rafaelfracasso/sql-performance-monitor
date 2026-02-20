#!/usr/bin/env python3
"""
Script de teste para utilities (query_cache, sanitizer, performance_checker, etc).
"""
import os
import json
import tempfile
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()


def test_query_cache():
    """Testa QueryCache."""
    print("=" * 80)
    print("TESTE: QueryCache")
    print("=" * 80)

    from sql_monitor.query_cache import QueryCache
    from sql_monitor.utils.metrics_store import MetricsStore

    # Criar banco temporário para teste
    temp_db = tempfile.NamedTemporaryFile(mode='w', suffix='.duckdb', delete=False)
    temp_db.close()

    try:
        # Criar MetricsStore e QueryCache
        store = MetricsStore(db_path=temp_db.name)
        store.init_config_defaults()
        cache = QueryCache(metrics_store=store)

        # Teste 1: Adicionar e recuperar
        print("\n1️⃣  Testando add/get...")
        query_hash = "test_hash_1"

        cache.add_analyzed_query(
            query_hash=query_hash,
            database="test_db",
            schema="dbo",
            table="test_table",
            log_file="/tmp/test_query_1.log",
            query_preview="SELECT * FROM test"
        )
        retrieved = cache.get_cached_query(query_hash)

        if retrieved:
            print("   ✓ Query adicionada e recuperada com sucesso")
        else:
            print("   ✗ Falha ao recuperar query")
            return False

        # Teste 2: Cache miss
        print("\n2️⃣  Testando cache miss...")
        missing = cache.get_cached_query("non_existent_hash")
        if missing is None:
            print("   ✓ Cache miss funcionando corretamente")
        else:
            print("   ✗ Cache miss não funcionou")
            return False

        # Teste 3: Persistência automática
        print("\n3️⃣  Testando persistência automática...")
        cache2 = QueryCache(metrics_store=store)
        retrieved2 = cache2.get_cached_query(query_hash)

        if retrieved2:
            print("   ✓ Cache persistido automaticamente")
        else:
            print("   ✗ Falha ao acessar cache persistido")
            return False

        # Teste 4: Verificar validação de cache
        print("\n4️⃣  Testando verificação de validade...")
        old_hash = "old_hash"
        cache.add_analyzed_query(
            query_hash=old_hash,
            database="test_db",
            schema="dbo",
            table="old_table",
            log_file="/tmp/old_query.log",
            query_preview="SELECT * FROM old"
        )

        # Verificar se está válido (deve estar pois acabou de ser adicionado)
        if cache.is_cached_and_valid(old_hash):
            print("   ✓ Cache recém-adicionado está válido")
        else:
            print("   ✗ Cache deveria estar válido")
            return False

        # Teste 5: Estatísticas
        print("\n5️⃣  Testando estatísticas...")
        stats = cache.get_statistics()
        if stats['total_queries'] >= 2:
            print(f"   ✓ Estatísticas corretas: {stats['total_queries']} queries no cache")
        else:
            print(f"   ✗ Estatísticas incorretas: {stats}")
            return False

        print("\n✅ QueryCache funcionando corretamente!")
        return True

    except Exception as e:
        print(f"\n❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        try:
            os.unlink(temp_db.name)
        except:
            pass


def test_query_sanitizer():
    """Testa QuerySanitizer."""
    print("\n" + "=" * 80)
    print("TESTE: QuerySanitizer")
    print("=" * 80)

    from sql_monitor.utils.query_sanitizer import QuerySanitizer

    sanitizer = QuerySanitizer()

    test_cases = [
        {
            "name": "Valores literais numéricos",
            "input": "SELECT * FROM users WHERE id = 123",
            "should_contain": "@p",
            "should_not_contain": "123"
        },
        {
            "name": "Valores literais string",
            "input": "SELECT * FROM users WHERE name = 'John Doe'",
            "should_contain": "@p",
            "should_not_contain": "John"
        },
        {
            "name": "Múltiplos valores",
            "input": "SELECT * FROM users WHERE id = 1 AND status = 'active'",
            "should_contain": "@p",
            "should_not_contain": "active"
        },
        {
            "name": "Query sem valores literais",
            "input": "SELECT * FROM users",
            "should_contain": "SELECT",
            "should_not_contain": None
        }
    ]

    all_passed = True

    for i, test in enumerate(test_cases, 1):
        print(f"\n{i}️⃣  Testando: {test['name']}")
        try:
            # sanitize() retorna tupla (query_sanitizada, placeholder_map)
            result, placeholder_map = sanitizer.sanitize(test['input'])
            print(f"   Input:  {test['input'][:60]}")
            print(f"   Output: {result[:60] if result else 'None'}")
            if placeholder_map:
                print(f"   Placeholders: {len(placeholder_map)} encontrados")

            # Verificar condições
            if test['should_contain'] and test['should_contain'] not in result:
                print(f"   ✗ Deveria conter '{test['should_contain']}'")
                all_passed = False
            elif test['should_not_contain'] and test['should_not_contain'] in result:
                print(f"   ✗ Não deveria conter '{test['should_not_contain']}'")
                all_passed = False
            else:
                print(f"   ✓ Sanitização correta")

        except Exception as e:
            print(f"   ✗ Erro: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False

    if all_passed:
        print("\n✅ QuerySanitizer funcionando corretamente!")
    else:
        print("\n❌ Alguns testes falharam")

    return all_passed


def test_performance_checker():
    """Testa PerformanceChecker."""
    print("\n" + "=" * 80)
    print("TESTE: PerformanceChecker")
    print("=" * 80)

    import tempfile, os
    from sql_monitor.utils.performance_checker import PerformanceChecker
    from sql_monitor.utils.metrics_store import MetricsStore

    # Criar MetricsStore temporário com thresholds customizados
    fd, db_path = tempfile.mkstemp(suffix='.duckdb')
    os.close(fd); os.unlink(db_path)
    store = MetricsStore(db_path=db_path)
    store.init_config_defaults()
    # Ajustar thresholds para os valores do teste (sqlserver)
    store.execute_query(
        "UPDATE performance_thresholds_by_dbtype SET "
        "execution_time_ms = 5000, cpu_time_ms = 5000, logical_reads = 10000, "
        "physical_reads = 5000, writes = 1000 "
        "WHERE db_type = 'sqlserver'"
    )
    checker = PerformanceChecker(metrics_store=store, db_type='sqlserver')

    test_cases = [
        {
            "name": "Query com alto CPU",
            "query": {"cpu_time_ms": 10000, "duration_seconds": 1, "logical_reads": 1000},
            "should_flag": True
        },
        {
            "name": "Query com alto tempo de execução",
            "query": {"cpu_time_ms": 1000, "duration_seconds": 10, "logical_reads": 1000},
            "should_flag": True
        },
        {
            "name": "Query com alto logical reads",
            "query": {"cpu_time_ms": 1000, "duration_seconds": 1, "logical_reads": 20000},
            "should_flag": True
        },
        {
            "name": "Query com table scan",
            "query": {"cpu_time_ms": 100, "duration_seconds": 1, "logical_reads": 100, "has_table_scan": True},
            "should_flag": True
        },
        {
            "name": "Query normal (sem issues)",
            "query": {"cpu_time_ms": 100, "duration_seconds": 1, "logical_reads": 100},
            "should_flag": False
        }
    ]

    all_passed = True

    for i, test in enumerate(test_cases, 1):
        print(f"\n{i}️⃣  Testando: {test['name']}")
        try:
            is_problematic = checker.is_problematic(test['query'])
            expected = test['should_flag']

            if is_problematic == expected:
                print(f"   ✓ Detecção correta (problematic: {is_problematic})")
                if is_problematic:
                    reasons = checker.get_violation_reasons(test['query'])
                    print(f"   Razões: {', '.join(reasons[:2])}")
            else:
                print(f"   ✗ Esperado: {expected}, Obtido: {is_problematic}")
                all_passed = False

        except Exception as e:
            print(f"   ✗ Erro: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False

    # Cleanup
    store.close()
    if os.path.exists(db_path):
        os.unlink(db_path)

    if all_passed:
        print("\n✅ PerformanceChecker funcionando corretamente!")
    else:
        print("\n❌ Alguns testes falharam")

    return all_passed


def test_credentials_resolver():
    """Testa CredentialsResolver."""
    print("\n" + "=" * 80)
    print("TESTE: CredentialsResolver")
    print("=" * 80)

    from sql_monitor.utils.credentials_resolver import CredentialsResolver

    # Configurar variáveis de ambiente para teste
    os.environ['TEST_USER'] = 'test_username'
    os.environ['TEST_PASS'] = 'test_password'

    test_cases = [
        {
            "name": "Resolver variável simples",
            "input": {"username": "${TEST_USER}"},
            "expected": {"username": "test_username"}
        },
        {
            "name": "Resolver múltiplas variáveis",
            "input": {"username": "${TEST_USER}", "password": "${TEST_PASS}"},
            "expected": {"username": "test_username", "password": "test_password"}
        },
        {
            "name": "Valores literais (sem ${})",
            "input": {"server": "localhost", "port": "1433"},
            "expected": {"server": "localhost", "port": "1433"}
        },
        {
            "name": "Valores mistos",
            "input": {"username": "${TEST_USER}", "server": "localhost"},
            "expected": {"username": "test_username", "server": "localhost"}
        }
    ]

    all_passed = True

    for i, test in enumerate(test_cases, 1):
        print(f"\n{i}️⃣  Testando: {test['name']}")
        try:
            result = CredentialsResolver.resolve_credentials(test['input'])

            if result == test['expected']:
                print(f"   ✓ Resolução correta")
                print(f"   Input:    {test['input']}")
                print(f"   Expected: {test['expected']}")
                print(f"   Output:   {result}")
            else:
                print(f"   ✗ Resolução incorreta")
                print(f"   Expected: {test['expected']}")
                print(f"   Output:   {result}")
                all_passed = False

        except Exception as e:
            print(f"   ✗ Erro: {e}")
            all_passed = False

    # Teste de erro: variável não definida
    print(f"\n5️⃣  Testando variável não definida...")
    try:
        CredentialsResolver.resolve_credentials({"password": "${NON_EXISTENT_VAR}"})
        print(f"   ✗ Deveria ter lançado ValueError")
        all_passed = False
    except ValueError as e:
        print(f"   ✓ ValueError lançado corretamente: {e}")

    if all_passed:
        print("\n✅ CredentialsResolver funcionando corretamente!")
    else:
        print("\n❌ Alguns testes falharam")

    return all_passed


def test_structured_logger():
    """Testa StructuredLogger."""
    print("\n" + "=" * 80)
    print("TESTE: StructuredLogger")
    print("=" * 80)

    from sql_monitor.utils.structured_logger import create_logger

    try:
        print("\n1️⃣  Criando logger estruturado...")
        logger = create_logger("test_logger", structured=True)
        print("   ✓ Logger criado")

        print("\n2️⃣  Testando log com contexto...")
        logger.info("Teste de log", extra={"database": "TestDB", "operation": "test"})
        print("   ✓ Log com contexto funcionando")

        print("\n3️⃣  Testando diferentes níveis...")
        logger.debug("Debug message", extra={"detail": "teste"})
        logger.info("Info message", extra={"detail": "teste"})
        logger.warning("Warning message", extra={"detail": "teste"})
        logger.error("Error message", extra={"detail": "teste"})
        print("   ✓ Níveis de log funcionando")

        print("\n4️⃣  Testando logger não estruturado...")
        logger2 = create_logger("test_logger_plain", structured=False)
        logger2.info("Mensagem simples")
        print("   ✓ Logger não estruturado funcionando")

        print("\n✅ StructuredLogger funcionando corretamente!")
        return True

    except Exception as e:
        print(f"\n❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_sql_formatter():
    """Testa SQLFormatter."""
    print("\n" + "=" * 80)
    print("TESTE: SQLFormatter")
    print("=" * 80)

    from sql_monitor.utils.sql_formatter import format_sql

    test_cases = [
        {
            "name": "Query simples",
            "input": "SELECT * FROM users WHERE id = 1"
        },
        {
            "name": "Query com JOIN",
            "input": "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"
        },
        {
            "name": "Query com subquery",
            "input": "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"
        },
        {
            "name": "Formatação compacta",
            "input": "SELECT    *    FROM    users    WHERE    id=1"
        }
    ]

    all_passed = True

    for i, test in enumerate(test_cases, 1):
        print(f"\n{i}️⃣  Testando: {test['name']}")
        try:
            # Testar formatação normal
            result = format_sql(test['input'])
            print(f"   Input:  {test['input'][:60]}")
            print(f"   Output: {result[:100] if result else 'None'}...")

            if result and len(result) > 0:
                # Testar formatação compacta
                compact = format_sql(test['input'], compact=True)
                print(f"   Compact: {compact[:100] if compact else 'None'}...")
                print(f"   ✓ Formatação executada")
            else:
                print(f"   ✗ Formatação falhou")
                all_passed = False

        except Exception as e:
            print(f"   ✗ Erro: {e}")
            import traceback
            traceback.print_exc()
            all_passed = False

    if all_passed:
        print("\n✅ SQLFormatter funcionando corretamente!")
    else:
        print("\n❌ Alguns testes falharam")

    return all_passed


if __name__ == '__main__':
    print("\n🚀 Iniciando testes de utilities...\n")

    results = []

    # Teste 1: QueryCache
    result = test_query_cache()
    results.append(("QueryCache", result))

    # Teste 2: QuerySanitizer
    result = test_query_sanitizer()
    results.append(("QuerySanitizer", result))

    # Teste 3: PerformanceChecker
    result = test_performance_checker()
    results.append(("PerformanceChecker", result))

    # Teste 4: CredentialsResolver
    result = test_credentials_resolver()
    results.append(("CredentialsResolver", result))

    # Teste 5: StructuredLogger
    result = test_structured_logger()
    results.append(("StructuredLogger", result))

    # Teste 6: SQLFormatter
    result = test_sql_formatter()
    results.append(("SQLFormatter", result))

    # Resumo
    print("\n" + "=" * 80)
    print("RESUMO DOS TESTES DE UTILITIES")
    print("=" * 80)

    for name, success in results:
        status = "✓ PASSOU" if success else "✗ FALHOU"
        print(f"{status}: {name}")

    total = len(results)
    passed = sum(1 for _, success in results if success)
    print(f"\nTotal: {passed}/{total} testes passaram")

    if passed == total:
        print("\n🎉 TODOS OS TESTES PASSARAM!")
    else:
        print(f"\n⚠️  {total - passed} teste(s) falharam")

    print("\n" + "=" * 80)
    print("✓ TESTES DE UTILITIES CONCLUÍDOS!")
    print("=" * 80)
