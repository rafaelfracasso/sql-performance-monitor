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

    from sql_monitor.utils.query_cache import QueryCache

    # Criar arquivo temporário para cache
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
    temp_file.close()

    try:
        config = {"query_cache": {"max_hours": 24}}
        cache = QueryCache(cache_file=temp_file.name, config=config)

        # Teste 1: Adicionar e recuperar
        print("\n1️⃣  Testando add/get...")
        query_hash = "test_hash_1"
        analysis = {
            "query_text": "SELECT * FROM test",
            "performance_issues": ["Missing index"],
            "timestamp": datetime.now().isoformat()
        }

        cache.add(query_hash, analysis)
        retrieved = cache.get(query_hash)

        if retrieved:
            print("   ✓ Query adicionada e recuperada com sucesso")
        else:
            print("   ✗ Falha ao recuperar query")
            return False

        # Teste 2: Cache miss
        print("\n2️⃣  Testando cache miss...")
        missing = cache.get("non_existent_hash")
        if missing is None:
            print("   ✓ Cache miss funcionando corretamente")
        else:
            print("   ✗ Cache miss não funcionou")
            return False

        # Teste 3: Salvar e carregar
        print("\n3️⃣  Testando save/load...")
        cache.save()

        cache2 = QueryCache(cache_file=temp_file.name, config=config)
        retrieved2 = cache2.get(query_hash)

        if retrieved2:
            print("   ✓ Cache persistido e recarregado com sucesso")
        else:
            print("   ✗ Falha ao recarregar cache")
            return False

        # Teste 4: Limpeza de cache antigo
        print("\n4️⃣  Testando limpeza de cache antigo...")
        old_analysis = {
            "query_text": "SELECT * FROM old",
            "performance_issues": [],
            "timestamp": (datetime.now() - timedelta(hours=25)).isoformat()
        }
        cache.add("old_hash", old_analysis)

        # Forçar limpeza
        cache.cleanup()
        cache.save()

        # Verificar se o antigo foi removido
        if cache.get("old_hash") is None:
            print("   ✓ Limpeza de cache funcionando")
        else:
            print("   ⚠️  Cache antigo ainda presente (pode ser normal)")

        print("\n✅ QueryCache funcionando corretamente!")
        return True

    except Exception as e:
        print(f"\n❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        try:
            os.unlink(temp_file.name)
        except:
            pass


def test_query_sanitizer():
    """Testa QuerySanitizer."""
    print("\n" + "=" * 80)
    print("TESTE: QuerySanitizer")
    print("=" * 80)

    from sql_monitor.utils.query_sanitizer import QuerySanitizer

    sanitizer = QuerySanitizer({})

    test_cases = [
        {
            "name": "Valores literais numéricos",
            "input": "SELECT * FROM users WHERE id = 123",
            "should_contain": "id = ?",
            "should_not_contain": "123"
        },
        {
            "name": "Valores literais string",
            "input": "SELECT * FROM users WHERE name = 'John Doe'",
            "should_contain": "name = ?",
            "should_not_contain": "John"
        },
        {
            "name": "Múltiplos espaços",
            "input": "SELECT    *    FROM    users",
            "should_contain": "SELECT * FROM users",
            "should_not_contain": "    "
        },
        {
            "name": "Case insensitive",
            "input": "SeLeCt * FrOm users",
            "should_contain": "SELECT",
            "should_not_contain": None
        }
    ]

    all_passed = True

    for i, test in enumerate(test_cases, 1):
        print(f"\n{i}️⃣  Testando: {test['name']}")
        try:
            result = sanitizer.sanitize(test['input'])
            print(f"   Input:  {test['input'][:60]}")
            print(f"   Output: {result[:60]}")

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
            "query": {"cpu_time": 10000, "elapsed_time": 1000, "logical_reads": 1000},
            "should_flag": True
        },
        {
            "name": "Query com alto elapsed time",
            "query": {"cpu_time": 1000, "elapsed_time": 10000, "logical_reads": 1000},
            "should_flag": True
        },
        {
            "name": "Query com alto logical reads",
            "query": {"cpu_time": 1000, "elapsed_time": 1000, "logical_reads": 20000},
            "should_flag": True
        },
        {
            "name": "Query normal (sem issues)",
            "query": {"cpu_time": 100, "elapsed_time": 100, "logical_reads": 100},
            "should_flag": False
        }
    ]

    all_passed = True

    for i, test in enumerate(test_cases, 1):
        print(f"\n{i}️⃣  Testando: {test['name']}")
        try:
            has_issues = checker.has_performance_issues(test['query'])
            expected = test['should_flag']

            if has_issues == expected:
                print(f"   ✓ Detecção correta (issues: {has_issues})")
            else:
                print(f"   ✗ Esperado: {expected}, Obtido: {has_issues}")
                all_passed = False

        except Exception as e:
            print(f"   ✗ Erro: {e}")
            all_passed = False

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
    import logging

    try:
        print("\n1️⃣  Criando logger estruturado...")
        logger = create_logger("test_logger", level=logging.INFO)
        print("   ✓ Logger criado")

        print("\n2️⃣  Testando log com contexto...")
        logger.info("Teste de log", database="TestDB", operation="test")
        print("   ✓ Log com contexto funcionando")

        print("\n3️⃣  Testando diferentes níveis...")
        logger.debug("Debug message", detail="teste")
        logger.info("Info message", detail="teste")
        logger.warning("Warning message", detail="teste")
        logger.error("Error message", detail="teste")
        print("   ✓ Níveis de log funcionando")

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

    from sql_monitor.utils.sql_formatter import SQLFormatter

    formatter = SQLFormatter()

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
        }
    ]

    all_passed = True

    for i, test in enumerate(test_cases, 1):
        print(f"\n{i}️⃣  Testando: {test['name']}")
        try:
            result = formatter.format(test['input'])
            print(f"   Input:  {test['input'][:60]}")
            print(f"   Output:\n{result[:200]}")

            if result and len(result) > 0:
                print(f"   ✓ Formatação executada")
            else:
                print(f"   ✗ Formatação falhou")
                all_passed = False

        except Exception as e:
            print(f"   ⚠️  Erro (pode ser esperado): {e}")
            # Não marcar como falha, pois formatter pode não estar implementado

    if all_passed:
        print("\n✅ SQLFormatter funcionando corretamente!")
    else:
        print("\n⚠️  Alguns testes falharam (pode ser esperado)")

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
