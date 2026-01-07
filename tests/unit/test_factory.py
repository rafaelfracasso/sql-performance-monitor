#!/usr/bin/env python3
"""
Script de teste para Database Factory.
"""
import os
from dotenv import load_dotenv
from sql_monitor.core.database_types import DatabaseType
from sql_monitor.factories.database_factory import DatabaseFactory

# Carregar variáveis de ambiente
load_dotenv()

def test_factory_sqlserver():
    """Testa factory com SQL Server."""
    print("=" * 80)
    print("TESTE DATABASE FACTORY - SQL SERVER")
    print("=" * 80)

    credentials = {
        'server': os.getenv('SQL_SERVER'),
        'port': os.getenv('SQL_PORT', '1433'),
        'database': os.getenv('SQL_DATABASE'),
        'username': os.getenv('SQL_USERNAME'),
        'password': os.getenv('SQL_PASSWORD'),
        'driver': os.getenv('SQL_DRIVER', 'ODBC Driver 18 for SQL Server')
    }

    try:
        print("\nCriando componentes SQL Server via Factory...")
        conn, collector, extractor = DatabaseFactory.create_components(
            DatabaseType.SQLSERVER,
            credentials
        )

        print(f"   Connection: {type(conn).__name__}")
        print(f"   Collector: {type(collector).__name__}")
        print(f"   Extractor: {type(extractor).__name__}")

        # Testar conexão
        if conn.connect():
            print("\n   Testando conexão...")
            if conn.test_connection():
                print("   ✓ SQL Server Factory funcionando!")
            conn.disconnect()
            return True
        else:
            print("   ✗ Falha ao conectar")
            return False

    except Exception as e:
        print(f"   ✗ Erro: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_factory_postgresql():
    """Testa factory com PostgreSQL."""
    print("\n" + "=" * 80)
    print("TESTE DATABASE FACTORY - POSTGRESQL")
    print("=" * 80)

    credentials = {
        'server': os.getenv('PG_SERVER'),
        'port': os.getenv('PG_PORT', '5432'),
        'database': os.getenv('PG_DATABASE'),
        'username': os.getenv('PG_USERNAME'),
        'password': os.getenv('PG_PASSWORD')
    }

    try:
        print("\nCriando componentes PostgreSQL via Factory...")
        conn, collector, extractor = DatabaseFactory.create_components(
            DatabaseType.POSTGRESQL,
            credentials
        )

        print(f"   Connection: {type(conn).__name__}")
        print(f"   Collector: {type(collector).__name__}")
        print(f"   Extractor: {type(extractor).__name__}")

        # Testar conexão
        if conn.connect():
            print("\n   Testando conexão...")
            if conn.test_connection():
                print("   ✓ PostgreSQL Factory funcionando!")
            conn.disconnect()
            return True
        else:
            print("   ✗ Falha ao conectar")
            return False

    except Exception as e:
        print(f"   ✗ Erro: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_factory_hana():
    """Testa factory com SAP HANA (deve falhar - não implementado)."""
    print("\n" + "=" * 80)
    print("TESTE DATABASE FACTORY - SAP HANA (Esperado: NotImplementedError)")
    print("=" * 80)

    credentials = {
        'server': 'localhost',
        'port': '30015',
        'database': 'SYSTEMDB',
        'username': 'SYSTEM',
        'password': 'password'
    }

    try:
        print("\nTentando criar componentes SAP HANA via Factory...")
        conn, collector, extractor = DatabaseFactory.create_components(
            DatabaseType.HANA,
            credentials
        )
        print("   ✗ Não deveria ter chegado aqui!")
        return False

    except NotImplementedError as e:
        print(f"   ✓ NotImplementedError esperado: {e}")
        return True

    except Exception as e:
        print(f"   ✗ Erro inesperado: {e}")
        return False

def test_supported_databases():
    """Testa métodos auxiliares da factory."""
    print("\n" + "=" * 80)
    print("TESTE MÉTODOS AUXILIARES")
    print("=" * 80)

    print("\nBancos suportados:")
    supported = DatabaseFactory.get_supported_databases()
    for db_type in supported:
        print(f"   - {db_type.value}: {db_type.name}")

    print("\nVerificando suporte:")
    print(f"   SQL Server suportado: {DatabaseFactory.is_supported(DatabaseType.SQLSERVER)}")
    print(f"   PostgreSQL suportado: {DatabaseFactory.is_supported(DatabaseType.POSTGRESQL)}")
    print(f"   HANA suportado: {DatabaseFactory.is_supported(DatabaseType.HANA)}")

    return True

if __name__ == '__main__':
    print("\n🚀 Iniciando testes da Database Factory...\n")

    results = []

    # Teste 1: Métodos auxiliares
    results.append(("Métodos auxiliares", test_supported_databases()))

    # Teste 2: SQL Server (se credenciais disponíveis)
    if os.getenv('SQL_SERVER'):
        results.append(("SQL Server Factory", test_factory_sqlserver()))
    else:
        print("\n⚠️  Credenciais SQL Server não configuradas, pulando teste")

    # Teste 3: PostgreSQL (se credenciais disponíveis)
    if os.getenv('PG_SERVER'):
        results.append(("PostgreSQL Factory", test_factory_postgresql()))
    else:
        print("\n⚠️  Credenciais PostgreSQL não configuradas, pulando teste")

    # Teste 4: HANA (deve falhar)
    results.append(("HANA NotImplementedError", test_factory_hana()))

    # Resumo
    print("\n" + "=" * 80)
    print("RESUMO DOS TESTES")
    print("=" * 80)

    for name, success in results:
        status = "✓ PASSOU" if success else "✗ FALHOU"
        print(f"{status}: {name}")

    total = len(results)
    passed = sum(1 for _, success in results if success)
    print(f"\nTotal: {passed}/{total} testes passaram")

    if passed == total:
        print("\n✓ TODOS OS TESTES PASSARAM!")
    else:
        print(f"\n✗ {total - passed} teste(s) falharam")
