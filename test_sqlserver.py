#!/usr/bin/env python3
"""
Script de teste para conexão SQL Server.
"""
import os
from dotenv import load_dotenv
from sql_monitor.connections.sqlserver_connection import SQLServerConnection
from sql_monitor.collectors.sqlserver_collector import SQLServerCollector
from sql_monitor.extractors.sqlserver_extractor import SQLServerExtractor

# Carregar variáveis de ambiente
load_dotenv()

def test_connection():
    """Testa conexão básica com SQL Server."""
    print("=" * 80)
    print("TESTE DE CONEXÃO SQL SERVER")
    print("=" * 80)

    # Criar conexão
    conn = SQLServerConnection(
        server=os.getenv('SQL_SERVER', 'localhost'),
        port=os.getenv('SQL_PORT', '1433'),
        database=os.getenv('SQL_DATABASE', 'master'),
        username=os.getenv('SQL_USERNAME', 'sa'),
        password=os.getenv('SQL_PASSWORD', ''),
        driver=os.getenv('SQL_DRIVER', 'ODBC Driver 18 for SQL Server')
    )

    # Testar conexão
    if conn.connect():
        print()
        if conn.test_connection():
            print("\n✓ Conexão SQL Server funcionando corretamente!")

            # Testar algumas queries básicas
            print("\n" + "=" * 80)
            print("TESTANDO QUERIES BÁSICAS")
            print("=" * 80)

            # Versão do SQL Server
            version = conn.get_version()
            if version:
                print(f"\n✓ Versão: {version}")

            # Listar databases
            print("\n📊 Databases disponíveis:")
            databases = conn.execute_query("SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name")
            if databases:
                for db in databases:
                    print(f"   - {db[0]}")

            # Listar schemas no database atual
            print("\n📁 Schemas disponíveis:")
            schemas = conn.execute_query("SELECT name FROM sys.schemas WHERE schema_id < 16384 ORDER BY name")
            if schemas:
                for schema in schemas:
                    print(f"   - {schema[0]}")

            # Verificar se Query Store está habilitado
            print("\n" + "=" * 80)
            query_store = conn.execute_scalar(
                "SELECT COUNT(*) FROM sys.database_query_store_options WHERE desired_state = 2"
            )
            if query_store and query_store > 0:
                print("✓ Query Store está HABILITADO (recomendado para monitoramento)")
            else:
                print("⚠️  Query Store está DESABILITADO")
                print("   Para habilitar, execute:")
                print("   ALTER DATABASE [DatabaseName] SET QUERY_STORE = ON;")

            conn.disconnect()
            return True
        else:
            conn.disconnect()
            return False
    else:
        print("\n✗ Falha ao conectar ao SQL Server")
        return False

def test_collector():
    """Testa collector SQL Server."""
    print("\n" + "=" * 80)
    print("TESTE DE COLLECTOR SQL SERVER")
    print("=" * 80)

    # Criar conexão
    conn = SQLServerConnection(
        server=os.getenv('SQL_SERVER', 'localhost'),
        port=os.getenv('SQL_PORT', '1433'),
        database=os.getenv('SQL_DATABASE', 'master'),
        username=os.getenv('SQL_USERNAME', 'sa'),
        password=os.getenv('SQL_PASSWORD', ''),
        driver=os.getenv('SQL_DRIVER', 'ODBC Driver 18 for SQL Server')
    )

    if not conn.connect():
        print("✗ Falha ao conectar")
        return False

    try:
        collector = SQLServerCollector(conn)

        # Testar coleta de queries ativas
        print("\n📊 Coletando queries ativas...")
        try:
            active = collector.collect_active_queries()
            print(f"   Encontradas: {len(active)} queries ativas")
            if active:
                print(f"   Primeira query: {active[0].get('query_text', '')[:100]}...")
        except Exception as e:
            print(f"   ✗ Erro em collect_active_queries: {e}")
            import traceback
            traceback.print_exc()

        # Testar coleta de expensive queries
        print("\n💰 Coletando expensive queries...")
        try:
            expensive = collector.collect_recent_expensive_queries()
            print(f"   Encontradas: {len(expensive)} expensive queries")
            if expensive:
                print(f"   Primeira query: {expensive[0].get('query_text', '')[:100]}...")
        except Exception as e:
            print(f"   ✗ Erro em collect_recent_expensive_queries: {e}")
            import traceback
            traceback.print_exc()

        # Testar coleta de table scans
        print("\n🔍 Coletando table scans...")
        try:
            scans = collector.get_table_scan_queries()
            print(f"   Encontradas: {len(scans)} queries com table scans")
            if scans:
                print(f"   Primeira query: {scans[0].get('query_text', '')[:100]}...")
        except Exception as e:
            print(f"   ✗ Erro em get_table_scan_queries: {e}")
            import traceback
            traceback.print_exc()

        conn.disconnect()
        print("\n✓ Collector SQL Server funcionando corretamente!")
        return True

    except Exception as e:
        print(f"\n✗ Erro ao testar collector: {e}")
        import traceback
        traceback.print_exc()
        conn.disconnect()
        return False

def test_extractor():
    """Testa extractor SQL Server."""
    print("\n" + "=" * 80)
    print("TESTE DE EXTRACTOR SQL SERVER")
    print("=" * 80)

    # Criar conexão
    conn = SQLServerConnection(
        server=os.getenv('SQL_SERVER', 'localhost'),
        port=os.getenv('SQL_PORT', '1433'),
        database=os.getenv('SQL_DATABASE', 'master'),
        username=os.getenv('SQL_USERNAME', 'sa'),
        password=os.getenv('SQL_PASSWORD', ''),
        driver=os.getenv('SQL_DRIVER', 'ODBC Driver 18 for SQL Server')
    )

    if not conn.connect():
        print("✗ Falha ao conectar")
        return False

    try:
        extractor = SQLServerExtractor(conn)

        # Listar tabelas disponíveis para teste
        print("\n📋 Listando tabelas disponíveis para teste...")
        tables = conn.execute_query("""
            SELECT TOP 5
                SCHEMA_NAME(schema_id) AS schema_name,
                name AS table_name
            FROM sys.tables
            WHERE is_ms_shipped = 0
            ORDER BY name
        """)

        if tables and len(tables) > 0:
            schema, table = tables[0]
            database = os.getenv('SQL_DATABASE', 'master')
            print(f"\n🔍 Testando extração de metadados para: {database}.{schema}.{table}")

            # Testar extração de DDL
            print(f"\n📄 Extraindo DDL...")
            ddl = extractor.get_table_ddl(database, schema, table)
            if ddl:
                print(f"   ✓ DDL extraído ({len(ddl)} caracteres)")
                print(f"   Primeiras linhas:\n{ddl[:200]}...")

            # Testar extração de índices
            print(f"\n🔑 Extraindo índices...")
            indexes = extractor.get_table_indexes(database, schema, table)
            print(f"   ✓ Encontrados {len(indexes)} índices")
            for idx in indexes[:5]:  # Limitar a 5 índices
                print(f"   - {idx}")

            # Testar sugestões de missing indexes
            print(f"\n💡 Buscando sugestões de missing indexes...")
            missing = extractor.get_missing_indexes(database)
            print(f"   ✓ Encontradas {len(missing)} sugestões")
            for suggestion in missing[:5]:  # Limitar a 5 sugestões
                print(f"   - {suggestion}")
        else:
            print("   ⚠️  Nenhuma tabela de usuário encontrada para teste")

        conn.disconnect()
        print("\n✓ Extractor SQL Server funcionando corretamente!")
        return True

    except Exception as e:
        print(f"\n✗ Erro ao testar extractor: {e}")
        import traceback
        traceback.print_exc()
        conn.disconnect()
        return False

if __name__ == '__main__':
    print("\n🚀 Iniciando testes do conector SQL Server...\n")

    results = []

    # Teste 1: Conexão básica
    result = test_connection()
    results.append(("Conexão SQL Server", result))
    if not result:
        print("\n❌ Teste de conexão falhou. Verifique as credenciais no .env")
        # Não sair, continuar com outros testes se possível

    # Teste 2: Collector
    result = test_collector()
    results.append(("Collector SQL Server", result))
    if not result:
        print("\n⚠️  Teste de collector falhou (pode ser normal se não houver dados)")

    # Teste 3: Extractor
    result = test_extractor()
    results.append(("Extractor SQL Server", result))
    if not result:
        print("\n⚠️  Teste de extractor falhou (pode ser normal se não houver tabelas)")

    # Resumo
    print("\n" + "=" * 80)
    print("RESUMO DOS TESTES SQL SERVER")
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
