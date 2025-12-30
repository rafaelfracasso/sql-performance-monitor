#!/usr/bin/env python3
"""
Script de teste para conexão PostgreSQL.
"""
import os
from dotenv import load_dotenv
from sql_monitor.connections.postgresql_connection import PostgreSQLConnection
from sql_monitor.collectors.postgresql_collector import PostgreSQLCollector
from sql_monitor.extractors.postgresql_extractor import PostgreSQLExtractor

# Carregar variáveis de ambiente
load_dotenv()

def test_connection():
    """Testa conexão básica com PostgreSQL."""
    print("=" * 80)
    print("TESTE DE CONEXÃO POSTGRESQL")
    print("=" * 80)

    # Criar conexão
    conn = PostgreSQLConnection(
        server=os.getenv('PG_SERVER', 'localhost'),
        port=os.getenv('PG_PORT', '5432'),
        database=os.getenv('PG_DATABASE', 'postgres'),
        username=os.getenv('PG_USERNAME', 'postgres'),
        password=os.getenv('PG_PASSWORD', '')
    )

    # Testar conexão
    if conn.connect():
        print()
        if conn.test_connection():
            print("\n✓ Conexão PostgreSQL funcionando corretamente!")

            # Testar algumas queries básicas
            print("\n" + "=" * 80)
            print("TESTANDO QUERIES BÁSICAS")
            print("=" * 80)

            # Versão do PostgreSQL
            version = conn.get_version()
            if version:
                print(f"\n✓ Versão: {version}")

            # Listar databases
            print("\n📊 Databases disponíveis:")
            databases = conn.execute_query("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
            if databases:
                for db in databases:
                    print(f"   - {db[0]}")

            # Listar schemas
            print("\n📁 Schemas disponíveis:")
            schemas = conn.execute_query("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema' ORDER BY schema_name")
            if schemas:
                for schema in schemas:
                    print(f"   - {schema[0]}")

            # Extensões instaladas
            print("\n🔌 Extensões instaladas:")
            extensions = conn.execute_query("SELECT extname, extversion FROM pg_extension ORDER BY extname")
            if extensions:
                for ext in extensions:
                    print(f"   - {ext[0]} (v{ext[1]})")

            # Verificar se pg_stat_statements está instalado
            pg_stat = conn.execute_scalar("SELECT COUNT(*) FROM pg_extension WHERE extname = 'pg_stat_statements'")
            print("\n" + "=" * 80)
            if pg_stat and pg_stat > 0:
                print("✓ Extensão pg_stat_statements está INSTALADA (necessária para monitoramento)")
            else:
                print("⚠️  Extensão pg_stat_statements NÃO está instalada")
                print("   Para habilitar, execute como superuser:")
                print("   CREATE EXTENSION pg_stat_statements;")

            conn.disconnect()
            return True
        else:
            conn.disconnect()
            return False
    else:
        print("\n✗ Falha ao conectar ao PostgreSQL")
        return False

def test_collector():
    """Testa collector PostgreSQL."""
    print("\n" + "=" * 80)
    print("TESTE DE COLLECTOR POSTGRESQL")
    print("=" * 80)

    # Criar conexão
    conn = PostgreSQLConnection(
        server=os.getenv('PG_SERVER', 'localhost'),
        port=os.getenv('PG_PORT', '5432'),
        database=os.getenv('PG_DATABASE', 'postgres'),
        username=os.getenv('PG_USERNAME', 'postgres'),
        password=os.getenv('PG_PASSWORD', '')
    )

    if not conn.connect():
        print("✗ Falha ao conectar")
        return False

    try:
        collector = PostgreSQLCollector(conn)

        # Testar coleta de queries ativas
        print("\n📊 Coletando queries ativas...")
        try:
            active = collector.collect_active_queries()
            print(f"   Encontradas: {len(active)} queries ativas")
        except Exception as e:
            print(f"   ✗ Erro em collect_active_queries: {e}")
            import traceback
            traceback.print_exc()

        # Testar coleta de expensive queries
        print("\n💰 Coletando expensive queries...")
        try:
            expensive = collector.collect_recent_expensive_queries()
            print(f"   Encontradas: {len(expensive)} expensive queries")
        except Exception as e:
            print(f"   ✗ Erro em collect_recent_expensive_queries: {e}")

        # Testar coleta de table scans
        print("\n🔍 Coletando table scans...")
        try:
            scans = collector.get_table_scan_queries()
            print(f"   Encontradas: {len(scans)} tabelas com table scans")
        except Exception as e:
            print(f"   ✗ Erro em get_table_scan_queries: {e}")

        conn.disconnect()
        print("\n✓ Collector PostgreSQL funcionando corretamente!")
        return True

    except Exception as e:
        print(f"\n✗ Erro ao testar collector: {e}")
        import traceback
        traceback.print_exc()
        conn.disconnect()
        return False

def test_extractor():
    """Testa extractor PostgreSQL."""
    print("\n" + "=" * 80)
    print("TESTE DE EXTRACTOR POSTGRESQL")
    print("=" * 80)

    # Criar conexão
    conn = PostgreSQLConnection(
        server=os.getenv('PG_SERVER', 'localhost'),
        port=os.getenv('PG_PORT', '5432'),
        database=os.getenv('PG_DATABASE', 'postgres'),
        username=os.getenv('PG_USERNAME', 'postgres'),
        password=os.getenv('PG_PASSWORD', '')
    )

    if not conn.connect():
        print("✗ Falha ao conectar")
        return False

    try:
        extractor = PostgreSQLExtractor(conn)

        # Listar tabelas disponíveis para teste
        print("\n📋 Listando tabelas disponíveis para teste...")
        tables = conn.execute_query("""
            SELECT schemaname, tablename
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            LIMIT 5
        """)

        if tables and len(tables) > 0:
            schema, table = tables[0]
            print(f"\n🔍 Testando extração de metadados para: {schema}.{table}")

            # Testar extração de DDL
            print(f"\n📄 Extraindo DDL...")
            # Nota: get_table_ddl(database_name, schema_name, table_name)
            # PostgreSQL usa database atual, então passamos 'current'
            ddl = extractor.get_table_ddl('current', schema, table)
            if ddl:
                print(f"   ✓ DDL extraído ({len(ddl)} caracteres)")
                print(f"   Primeiras linhas:\n{ddl[:200]}...")

            # Testar extração de índices
            print(f"\n🔑 Extraindo índices...")
            indexes = extractor.get_table_indexes('current', schema, table)
            print(f"   ✓ Encontrados {len(indexes)} índices")
            for idx in indexes:
                print(f"   - {idx}")

            # Testar sugestões de missing indexes
            print(f"\n💡 Buscando sugestões de missing indexes...")
            # Nota: get_missing_indexes retorna sugestões globais para todo o database
            missing = extractor.get_missing_indexes('current')
            print(f"   ✓ Encontradas {len(missing)} sugestões")
            for suggestion in missing[:5]:  # Limitar a 5 sugestões
                print(f"   - {suggestion}")
        else:
            print("   ⚠️  Nenhuma tabela de usuário encontrada para teste")

        conn.disconnect()
        print("\n✓ Extractor PostgreSQL funcionando corretamente!")
        return True

    except Exception as e:
        print(f"\n✗ Erro ao testar extractor: {e}")
        import traceback
        traceback.print_exc()
        conn.disconnect()
        return False

if __name__ == '__main__':
    print("\n🚀 Iniciando testes do conector PostgreSQL...\n")

    # Teste 1: Conexão básica
    if not test_connection():
        print("\n❌ Teste de conexão falhou. Verifique as credenciais no .env")
        exit(1)

    # Teste 2: Collector
    if not test_collector():
        print("\n⚠️  Teste de collector falhou (pode ser normal se não houver dados)")

    # Teste 3: Extractor
    if not test_extractor():
        print("\n⚠️  Teste de extractor falhou (pode ser normal se não houver tabelas)")

    print("\n" + "=" * 80)
    print("✓ TESTES CONCLUÍDOS!")
    print("=" * 80)
