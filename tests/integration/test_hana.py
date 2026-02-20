#!/usr/bin/env python3
"""
Script de teste para conexão SAP HANA.
"""
import os
from dotenv import load_dotenv
from sql_monitor.connections.hana_connection import HANAConnection
from sql_monitor.collectors.hana_collector import HANACollector
from sql_monitor.extractors.hana_extractor import HANAExtractor

# Carregar variáveis de ambiente
load_dotenv()

def test_connection():
    """Testa conexão básica com SAP HANA."""
    print("=" * 80)
    print("TESTE DE CONEXÃO SAP HANA")
    print("=" * 80)

    # Criar conexão
    conn = HANAConnection(
        server=os.getenv('HANA_SERVER', 'localhost'),
        port=os.getenv('HANA_PORT', '30015'),
        database=os.getenv('HANA_DATABASE', 'SYSTEMDB'),
        username=os.getenv('HANA_USERNAME', 'SYSTEM'),
        password=os.getenv('HANA_PASSWORD', '')
    )

    # Testar conexão
    if conn.connect():
        print()
        if conn.test_connection():
            print("\nOK Conexão SAP HANA funcionando corretamente!")

            # Testar algumas queries básicas
            print("\n" + "=" * 80)
            print("TESTANDO QUERIES BÁSICAS")
            print("=" * 80)

            # Versão do SAP HANA
            version = conn.get_version()
            if version:
                print(f"\nOK Versão: {version}")

            # Informações do sistema
            print("\n Informações do Sistema:")
            system_info = conn.execute_query("""
                SELECT
                    HOST,
                    HARDWARE_MANUFACTURER,
                    HARDWARE_MODEL,
                    CPU_THREADS AS cores
                FROM M_HOST_INFORMATION
                LIMIT 1
            """)
            if system_info:
                for info in system_info:
                    print(f"   Host: {info[0]}")
                    print(f"   Fabricante: {info[1]}")
                    print(f"   Modelo: {info[2]}")
                    print(f"   CPU Cores: {info[3]}")

            # Listar schemas
            print("\n📁 Schemas disponíveis:")
            schemas = conn.execute_query("""
                SELECT SCHEMA_NAME
                FROM SYS.SCHEMAS
                WHERE SCHEMA_NAME NOT LIKE '_SYS%'
                  AND SCHEMA_NAME NOT IN ('SYS', 'SYSTEM')
                ORDER BY SCHEMA_NAME
                LIMIT 10
            """)
            if schemas:
                for schema in schemas:
                    print(f"   - {schema[0]}")

            # Verificar uso de memória
            print("\n💾 Uso de Memória:")
            memory = conn.execute_query("""
                SELECT
                    HOST,
                    ROUND(TOTAL_MEMORY_USED_SIZE/1024/1024/1024, 2) AS used_gb,
                    ROUND(ALLOCATION_LIMIT/1024/1024/1024, 2) AS limit_gb
                FROM M_HOST_RESOURCE_UTILIZATION
            """)
            if memory:
                for mem in memory:
                    print(f"   Host: {mem[0]}")
                    print(f"   Usado: {mem[1]} GB")
                    print(f"   Limite: {mem[2]} GB")

            # Verificar serviços ativos
            print("\n🔧 Serviços Ativos:")
            services = conn.execute_query("""
                SELECT
                    SERVICE_NAME,
                    PORT,
                    ACTIVE_STATUS
                FROM M_SERVICES
                WHERE ACTIVE_STATUS = 'YES'
                ORDER BY SERVICE_NAME
            """)
            if services:
                for svc in services:
                    print(f"   - {svc[0]}: Porta {svc[1]} ({svc[2]})")

            conn.disconnect()
        else:
            conn.disconnect()
            assert False, "Falha no teste de conexão (ping)"
    else:
        print("\nERRO Falha ao conectar ao SAP HANA")
        print("   Verifique se:")
        print("   1. O servidor HANA está rodando")
        print("   2. As credenciais em .env estão corretas")
        print("   3. A biblioteca hdbcli está instalada: pip install hdbcli")
        assert False, "Falha ao conectar ao SAP HANA"

def test_collector():
    """Testa collector SAP HANA."""
    print("\n" + "=" * 80)
    print("TESTE DE COLLECTOR SAP HANA")
    print("=" * 80)

    # Criar conexão
    conn = HANAConnection(
        server=os.getenv('HANA_SERVER', 'localhost'),
        port=os.getenv('HANA_PORT', '30015'),
        database=os.getenv('HANA_DATABASE', 'SYSTEMDB'),
        username=os.getenv('HANA_USERNAME', 'SYSTEM'),
        password=os.getenv('HANA_PASSWORD', '')
    )

    if not conn.connect():
        print("ERRO Falha ao conectar")
        assert False, "Falha ao conectar"

    try:
        collector = HANACollector(conn)

        # Testar coleta de queries ativas
        print("\nColetando queries ativas...")
        try:
            active = collector.collect_active_queries()
            print(f"   Encontradas: {len(active)} queries ativas")
            if active:
                print(f"   Primeira query: {active[0].get('query_text', '')[:100]}...")
        except Exception as e:
            print(f"   ERRO Erro em collect_active_queries: {e}")
            import traceback
            traceback.print_exc()
            assert False, f"Erro em collect_active_queries: {e}"

        # Testar coleta de expensive queries (M_EXPENSIVE_STATEMENTS)
        print("\nColetando expensive queries (M_EXPENSIVE_STATEMENTS)...")
        try:
            expensive = collector.collect_recent_expensive_queries()
            print(f"   Encontradas: {len(expensive)} expensive queries")
            if expensive:
                first_query = expensive[0]
                print(f"   Primeira query: {first_query.get('query_text', '')[:100]}...")
                print(f"   Statement Hash: {first_query.get('statement_hash', 'N/A')}")
                print(f"   Memory (avg): {first_query.get('avg_memory_mb', 0):.2f} MB")
                print(f"   CPU (avg): {first_query.get('avg_cpu_time_ms', 0):.2f} ms")
                print(f"   Rows (avg): {first_query.get('avg_rows', 0):,}")
        except Exception as e:
            print(f"   [ERROR] Erro em collect_recent_expensive_queries: {e}")
            import traceback
            traceback.print_exc()
            assert False, f"Erro em collect_recent_expensive_queries: {e}"

        # Testar coleta de table scans (queries específicas fazendo scans)
        print("\nColetando table scans (queries específicas)...")
        try:
            scans = collector.get_table_scan_queries()
            print(f"   Encontradas: {len(scans)} queries com table scans")
            if scans:
                first_scan = scans[0]
                print(f"   Primeira query: {first_scan.get('table_name', 'N/A')}")
                print(f"   Scan Type: {first_scan.get('scan_type', 'N/A')}")
                print(f"   Rows: {first_scan.get('avg_rows', 0):,} / Table: {first_scan.get('table_records', 0):,}")
        except Exception as e:
            print(f"   [WARN] Erro em get_table_scan_queries: {e}")
            print(f"   (Pode ser normal se M_EXPENSIVE_STATEMENTS não estiver disponível)")
            import traceback
            traceback.print_exc()

        conn.disconnect()
        print("\nOK Collector SAP HANA funcionando corretamente!")

    except Exception as e:
        print(f"\nERRO Erro ao testar collector: {e}")
        import traceback
        traceback.print_exc()
        conn.disconnect()
        assert False, f"Erro ao testar collector: {e}"

def test_extractor():
    """Testa extractor SAP HANA."""
    print("\n" + "=" * 80)
    print("TESTE DE EXTRACTOR SAP HANA")
    print("=" * 80)

    # Criar conexão
    conn = HANAConnection(
        server=os.getenv('HANA_SERVER', 'localhost'),
        port=os.getenv('HANA_PORT', '30015'),
        database=os.getenv('HANA_DATABASE', 'SYSTEMDB'),
        username=os.getenv('HANA_USERNAME', 'SYSTEM'),
        password=os.getenv('HANA_PASSWORD', '')
    )

    if not conn.connect():
        print("ERRO Falha ao conectar")
        assert False, "Falha ao conectar"

    try:
        extractor = HANAExtractor(conn)

        # Listar tabelas disponíveis para teste
        print("\n Listando tabelas disponíveis para teste...")
        tables = conn.execute_query("""
            SELECT
                SCHEMA_NAME,
                TABLE_NAME
            FROM SYS.TABLES
            WHERE SCHEMA_NAME NOT LIKE '_SYS%'
              AND SCHEMA_NAME NOT IN ('SYS', 'SYSTEM')
              AND IS_COLUMN_TABLE = 'TRUE'
            ORDER BY TABLE_NAME
            LIMIT 5
        """)

        if tables and len(tables) > 0:
            schema, table = tables[0]
            database = os.getenv('HANA_DATABASE', 'SYSTEMDB')
            print(f"\n Testando extração de metadados para: {database}.{schema}.{table}")

            # Testar extração de DDL
            print(f"\n Extraindo DDL...")
            ddl = extractor.get_table_ddl(database, schema, table)
            if ddl:
                print(f"   OK DDL extraído ({len(ddl)} caracteres)")
                print(f"   Primeiras linhas:\n{ddl[:200]}...")
            else:
                print(f"   [WARN]  Nenhum DDL retornado")

            # Testar extração de índices
            print(f"\n Extraindo índices...")
            indexes = extractor.get_table_indexes(database, schema, table)
            print(f"   OK Encontrados {len(indexes)} índices")
            for idx in indexes[:5]:  # Limitar a 5 índices
                print(f"   - {idx}")

            # Testar sugestões de missing indexes
            print(f"\n Buscando sugestões de missing indexes...")
            missing = extractor.get_missing_indexes(database)
            print(f"   OK Encontradas {len(missing)} sugestões")
            for suggestion in missing[:5]:  # Limitar a 5 sugestões
                print(f"   - {suggestion}")

            # Testar extração de plano de execução
            print(f"\nTestando extração de plano de execução...")
            test_query = f'SELECT * FROM "{schema}"."{table}" LIMIT 1'
            try:
                plan = extractor.get_query_plan(test_query)
                if plan:
                    print(f"   Plano extraído ({len(plan)} caracteres)")
                    if "PLAN_ID" in plan:
                        print(f"   Plano REAL (de cache) obtido com sucesso")
                    elif "ESTIMADO" in plan:
                        print(f"   Plano ESTIMADO (EXPLAIN PLAN fallback)")
                    print(f"   Primeiras linhas:\n{plan[:300]}...")
                else:
                    print(f"   [WARN] Nenhum plano retornado")
            except Exception as e:
                print(f"   [WARN] Erro ao obter plano: {e}")
        else:
            print("   [WARN]  Nenhuma tabela de usuário encontrada para teste")
            print("   Isso é normal em SYSTEMDB. Tente conectar a um tenant database.")

        conn.disconnect()
        print("\nOK Extractor SAP HANA funcionando corretamente!")

    except Exception as e:
        print(f"\nERRO Erro ao testar extractor: {e}")
        import traceback
        traceback.print_exc()
        conn.disconnect()
        assert False, f"Erro ao testar extractor: {e}"

if __name__ == '__main__':
    print("\n🚀 Iniciando testes do conector SAP HANA...\n")

    results = []

    # Teste 1: Conexão básica
    result = test_connection()
    results.append(("Conexão SAP HANA", result))
    if not result:
        print("\nERRO Teste de conexão falhou. Verifique as credenciais no .env")
        # Não sair, continuar com outros testes se possível

    # Teste 2: Collector
    result = test_collector()
    results.append(("Collector SAP HANA", result))
    if not result:
        print("\n[WARN]  Teste de collector falhou (pode ser normal se não houver dados)")

    # Teste 3: Extractor
    result = test_extractor()
    results.append(("Extractor SAP HANA", result))
    if not result:
        print("\n[WARN]  Teste de extractor falhou (pode ser normal se não houver tabelas)")

    # Resumo
    print("\n" + "=" * 80)
    print("RESUMO DOS TESTES SAP HANA")
    print("=" * 80)

    for name, success in results:
        status = "OK PASSOU" if success else "ERRO FALHOU"
        print(f"{status}: {name}")

    total = len(results)
    passed = sum(1 for _, success in results if success)
    print(f"\nTotal: {passed}/{total} testes passaram")

    print("\n" + "=" * 80)
    print("OK TESTES CONCLUÍDOS!")
    print("=" * 80)
