#!/usr/bin/env python3
"""
Teste de ciclo completo de monitoramento com SQL Server.
"""
import os
import time
from dotenv import load_dotenv

load_dotenv()

def test_full_monitoring_cycle():
    """Testa um ciclo completo de monitoramento em SQL Server."""
    print("=" * 80)
    print("TESTE: Ciclo Completo de Monitoramento - SQL Server")
    print("=" * 80)

    from sql_monitor.core.database_types import DatabaseType
    from sql_monitor.factories.database_factory import DatabaseFactory
    from sql_monitor.query_cache import QueryCache
    from sql_monitor.utils.metrics_store import MetricsStore
    from sql_monitor.utils.performance_checker import PerformanceChecker
    from sql_monitor.utils.query_sanitizer import QuerySanitizer
    from sql_monitor.utils.llm_analyzer import LLMAnalyzer

    # Verificar credenciais
    server = os.getenv('SQL_SERVER')
    if not server:
        print("\n⚠️  Credenciais SQL Server não configuradas")
        return False

    print(f"\n✓ Servidor: {server}")

    # Criar componentes
    print("\n" + "=" * 80)
    print("FASE 1: Inicialização de Componentes")
    print("=" * 80)

    credentials = {
        'server': os.getenv('SQL_SERVER'),
        'port': os.getenv('SQL_PORT', '1433'),
        'database': os.getenv('SQL_DATABASE', 'master'),
        'username': os.getenv('SQL_USERNAME'),
        'password': os.getenv('SQL_PASSWORD'),
        'driver': os.getenv('SQL_DRIVER', 'ODBC Driver 18 for SQL Server')
    }

    try:
        print("\n1. Criando componentes via Factory...")
        connection, collector, extractor = DatabaseFactory.create_components(
            DatabaseType.SQLSERVER,
            credentials
        )
        print("   ✓ Connection, Collector, Extractor criados")

        print("\n2. Criando utilitários...")
        store = MetricsStore(db_path='/tmp/test_cycle_metrics.duckdb')
        store.init_config_defaults()
        cache = QueryCache(metrics_store=store)
        print("   ✓ QueryCache criado")

        checker = PerformanceChecker(
            metrics_store=store,
            db_type='sqlserver'
        )
        print("   ✓ PerformanceChecker criado")

        sanitizer = QuerySanitizer()
        print("   ✓ QuerySanitizer criado")

        # LLM Analyzer (opcional)
        api_key = os.getenv('GROQ_API_KEY')
        llm_enabled = api_key and api_key != 'your_groq_api_key_here'

        if llm_enabled:
            analyzer = LLMAnalyzer({
                'api_key': api_key,
                'model': 'llama-3.3-70b-versatile',
                'max_tokens': 1500
            })
            print("   ✓ LLM Analyzer criado")
        else:
            analyzer = None
            print("   ⚠️  LLM Analyzer desabilitado (sem API key)")

        # Conectar
        print("\n" + "=" * 80)
        print("FASE 2: Conexão ao Banco")
        print("=" * 80)

        if not connection.connect():
            print("\n✗ Falha ao conectar")
            return False

        print("\n✓ Conectado com sucesso!")

        # Coletar queries
        print("\n" + "=" * 80)
        print("FASE 3: Coleta de Queries")
        print("=" * 80)

        print("\n1. Coletando queries ativas (durando >= 5s)...")
        active_queries = collector.collect_active_queries(min_duration_seconds=5)
        print(f"   ✓ Encontradas: {len(active_queries)} queries ativas")

        print("\n2. Coletando expensive queries (top 5)...")
        expensive_queries = collector.collect_recent_expensive_queries(top_n=5)
        print(f"   ✓ Encontradas: {len(expensive_queries)} expensive queries")

        print("\n3. Coletando table scans...")
        table_scans = collector.get_table_scan_queries()
        print(f"   ✓ Encontradas: {len(table_scans)} com table scans")

        all_queries = active_queries + expensive_queries[:3]  # Limitando para teste

        # Processar queries
        if all_queries:
            print("\n" + "=" * 80)
            print(f"FASE 4: Processamento de {len(all_queries)} Queries")
            print("=" * 80)

            for i, query_info in enumerate(all_queries[:2], 1):  # Apenas 2 para teste
                print(f"\n--- Query {i}/{min(2, len(all_queries))} ---")

                query_text = query_info.get('query_text', '')
                if not query_text or len(query_text) < 10:
                    print("   ⚠️  Query muito curta, pulando...")
                    continue

                print(f"Query: {query_text[:80]}...")

                # 1. Verificar performance
                is_problematic = checker.is_problematic(query_info)
                print(f"\n1. Performance Check: {'❌ Problemática' if is_problematic else '✅ OK'}")

                if is_problematic:
                    reasons = checker.get_violation_reasons(query_info)
                    for reason in reasons[:2]:
                        print(f"   - {reason}")

                # 2. Sanitizar query
                print("\n2. Sanitizando query...")
                try:
                    sanitized, placeholder_map = sanitizer.sanitize(query_text)
                    print(f"   ✓ Sanitizada: {sanitized[:60]}...")

                    # Gerar hash para cache
                    query_hash = cache.generate_hash(
                        sanitized_query=sanitized,
                        database=query_info.get('database_name', 'unknown'),
                        schema='dbo',
                        table='unknown'
                    )

                    # 3. Verificar cache
                    print("\n3. Verificando cache...")
                    if cache.is_cached_and_valid(query_hash):
                        print(f"   ✓ Query já analisada recentemente")
                        cached = cache.get_cached_query(query_hash)
                        hours_ago = cache.get_hours_since_analysis(query_hash)
                        print(f"   📊 Analisada há {hours_ago:.1f} horas")
                    else:
                        print(f"   ⚠️  Query não está em cache")

                        # 4. Análise LLM (apenas se habilitado e problemático)
                        if analyzer and is_problematic:
                            print("\n4. Análise LLM...")
                            try:
                                analysis = analyzer.analyze_query_performance(
                                    sanitized_query=sanitized,
                                    placeholder_map=str(placeholder_map),
                                    table_ddl="-- DDL not available in test",
                                    existing_indexes="-- Indexes not available in test",
                                    metrics=query_info,
                                    query_plan=None
                                )

                                if analysis and analysis.get('explanation'):
                                    print(f"   ✓ Análise recebida")
                                    print(f"   📝 {analysis['explanation'][:100]}...")

                                    # Adicionar ao cache
                                    cache.add_analyzed_query(
                                        query_hash=query_hash,
                                        database=query_info.get('database_name', 'unknown'),
                                        schema='dbo',
                                        table='unknown',
                                        log_file='/tmp/test_analysis.log',
                                        query_preview=query_text[:100]
                                    )
                                    print(f"   ✓ Adicionada ao cache")
                                else:
                                    print(f"   ⚠️  Análise vazia ou incompleta")

                            except Exception as e:
                                print(f"   ⚠️  Erro na análise LLM: {e}")
                        elif not analyzer:
                            print("\n4. Análise LLM: ⚠️  Desabilitado")
                        else:
                            print("\n4. Análise LLM: ⚠️  Pulado (query não é problemática)")

                except Exception as e:
                    print(f"   ✗ Erro no processamento: {e}")
                    import traceback
                    traceback.print_exc()

        else:
            print("\n⚠️  Nenhuma query coletada para processar")

        # Estatísticas finais
        print("\n" + "=" * 80)
        print("FASE 5: Estatísticas Finais")
        print("=" * 80)

        print(f"\n📊 Queries coletadas:")
        print(f"   - Ativas: {len(active_queries)}")
        print(f"   - Expensive: {len(expensive_queries)}")
        print(f"   - Table scans: {len(table_scans)}")
        print(f"   - Total processadas: {min(2, len(all_queries))}")

        cache_stats = cache.get_statistics()
        print(f"\n📦 Cache:")
        print(f"   - Total queries: {cache_stats['total_queries']} entradas")
        print(f"   - Total views: {cache_stats['total_views']}")
        print(f"   - Média views/query: {cache_stats['avg_views_per_query']:.1f}")

        # Desconectar
        connection.disconnect()
        print("\n✓ Desconectado")

        print("\n✅ Ciclo completo de monitoramento executado com sucesso!")
        return True

    except Exception as e:
        print(f"\n✗ Erro no ciclo: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    print("\n🚀 Iniciando teste de ciclo completo...\n")

    success = test_full_monitoring_cycle()

    print("\n" + "=" * 80)
    print("RESULTADO FINAL")
    print("=" * 80)

    if success:
        print("\n✅ TESTE DE CICLO COMPLETO PASSOU!")
    else:
        print("\n✗ TESTE DE CICLO COMPLETO FALHOU!")
