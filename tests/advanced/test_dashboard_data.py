#!/usr/bin/env python3
"""
Teste para verificar se há dados no MetricsStore e se o dashboard funciona.
"""
import os
from dotenv import load_dotenv

load_dotenv()


def test_metrics_store_data():
    """Verifica se há dados no MetricsStore."""
    print("=" * 80)
    print("TESTE: Dados do MetricsStore para Dashboard")
    print("=" * 80)

    try:
        from sql_monitor.utils.metrics_store import MetricsStore
        from pathlib import Path

        # MetricsStore usa DuckDB (arquivo local)
        db_path = "logs/metrics.duckdb"

        print("\n1. Criando MetricsStore...")
        print(f"   Database: {db_path} (DuckDB)")

        # Verificar se arquivo existe
        if Path(db_path).exists():
            print(f"   ✓ Arquivo existe: {Path(db_path).stat().st_size / 1024:.1f} KB")
        else:
            print(f"   ⚠️  Arquivo não existe (será criado vazio)")

        store = MetricsStore(db_path=db_path)
        print("   ✓ MetricsStore criado")

        # Verificar se há dados
        print("\n2. Verificando dados no banco...")
        conn = store._get_connection()

        # Contar queries coletadas
        queries_count = conn.execute("SELECT COUNT(*) FROM queries_collected").fetchone()[0]
        print(f"   Queries coletadas: {queries_count}")

        # Contar análises LLM
        analyses_count = conn.execute("SELECT COUNT(*) FROM llm_analyses").fetchone()[0]
        print(f"   Análises LLM: {analyses_count}")

        # Contar alertas
        alerts_count = conn.execute("SELECT COUNT(*) FROM performance_alerts").fetchone()[0]
        print(f"   Alertas: {alerts_count}")

        # Contar ciclos de monitoramento
        cycles_count = conn.execute("SELECT COUNT(*) FROM monitoring_cycles").fetchone()[0]
        print(f"   Ciclos de monitoramento: {cycles_count}")

        if queries_count == 0 and analyses_count == 0:
            print("\n⚠️  ATENÇÃO: Banco de dados está VAZIO!")
            print("   O dashboard mostrará zeros porque não há dados históricos")
            print("   Para popular dados, execute:")
            print("   - python tests/integration/test_sqlserver.py")
            print("   - Ou rode o monitor principal por algumas horas")
        else:
            print("\n✓ Há dados no banco!")

        print("\n3. Testando QueryAnalytics...")
        from sql_monitor.utils.query_analytics import QueryAnalytics

        analytics = QueryAnalytics(store)
        print("   ✓ QueryAnalytics criado")

        # Buscar resumo executivo
        summary = analytics.get_executive_summary(hours=24)
        print(f"\n4. Resumo Executivo (últimas 24h):")
        print(f"   DB Time Total: {summary.get('total_db_time_ms', 0):.0f} ms")
        print(f"   Latência Média: {summary.get('avg_latency_ms', 0):.1f} ms")
        print(f"   Total de ocorrências: {summary['total_occurrences']}")
        print(f"   Análises LLM: {summary['analyses_performed']}")
        print(f"   Total de tokens: {summary['total_llm_tokens']}")
        print(f"   Alertas totais: {summary['alerts']['total']}")
        print(f"   - Críticos: {summary['alerts']['critical']}")
        print(f"   - Altos: {summary['alerts']['high']}")

        # Verificar health
        health = analytics.get_monitoring_health(hours=24)
        print(f"\n5. Health do Monitoramento:")
        print(f"   Instâncias ativas: {len(health['active_instances'])}")
        print(f"   Ciclos totais: {health['total_cycles']}")
        print(f"   Ciclos com sucesso: {health['successful_cycles']}")
        print(f"   Taxa de sucesso: {health['success_rate_percent']:.1f}%")

        # Verificar worst queries
        worst_result = analytics.get_worst_performers(metric='cpu_time_ms', hours=24, limit=5)
        worst_queries = worst_result['queries']
        print(f"\n6. Top 5 Queries Problemáticas:")
        print(f"   Total encontradas: {worst_result['total']}")

        if len(worst_queries) > 0:
            for i, query in enumerate(worst_queries[:3], 1):
                print(f"   {i}. {query['instance_name']} - CPU: {query['avg_cpu_time_ms']:.0f}ms")

        # Verificar alertas
        alerts = analytics.get_recent_alerts(hours=24, limit=10)
        print(f"\n7. Alertas Recentes:")
        print(f"   Total encontrados: {len(alerts)}")

        print("\n✅ Teste concluído!")

    except ImportError as e:
        print(f"\n✗ Erro de importação: {e}")
        assert False, f"Erro de importação: {e}"
    except Exception as e:
        print(f"\n✗ Erro: {e}")
        import traceback
        traceback.print_exc()
        assert False, f"Erro: {e}"


def test_dashboard_with_mock_data():
    """Testa dashboard com dados mockados se não houver dados reais."""
    print("\n" + "=" * 80)
    print("TESTE: Dashboard com Dados Mockados")
    print("=" * 80)

    print("\n⚠️  Se o dashboard está mostrando dados estranhos,")
    print("   provavelmente o DuckDB foi populado com dados de teste.")
    print("\nPara limpar dados de teste:")
    print("   rm -rf sql_monitor_data/metrics.duckdb*")
    print("\nPara popular com dados reais:")
    print("   1. Configure credenciais no .env")
    print("   2. Execute: python -m sql_monitor.main")
    print("   3. Deixe rodar por algumas horas para coletar dados")


if __name__ == '__main__':
    print("\n🚀 Verificando dados do dashboard...\n")

    results = []

    # Teste 1: Verificar MetricsStore
    results.append(("MetricsStore Data", test_metrics_store_data()))

    # Teste 2: Informações sobre mock data
    results.append(("Mock Data Info", test_dashboard_with_mock_data()))

    # Resumo
    print("\n" + "=" * 80)
    print("RESUMO")
    print("=" * 80)

    for name, success in results:
        status = "✓ PASSOU" if success else "✗ FALHOU"
        print(f"{status}: {name}")
