#!/usr/bin/env python3
"""
Script de teste para validar as correções do dashboard.
"""
import sys
import os

# Adicionar diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_query_analytics_methods():
    """Testa os métodos corrigidos do QueryAnalytics."""
    print("=" * 80)
    print("TESTE: Validar Correções do Dashboard")
    print("=" * 80)

    try:
        from sql_monitor.utils.metrics_store import MetricsStore
        from sql_monitor.utils.query_analytics import QueryAnalytics

        # Criar instâncias
        print("\n1. Criando MetricsStore e QueryAnalytics...")
        store = MetricsStore(db_path="logs/metrics.duckdb")
        analytics = QueryAnalytics(store)
        print("   ✓ Instâncias criadas com sucesso")

        # Teste 1: get_worst_performers deve retornar todos os campos
        print("\n2. Testando get_worst_performers()...")
        result = analytics.get_worst_performers(metric='cpu_time_ms', hours=24, limit=5)
        queries = result['queries']

        if queries:
            first_query = queries[0]
            required_fields = [
                'query_hash', 'instance_name', 'database_name', 'table_name',
                'query_preview', 'avg_cpu_time_ms', 'avg_duration_ms',
                'avg_logical_reads', 'max_cpu_time_ms', 'max_duration_ms',
                'occurrences', 'severity', 'has_analysis'
            ]

            missing_fields = [f for f in required_fields if f not in first_query]

            if missing_fields:
                print(f"   ✗ Campos faltando: {missing_fields}")
                assert False, f"Campos faltando em worst_performers: {missing_fields}"
            else:
                print(f"   ✓ Todos os campos necessários estão presentes")
                print(f"   ✓ Retornou {len(queries)} queries")
        else:
            print("   ⚠️  Nenhuma query encontrada (banco pode estar vazio)")

        # Teste 2: get_alert_hotspots deve retornar contagens por severidade
        print("\n3. Testando get_alert_hotspots()...")
        hotspots = analytics.get_alert_hotspots(hours=24, min_alerts=1)

        if hotspots:
            first_hotspot = hotspots[0]
            required_fields = [
                'instance_name', 'database_name', 'table_name',
                'total_alerts', 'critical_count', 'high_count',
                'medium_count', 'low_count', 'affected_queries',
                'last_alert', 'alert_types'
            ]

            missing_fields = [f for f in required_fields if f not in first_hotspot]

            if missing_fields:
                print(f"   ✗ Campos faltando: {missing_fields}")
                assert False, f"Campos faltando em alert_hotspots: {missing_fields}"
            else:
                print(f"   ✓ Todos os campos necessários estão presentes")
                print(f"   ✓ Retornou {len(hotspots)} hotspots")
        else:
            print("   ⚠️  Nenhum hotspot encontrado (pode não haver alertas suficientes)")

        # Teste 3: get_monitoring_health deve retornar estatísticas de instâncias
        print("\n4. Testando get_monitoring_health()...")
        health = analytics.get_monitoring_health(hours=24)

        if health['active_instances']:
            first_instance = health['active_instances'][0]
            required_fields = [
                'name', 'type', 'queries_found', 'queries_analyzed',
                'cache_hits', 'errors', 'total_cycles', 'avg_cycle_duration_ms'
            ]

            missing_fields = [f for f in required_fields if f not in first_instance]

            if missing_fields:
                print(f"   ✗ Campos faltando: {missing_fields}")
                assert False, f"Campos faltando em active_instances: {missing_fields}"
            else:
                print(f"   ✓ Todos os campos necessários estão presentes")
                print(f"   ✓ Instâncias ativas: {len(health['active_instances'])}")
        else:
            print("   ⚠️  Nenhuma instância ativa encontrada")

        # Teste 4: Verificar estrutura do executive_summary
        print("\n5. Testando get_executive_summary()...")
        summary = analytics.get_executive_summary(hours=24)

        required_fields = [
            'period_hours', 'total_db_time_ms', 'avg_latency_ms', 'total_occurrences',
            'analyses_performed', 'total_llm_tokens', 'alerts'
        ]

        missing_fields = [f for f in required_fields if f not in summary]

        if missing_fields:
            print(f"   ✗ Campos faltando: {missing_fields}")
            assert False, f"Campos faltando em executive_summary: {missing_fields}"
        else:
            print(f"   ✓ Todos os campos necessários estão presentes")
            print(f"   ✓ DB Time Total: {summary['total_db_time_ms']:.0f} ms")
            print(f"   ✓ Total de alertas: {summary['alerts']['total']}")

        print("\n✅ Todos os testes passaram!")
        print("\nResumo das correções aplicadas:")
        print("1. get_worst_performers() - Agora retorna todas as métricas (cpu, duration, reads, etc.)")
        print("2. get_alert_hotspots() - Agora retorna contagens por severidade")
        print("3. get_monitoring_health() - Agora inclui estatísticas detalhadas das instâncias")

    except Exception as e:
        print(f"\n✗ Erro durante os testes: {e}")
        import traceback
        traceback.print_exc()
        assert False, f"Erro durante os testes: {e}"


if __name__ == '__main__':
    print("\n🚀 Iniciando testes de validação das correções do dashboard...\n")
    try:
        test_query_analytics_methods()
        print("\n" + "=" * 80)
        print("✅ SUCESSO: Todas as correções foram validadas")
        print("=" * 80)
        print("\nO dashboard deve funcionar corretamente agora.")
        print("Para iniciar a API e visualizar o dashboard:")
        print("  1. Configure weekly_optimizer.api.enabled = true no config.json")
        print("  2. Execute: python main.py")
        print("  3. Acesse: http://localhost:8080")
        sys.exit(0)
    except AssertionError:
        print("\n" + "=" * 80)
        print("✗ FALHA: Alguns testes falharam")
        print("=" * 80)
        sys.exit(1)
