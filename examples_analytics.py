#!/usr/bin/env python3
"""
Exemplos de uso da API de Analytics com DuckDB.

Este script demonstra como usar QueryAnalytics para:
- Gerar relatórios executivos
- Analise de tendências
- Identificar problemas críticos
- Monitorar saúde do sistema
"""
import json
from datetime import datetime
from sql_monitor.utils.metrics_store import MetricsStore
from sql_monitor.utils.query_analytics import QueryAnalytics


def print_section(title: str):
    """Imprime título de seção formatado."""
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def main():
    # Inicializar MetricsStore e Analytics
    metrics_store = MetricsStore(db_path="logs/metrics.duckdb")
    analytics = QueryAnalytics(metrics_store)

    # ========== RESUMO EXECUTIVO ==========
    print_section("📊 RESUMO EXECUTIVO - ÚLTIMAS 24 HORAS")

    summary = analytics.get_executive_summary(hours=24)

    print(f"Período: Últimas {summary['period_hours']} horas")
    print(f"Queries únicas detectadas: {summary['unique_queries']}")
    print(f"Ocorrências totais: {summary['total_occurrences']}")
    print(f"Análises LLM realizadas: {summary['analyses_performed']}")
    print(f"Duração média de análise: {summary['avg_analysis_duration_ms']:.2f}ms")
    print(f"Tokens LLM consumidos: {summary['total_llm_tokens']}")

    print(f"\n🚨 ALERTAS:")
    print(f"  • Critical: {summary['alerts']['critical']}")
    print(f"  • High: {summary['alerts']['high']}")
    print(f"  • Medium: {summary['alerts']['medium']}")
    print(f"  • Low: {summary['alerts']['low']}")
    print(f"  • TOTAL: {summary['alerts']['total']}")

    print(f"\n🔥 TOP 5 INSTÂNCIAS COM MAIS PROBLEMAS:")
    for idx, inst in enumerate(summary['top_instances'], 1):
        print(f"  {idx}. {inst['instance']}: {inst['problem_count']} queries problemáticas")

    # ========== TENDÊNCIAS DE PERFORMANCE ==========
    print_section("📈 TENDÊNCIAS DE PERFORMANCE - ÚLTIMOS 7 DIAS")

    trends = analytics.get_performance_trends(days=7, granularity='day')

    if trends:
        print(f"{'Data':<12} {'Queries':<8} {'Avg CPU (ms)':<15} {'Avg Duration (ms)':<18} {'Max CPU (ms)':<12}")
        print("-" * 80)
        for trend in trends[-7:]:  # Últimos 7 dias
            print(f"{trend['time_bucket']:<12} "
                  f"{trend['total_queries']:<8} "
                  f"{trend['avg_cpu_ms']:<15.2f} "
                  f"{trend['avg_duration_ms']:<18.2f} "
                  f"{trend['max_cpu_ms']:<12.2f}")
    else:
        print("Nenhum dado disponível para o período.")

    # ========== PIORES PERFORMERS ==========
    print_section("🐌 TOP 10 QUERIES MAIS LENTAS (CPU TIME)")

    worst_cpu = analytics.get_worst_performers(metric='cpu_time_ms', hours=24, limit=10)

    if worst_cpu:
        for idx, query in enumerate(worst_cpu, 1):
            print(f"\n{idx}. [{query['severity'] or 'N/A'}] {query['instance_name']}")
            print(f"   Tabela: {query['database_name']}.{query['table_name']}")
            print(f"   Avg CPU: {query['avg_cpu_time_ms']:.2f}ms | Max CPU: {query['max_cpu_time_ms']:.2f}ms")
            print(f"   Ocorrências: {query['occurrences']}")
            print(f"   Query: {query['query_preview'][:80]}...")
            if query['has_analysis']:
                print(f"   ✓ Análise LLM disponível")
                if query['recommendations_preview']:
                    print(f"   Recomendações: {query['recommendations_preview']}")
    else:
        print("Nenhuma query problemática detectada.")

    # ========== HOTSPOTS DE ALERTAS ==========
    print_section("🔥 HOTSPOTS DE ALERTAS - TABELAS COM MAIS PROBLEMAS")

    hotspots = analytics.get_alert_hotspots(hours=24, min_alerts=3)

    if hotspots:
        for idx, hotspot in enumerate(hotspots, 1):
            print(f"\n{idx}. {hotspot['instance_name']}.{hotspot['database_name']}.{hotspot['table_name']}")
            print(f"   Alertas: {hotspot['alert_count']}")
            print(f"   Queries afetadas: {hotspot['affected_queries']}")
            print(f"   Tipos de alerta: {hotspot['alert_types']}")
            print(f"   Último alerta: {hotspot['last_alert']}")
    else:
        print("Nenhum hotspot detectado (bom sinal!).")

    # ========== EFICIÊNCIA DO CACHE ==========
    print_section("💰 EFICIÊNCIA DO CACHE E ROI")

    cache_stats = analytics.get_cache_efficiency(hours=24)

    print(f"Período: Últimas {cache_stats['period_hours']} horas")
    print(f"Queries encontradas: {cache_stats['total_queries']}")
    print(f"Novas análises: {cache_stats['new_analyses']}")
    print(f"Cache hits: {cache_stats['cache_hits']}")
    print(f"Taxa de acerto: {cache_stats['cache_hit_rate_percent']:.2f}%")
    print(f"\n💵 ECONOMIA ESTIMADA:")
    print(f"  Tokens economizados: {cache_stats['estimated_tokens_saved']:,}")
    print(f"  Custo economizado: ${cache_stats['estimated_cost_saved_usd']:.4f} USD")
    print(f"\nDuração média de ciclo: {cache_stats['avg_cycle_duration_ms']:.2f}ms")

    # ========== SAÚDE DO MONITORAMENTO ==========
    print_section("🏥 SAÚDE DO SISTEMA DE MONITORAMENTO")

    health = analytics.get_monitoring_health(hours=24)

    print(f"Período: Últimas {health['period_hours']} horas")
    print(f"Ciclos executados: {health['total_cycles']}")
    print(f"  ✓ Sucesso: {health['successful_cycles']}")
    print(f"  ✗ Falhas: {health['failed_cycles']}")
    print(f"Taxa de sucesso: {health['success_rate_percent']:.2f}%")
    print(f"Duração média de ciclo: {health['avg_cycle_duration_ms']:.2f}ms")
    print(f"Duração máxima de ciclo: {health['max_cycle_duration_ms']:.2f}ms")
    print(f"Primeiro ciclo: {health['first_cycle']}")
    print(f"Último ciclo: {health['last_cycle']}")

    print(f"\n📍 INSTÂNCIAS ATIVAS ({len(health['active_instances'])}):")
    for inst in health['active_instances']:
        print(f"  • {inst['name']} ({inst['type']})")

    if health['instances_with_errors']:
        print(f"\n⚠️  INSTÂNCIAS COM ERROS:")
        for err in health['instances_with_errors']:
            print(f"  • {err['instance']}: {err['total_errors']} erros "
                  f"({err['error_rate_percent']:.2f}% error rate)")

    # ========== RECOMENDAÇÕES PRIORITÁRIAS ==========
    print_section("🎯 RECOMENDAÇÕES PRIORITÁRIAS")

    recommendations = analytics.get_recommendation_summary(hours=24, severity='high')

    if recommendations:
        for idx, rec in enumerate(recommendations[:5], 1):  # Top 5
            print(f"\n{idx}. [{rec['severity']}] {rec['instance_name']} - {rec['table_name']}")
            print(f"   Analisado em: {rec['analyzed_at']}")
            if rec['analysis_preview']:
                print(f"   Análise: {rec['analysis_preview']}")
            if rec['recommendations']:
                print(f"   ➤ {rec['recommendations']}")
    else:
        print("Nenhuma recomendação de alta severidade (excelente!).")

    # ========== ANÁLISE DE TABELA ESPECÍFICA (EXEMPLO) ==========
    # Descomentar e ajustar para analisar uma tabela específica
    """
    print_section("🔍 ANÁLISE DETALHADA DE TABELA")

    table_history = analytics.get_table_analysis_history(
        database_name='MyDatabase',
        schema_name='dbo',
        table_name='Orders',
        days=7
    )

    print(f"Tabela: {table_history['database']}.{table_history['schema']}.{table_history['table']}")
    print(f"Período: Últimos {table_history['period_days']} dias")
    print(f"\nQueries problemáticas: {len(table_history['problem_queries'])}")
    print(f"Alertas gerados: {len(table_history['alerts'])}")

    if table_history['latest_metadata']:
        meta = table_history['latest_metadata']
        if meta['row_count']:
            print(f"\nÚltimos metadados:")
            print(f"  Linhas: {meta['row_count']:,}")
            print(f"  Tamanho: {meta['total_size_mb']:.2f} MB")
    """

    # ========== EXPORTAR DADOS PARA ANÁLISE EXTERNA ==========
    print_section("💾 EXPORTAR DADOS")

    print("Os dados podem ser exportados para Parquet para análise em:")
    print("  • Python/Pandas")
    print("  • Power BI")
    print("  • Tableau")
    print("  • Apache Spark")
    print("\nExemplo:")
    print("  metrics_store.export_to_parquet('queries_collected', 'export_queries.parquet', hours=24)")
    print("  metrics_store.export_to_parquet('query_metrics', 'export_metrics.parquet', hours=24)")

    # ========== CONECTAR AO DUCKDB DIRETAMENTE ==========
    print_section("🦆 QUERIES CUSTOMIZADAS")

    print("Você também pode executar queries SQL customizadas diretamente:")
    print("\nExemplo:")
    print("""
    conn = metrics_store._get_connection()
    result = conn.execute('''
        SELECT database_name, table_name, COUNT(*) as problems
        FROM queries_collected
        WHERE collected_at >= NOW() - INTERVAL '24 hours'
        GROUP BY database_name, table_name
        ORDER BY problems DESC
        LIMIT 10
    ''').fetchall()
    """)

    print("\n" + "=" * 80)
    print("✅ ANÁLISE COMPLETA!")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Erro ao executar análise: {e}")
        import traceback
        traceback.print_exc()
