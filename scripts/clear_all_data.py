#!/usr/bin/env python3
"""
Script para limpar todos os dados de métricas e logs do sistema.

ATENÇÃO: Este script irá apagar TODOS os dados coletados!
- Trunca todas as tabelas do DuckDB
- Remove arquivos de log
- Mantém apenas a estrutura do banco

Uso:
    python scripts/clear_all_data.py
    python scripts/clear_all_data.py --confirm
"""
import os
import sys
import glob
from pathlib import Path

# Adicionar diretório raiz ao path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from sql_monitor.utils.metrics_store import MetricsStore


def clear_metrics_database(db_path: str = "logs/metrics.duckdb"):
    """
    Trunca todas as tabelas do DuckDB.

    Args:
        db_path: Caminho para o banco DuckDB
    """
    print("\n" + "=" * 80)
    print("LIMPANDO BANCO DE DADOS DE MÉTRICAS")
    print("=" * 80)

    if not os.path.exists(db_path):
        print(f"Banco de dados não encontrado: {db_path}")
        return

    try:
        store = MetricsStore(db_path=db_path)
        conn = store._get_connection()

        # Lista de tabelas para truncar
        tables = [
            'queries_collected',
            'query_metrics',
            'llm_analyses',
            'performance_alerts',
            'monitoring_cycles',
            'table_metadata'
        ]

        print(f"\nTruncando tabelas em: {db_path}")

        for table in tables:
            try:
                count_before = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                conn.execute(f"DELETE FROM {table}")
                print(f"  ✓ {table}: {count_before} registros removidos")
            except Exception as e:
                print(f"  ✗ Erro ao truncar {table}: {e}")

        # Vacuum para recuperar espaço
        print("\nExecutando VACUUM para recuperar espaço...")
        conn.execute("VACUUM")
        print("  ✓ VACUUM concluído")

        # Mostrar tamanho final
        size_mb = os.path.getsize(db_path) / (1024 * 1024)
        print(f"\nTamanho do banco após limpeza: {size_mb:.2f} MB")

    except Exception as e:
        print(f"\n✗ Erro ao limpar banco de dados: {e}")
        raise


def clear_log_files(log_dir: str = "logs"):
    """
    Remove arquivos de log.

    Args:
        log_dir: Diretório de logs
    """
    print("\n" + "=" * 80)
    print("LIMPANDO ARQUIVOS DE LOG")
    print("=" * 80)

    if not os.path.exists(log_dir):
        print(f"Diretório de logs não encontrado: {log_dir}")
        return

    # Padrões de arquivos para remover
    patterns = [
        "*.log",
        "*.log.*"
    ]

    files_removed = 0
    total_size = 0

    print(f"\nRemovendo arquivos em: {log_dir}")

    for pattern in patterns:
        for filepath in glob.glob(os.path.join(log_dir, pattern)):
            try:
                # Pular o banco de dados
                if filepath.endswith('.duckdb') or filepath.endswith('.duckdb.wal'):
                    continue

                size = os.path.getsize(filepath)
                os.remove(filepath)
                files_removed += 1
                total_size += size
                print(f"  ✓ Removido: {os.path.basename(filepath)} ({size / 1024:.2f} KB)")
            except Exception as e:
                print(f"  ✗ Erro ao remover {filepath}: {e}")

    if files_removed > 0:
        print(f"\nTotal: {files_removed} arquivos removidos ({total_size / (1024 * 1024):.2f} MB)")
    else:
        print("\nNenhum arquivo de log encontrado para remover")


def get_database_stats(db_path: str = "logs/metrics.duckdb"):
    """
    Mostra estatísticas atuais do banco de dados.

    Args:
        db_path: Caminho para o banco DuckDB
    """
    if not os.path.exists(db_path):
        return

    try:
        store = MetricsStore(db_path=db_path)
        conn = store._get_connection()

        print("\n" + "=" * 80)
        print("ESTATÍSTICAS DO BANCO DE DADOS")
        print("=" * 80)

        tables = [
            ('queries_collected', 'Queries Coletadas'),
            ('query_metrics', 'Métricas de Queries'),
            ('llm_analyses', 'Análises LLM'),
            ('performance_alerts', 'Alertas de Performance'),
            ('monitoring_cycles', 'Ciclos de Monitoramento'),
            ('table_metadata', 'Metadados de Tabelas')
        ]

        total_records = 0

        for table, label in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                total_records += count
                print(f"  {label:.<50} {count:>10,} registros")
            except Exception as e:
                print(f"  {label:.<50} ERRO: {e}")

        print(f"\n  {'Total de Registros':.<50} {total_records:>10,}")

        size_mb = os.path.getsize(db_path) / (1024 * 1024)
        print(f"  {'Tamanho do Banco':.<50} {size_mb:>10.2f} MB")

    except Exception as e:
        print(f"\nErro ao obter estatísticas: {e}")


def main():
    """Função principal."""
    print("\n" + "=" * 80)
    print("SCRIPT DE LIMPEZA COMPLETA DO SISTEMA")
    print("=" * 80)
    print("\nEste script irá:")
    print("  1. Truncar todas as tabelas do DuckDB")
    print("  2. Remover todos os arquivos de log")
    print("  3. Manter apenas a estrutura do banco")
    print("\n⚠️  ATENÇÃO: Esta operação NÃO PODE SER DESFEITA!")

    # Verificar argumento --confirm
    if "--confirm" not in sys.argv:
        print("\n" + "=" * 80)
        response = input("\nDeseja continuar? Digite 'SIM' para confirmar: ")

        if response.upper() != "SIM":
            print("\n✗ Operação cancelada pelo usuário")
            return
    else:
        print("\n✓ Confirmação automática via --confirm")

    try:
        # Mostrar estatísticas antes
        get_database_stats()

        # Limpar banco de dados
        clear_metrics_database()

        # Limpar logs
        clear_log_files()

        # Mostrar estatísticas depois
        get_database_stats()

        print("\n" + "=" * 80)
        print("✓ LIMPEZA CONCLUÍDA COM SUCESSO!")
        print("=" * 80)
        print("\nO sistema está pronto para coletar novos dados.")

    except Exception as e:
        print("\n" + "=" * 80)
        print("✗ ERRO DURANTE A LIMPEZA")
        print("=" * 80)
        print(f"\n{e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
