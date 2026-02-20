#!/bin/bash
# Script para limpar todos os dados de métricas e logs do sistema
#
# ATENÇÃO: Este script irá apagar TODOS os dados coletados!
# - Trunca todas as tabelas do DuckDB
# - Remove arquivos de log
# - Mantém apenas a estrutura do banco
#
# Uso:
#   bash scripts/clear_all_data.sh
#   bash scripts/clear_all_data.sh --confirm

set -e

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Diretórios
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
LOGS_DIR="$ROOT_DIR/logs"
DB_PATH="$LOGS_DIR/metrics.duckdb"

echo ""
echo "================================================================================"
echo "SCRIPT DE LIMPEZA COMPLETA DO SISTEMA"
echo "================================================================================"
echo ""
echo "Este script irá:"
echo "  1. Truncar todas as tabelas do DuckDB"
echo "  2. Remover todos os arquivos de log"
echo "  3. Manter apenas a estrutura do banco"
echo ""
echo -e "${RED}${BOLD}⚠️  ATENÇÃO: Esta operação NÃO PODE SER DESFEITA!${NC}"

# Verificar argumento --confirm
if [[ "$1" != "--confirm" ]]; then
    echo ""
    echo "================================================================================"
    read -p "Deseja continuar? Digite 'SIM' para confirmar: " response

    if [[ "$response" != "SIM" ]]; then
        echo ""
        echo -e "${RED}✗ Operação cancelada pelo usuário${NC}"
        exit 0
    fi
else
    echo ""
    echo -e "${GREEN}✓ Confirmação automática via --confirm${NC}"
fi

# Função para mostrar estatísticas do banco
show_db_stats() {
    local label=$1

    if [[ ! -f "$DB_PATH" ]]; then
        echo "Banco de dados não encontrado: $DB_PATH"
        return
    fi

    echo ""
    echo "================================================================================"
    echo "$label"
    echo "================================================================================"

    # Ativar venv se existir
    if [[ -f "$ROOT_DIR/.venv/bin/activate" ]]; then
        source "$ROOT_DIR/.venv/bin/activate"
    fi

    # Contar registros em cada tabela
    python3 << EOF
import duckdb
import os

db_path = "$DB_PATH"
conn = duckdb.connect(db_path)

tables = [
    ('queries_collected', 'Queries Coletadas'),
    ('query_metrics', 'Metricas de Queries'),
    ('llm_analyses', 'Analises LLM'),
    ('performance_alerts', 'Alertas de Performance'),
    ('monitoring_cycles', 'Ciclos de Monitoramento'),
    ('table_metadata', 'Metadados de Tabelas'),
    ('wait_stats_snapshots', 'Wait Stats Snapshots'),
    ('optimization_plans', 'Planos de Otimizacao'),
    ('optimization_items', 'Itens de Otimizacao'),
    ('optimization_executions', 'Execucoes de Otimizacao'),
    ('veto_records', 'Registros de Veto'),
]

total_records = 0

for table, label in tables:
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        total_records += count
        print(f"  {label:.<50} {count:>10,} registros")
    except Exception as e:
        print(f"  {label:.<50} ERRO")

print(f"\n  {'Total de Registros':.<50} {total_records:>10,}")

size_mb = os.path.getsize(db_path) / (1024 * 1024)
print(f"  {'Tamanho do Banco':.<50} {size_mb:>10.2f} MB")

conn.close()
EOF
}

# Limpar banco de dados
clear_database() {
    echo ""
    echo "================================================================================"
    echo "LIMPANDO BANCO DE DADOS DE MÉTRICAS"
    echo "================================================================================"

    if [[ ! -f "$DB_PATH" ]]; then
        echo "Banco de dados não encontrado: $DB_PATH"
        return
    fi

    # Ativar venv se existir
    if [[ -f "$ROOT_DIR/.venv/bin/activate" ]]; then
        source "$ROOT_DIR/.venv/bin/activate"
    fi

    echo ""
    echo "Truncando tabelas em: $DB_PATH"

    python3 << EOF
import duckdb

db_path = "$DB_PATH"
conn = duckdb.connect(db_path)

tables = [
    'queries_collected',
    'query_metrics',
    'llm_analyses',
    'performance_alerts',
    'monitoring_cycles',
    'table_metadata',
    'wait_stats_snapshots',
    'optimization_items',
    'optimization_plans',
    'optimization_executions',
    'veto_records',
]

for table in tables:
    try:
        count_before = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        conn.execute(f"DELETE FROM {table}")
        print(f"  ✓ {table}: {count_before} registros removidos")
    except Exception as e:
        print(f"  ✗ Erro ao truncar {table}: {e}")

print("\nExecutando VACUUM para recuperar espaço...")
conn.execute("VACUUM")
print("  ✓ VACUUM concluído")

conn.close()
EOF
}

# Limpar arquivos de log
clear_logs() {
    echo ""
    echo "================================================================================"
    echo "LIMPANDO ARQUIVOS DE LOG"
    echo "================================================================================"

    if [[ ! -d "$LOGS_DIR" ]]; then
        echo "Diretório de logs não encontrado: $LOGS_DIR"
        return
    fi

    echo ""
    echo "Removendo arquivos em: $LOGS_DIR"

    files_removed=0

    # Remover arquivos .log
    for file in "$LOGS_DIR"/*.log; do
        if [[ -f "$file" ]]; then
            size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null)
            rm -f "$file"
            echo "  ✓ Removido: $(basename "$file") ($((size / 1024)) KB)"
            ((files_removed++))
        fi
    done

    # Remover arquivos .log.*
    for file in "$LOGS_DIR"/*.log.*; do
        if [[ -f "$file" && ! "$file" =~ \.duckdb ]]; then
            size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null)
            rm -f "$file"
            echo "  ✓ Removido: $(basename "$file") ($((size / 1024)) KB)"
            ((files_removed++))
        fi
    done

    if [[ $files_removed -eq 0 ]]; then
        echo ""
        echo "Nenhum arquivo de log encontrado para remover"
    else
        echo ""
        echo "Total: $files_removed arquivos removidos"
    fi
}

# Executar limpeza
show_db_stats "ESTATÍSTICAS ANTES DA LIMPEZA"

clear_database

clear_logs

show_db_stats "ESTATÍSTICAS APÓS LIMPEZA"

echo ""
echo "================================================================================"
echo -e "${GREEN}${BOLD}✓ LIMPEZA CONCLUÍDA COM SUCESSO!${NC}"
echo "================================================================================"
echo ""
echo "O sistema está pronto para coletar novos dados."
echo ""
