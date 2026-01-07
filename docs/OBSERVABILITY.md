# Sistema de Observabilidade e Métricas

**Data**: 2025-12-29
**Versão**: 1.0
**Status**: ✅ Implementado

---

## 📋 Índice

1. [Visão Geral](#visão-geral)
2. [Arquitetura](#arquitetura)
3. [Instalação e Configuração](#instalação-e-configuração)
4. [Schema do Banco de Dados](#schema-do-banco-de-dados)
5. [API de Analytics](#api-de-analytics)
6. [Exemplos de Uso](#exemplos-de-uso)
7. [Queries Customizadas](#queries-customizadas)
8. [Export e Integração](#export-e-integração)
9. [Performance e Otimização](#performance-e-otimização)
10. [FAQ](#faq)

---

## 📊 Visão Geral

### O Problema

O sistema anterior usava:
- ✗ Logs TXT dispersos e difíceis de analisar
- ✗ Cache JSON sem análise temporal
- ✗ Sem histórico de métricas
- ✗ Impossível identificar tendências
- ✗ Nenhuma capacidade analítica

### A Solução

Implementamos um **sistema completo de observabilidade** usando **DuckDB**:

✅ **Banco de dados analítico embarcado** (OLAP otimizado)
✅ **Histórico completo** de queries e métricas
✅ **Análises temporais** e identificação de tendências
✅ **Queries analíticas rápidas** (sub-segundo)
✅ **API de alto nível** para dashboards
✅ **Export para ferramentas** (Grafana, Power BI, Tableau)
✅ **ROI de otimizações** (economia de tokens LLM)
✅ **Thread-safe** e performático

---

## 🏗️ Arquitetura

### Componentes Principais

```
┌─────────────────────────────────────────────────────────────────┐
│                    DatabaseMonitor (N instâncias)               │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  1. Coleta queries (active, expensive, table scans)      │  │
│  │  2. Extrai métricas (CPU, duration, reads, writes)       │  │
│  │  3. Analisa com LLM                                      │  │
│  │  4. Persiste tudo no MetricsStore ↓                      │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                     MetricsStore (DuckDB)                        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  • queries_collected     (histórico de queries)          │  │
│  │  • query_metrics        (CPU, duration, reads, etc.)     │  │
│  │  • llm_analyses         (análises LLM com TTL)           │  │
│  │  • monitoring_cycles    (estatísticas de execuções)      │  │
│  │  • performance_alerts   (alertas de threshold)           │  │
│  │  • table_metadata       (DDL, índices, estatísticas)     │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                     QueryAnalytics (API)                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  • get_executive_summary()       (dashboards)            │  │
│  │  • get_performance_trends()      (gráficos temporais)    │  │
│  │  • get_worst_performers()        (top N problemáticas)   │  │
│  │  • get_alert_hotspots()          (tabelas críticas)      │  │
│  │  • get_cache_efficiency()        (ROI de cache)          │  │
│  │  • get_monitoring_health()       (saúde do sistema)      │  │
│  │  • ... e mais 12 métodos analíticos                      │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                  Dashboards e Ferramentas                        │
│  • Python scripts (examples_analytics.py)                       │
│  • Jupyter Notebooks                                            │
│  • Power BI / Tableau (via Parquet export)                      │
│  • Grafana / Prometheus (via export futuro)                     │
│  • SQL direto no DuckDB                                         │
└─────────────────────────────────────────────────────────────────┘
```

### Fluxo de Dados

1. **Coleta**: DatabaseMonitor detecta query problemática
2. **Extração**: Métricas de performance são extraídas
3. **Verificação**: Verifica cache LLM no MetricsStore (TTL-based)
4. **Análise**: Se necessário, analisa com LLM
5. **Persistência**: Tudo é salvo no DuckDB:
   - Query coletada
   - Métricas de performance
   - Análise LLM (se nova)
   - Alertas (se threshold violado)
6. **Analytics**: API disponibiliza queries prontas para análise

---

## 🚀 Instalação e Configuração

### 1. Instalar Dependências

```bash
# DuckDB já está no requirements.txt
pip install duckdb>=0.9.0
```

### 2. Configuração (config.json)

```json
{
  "metrics_store": {
    "db_path": "logs/metrics.duckdb",
    "enable_compression": true,
    "retention_days": 30
  }
}
```

### 3. Inicialização Automática

O MetricsStore é inicializado automaticamente pelo `MultiDatabaseMonitor`:

```python
# Não é necessário fazer nada!
# O monitor já cria e usa o MetricsStore automaticamente
```

### 4. Verificar Funcionamento

Execute o script de exemplo:

```bash
python examples_analytics.py
```

---

## 🗄️ Schema do Banco de Dados

### 1. `queries_collected` - Histórico de Queries

Armazena todas as queries problemáticas detectadas.

```sql
CREATE TABLE queries_collected (
    id INTEGER PRIMARY KEY,
    query_hash VARCHAR(64) NOT NULL,          -- SHA256 da query sanitizada
    collected_at TIMESTAMP NOT NULL,          -- Quando foi detectada
    instance_name VARCHAR(100) NOT NULL,      -- Instância do banco
    db_type VARCHAR(20) NOT NULL,             -- sqlserver, postgresql, hana
    database_name VARCHAR(100),
    schema_name VARCHAR(100),
    table_name VARCHAR(100),
    query_text TEXT,                          -- Query original completa
    sanitized_query TEXT,                     -- Query sanitizada
    query_preview VARCHAR(200),               -- Primeiros 200 chars
    query_type VARCHAR(50),                   -- active, expensive, table_scan

    INDEX idx_query_hash (query_hash),
    INDEX idx_collected_at (collected_at),
    INDEX idx_instance (instance_name)
);
```

**Uso típico**: Análise histórica de queries problemáticas

### 2. `query_metrics` - Métricas de Performance

Armazena métricas detalhadas de performance.

```sql
CREATE TABLE query_metrics (
    id INTEGER PRIMARY KEY,
    query_hash VARCHAR(64) NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    instance_name VARCHAR(100) NOT NULL,

    -- Métricas de execução
    cpu_time_ms DOUBLE,                       -- Tempo de CPU (ms)
    duration_ms DOUBLE,                       -- Duração total (ms)
    logical_reads BIGINT,                     -- Leituras lógicas
    physical_reads BIGINT,                    -- Leituras físicas
    writes BIGINT,                            -- Escritas
    row_count BIGINT,                         -- Linhas retornadas

    -- Recursos
    memory_mb DOUBLE,                         -- Memória usada (MB)
    wait_time_ms DOUBLE,                      -- Tempo de espera (ms)
    blocking_session_id INTEGER,              -- Sessão bloqueando

    -- Status
    status VARCHAR(50),                       -- running, suspended, etc.
    wait_type VARCHAR(100),                   -- Tipo de wait
    execution_count INTEGER,                  -- Vezes executada

    INDEX idx_query_hash_metrics (query_hash),
    INDEX idx_cpu_time (cpu_time_ms),
    INDEX idx_duration (duration_ms)
);
```

**Uso típico**: Identificar queries que consomem mais recursos

### 3. `llm_analyses` - Análises LLM

Armazena resultados de análises LLM com TTL (cache).

```sql
CREATE TABLE llm_analyses (
    id INTEGER PRIMARY KEY,
    query_hash VARCHAR(64) NOT NULL UNIQUE,   -- Uma análise por query
    analyzed_at TIMESTAMP NOT NULL,           -- Quando foi analisada
    instance_name VARCHAR(100) NOT NULL,

    -- Contexto
    database_name VARCHAR(100),
    schema_name VARCHAR(100),
    table_name VARCHAR(100),

    -- Resultado da análise
    analysis_text TEXT,                       -- Análise completa do LLM
    recommendations TEXT,                     -- Recomendações
    severity VARCHAR(20),                     -- low, medium, high, critical
    estimated_impact VARCHAR(50),             -- Impacto estimado

    -- Metadados LLM
    model_used VARCHAR(50),                   -- gemini-flash-latest, etc.
    tokens_used INTEGER,                      -- Tokens consumidos
    analysis_duration_ms DOUBLE,              -- Duração da análise

    -- TTL e cache
    expires_at TIMESTAMP,                     -- Quando expira o cache
    last_seen TIMESTAMP,                      -- Última vez vista
    seen_count INTEGER DEFAULT 1,             -- Vezes detectada

    INDEX idx_query_hash_llm (query_hash),
    INDEX idx_expires_at (expires_at),
    INDEX idx_severity (severity)
);
```

**Uso típico**: Cache de análises LLM, economia de custos

### 4. `monitoring_cycles` - Ciclos de Monitoramento

Estatísticas de cada execução do monitor.

```sql
CREATE TABLE monitoring_cycles (
    id INTEGER PRIMARY KEY,
    cycle_started_at TIMESTAMP NOT NULL,
    cycle_ended_at TIMESTAMP,
    instance_name VARCHAR(100) NOT NULL,
    db_type VARCHAR(20) NOT NULL,

    -- Estatísticas
    queries_found INTEGER DEFAULT 0,          -- Queries detectadas
    queries_analyzed INTEGER DEFAULT 0,       -- Analisadas pelo LLM
    cache_hits INTEGER DEFAULT 0,             -- Cache hits
    errors INTEGER DEFAULT 0,                 -- Erros ocorridos

    -- Performance
    cycle_duration_ms DOUBLE,                 -- Duração do ciclo
    collection_duration_ms DOUBLE,            -- Tempo de coleta
    analysis_duration_ms DOUBLE,              -- Tempo de análise

    -- Status
    status VARCHAR(20),                       -- running, completed, failed
    error_message TEXT,

    INDEX idx_cycle_started (cycle_started_at),
    INDEX idx_instance_cycles (instance_name)
);
```

**Uso típico**: Monitorar saúde e performance do próprio monitor

### 5. `performance_alerts` - Alertas de Threshold

Registra violações de thresholds configurados.

```sql
CREATE TABLE performance_alerts (
    id INTEGER PRIMARY KEY,
    alert_time TIMESTAMP NOT NULL,
    instance_name VARCHAR(100) NOT NULL,
    query_hash VARCHAR(64) NOT NULL,

    -- Tipo de alerta
    alert_type VARCHAR(50),                   -- cpu_threshold, duration_threshold
    severity VARCHAR(20),                     -- low, medium, high, critical

    -- Valores
    threshold_value DOUBLE,                   -- Threshold configurado
    actual_value DOUBLE,                      -- Valor real medido

    -- Contexto
    database_name VARCHAR(100),
    table_name VARCHAR(100),
    query_preview VARCHAR(200),

    -- Notificação
    teams_notified BOOLEAN DEFAULT FALSE,     -- Se Teams foi notificado

    INDEX idx_alert_time (alert_time),
    INDEX idx_severity_alerts (severity)
);
```

**Uso típico**: Identificar problemas críticos que precisam ação imediata

### 6. `table_metadata` - Metadados de Tabelas

Snapshots de metadados de tabelas (DDL, índices, estatísticas).

```sql
CREATE TABLE table_metadata (
    id INTEGER PRIMARY KEY,
    captured_at TIMESTAMP NOT NULL,
    instance_name VARCHAR(100) NOT NULL,
    database_name VARCHAR(100),
    schema_name VARCHAR(100),
    table_name VARCHAR(100) NOT NULL,

    -- DDL e estrutura
    column_count INTEGER,
    columns_json TEXT,                        -- JSON com colunas

    -- Índices
    indexes_json TEXT,                        -- JSON com índices existentes
    missing_indexes_json TEXT,                -- Sugestões de índices

    -- Estatísticas
    row_count BIGINT,
    total_size_mb DOUBLE,
    index_size_mb DOUBLE,
    data_size_mb DOUBLE,

    INDEX idx_table_name_meta (table_name)
);
```

**Uso típico**: Análise histórica de crescimento de tabelas

---

## 🔧 API de Analytics

### QueryAnalytics - API de Alto Nível

A classe `QueryAnalytics` fornece métodos prontos para análises comuns.

#### Inicialização

```python
from sql_monitor.utils.metrics_store import MetricsStore
from sql_monitor.utils.query_analytics import QueryAnalytics

# Inicializar
metrics_store = MetricsStore(db_path="logs/metrics.duckdb")
analytics = QueryAnalytics(metrics_store)
```

### Métodos Disponíveis

#### 1. **get_executive_summary()** - Dashboard Executivo

Resumo de alto nível para gestores.

```python
summary = analytics.get_executive_summary(hours=24)

# Retorna:
{
    'period_hours': 24,
    'unique_queries': 150,
    'total_occurrences': 450,
    'analyses_performed': 50,
    'avg_analysis_duration_ms': 1200.5,
    'total_llm_tokens': 25000,
    'alerts': {
        'critical': 5,
        'high': 12,
        'medium': 28,
        'low': 10,
        'total': 55
    },
    'top_instances': [
        {'instance': 'SQL-PROD-01', 'problem_count': 120},
        {'instance': 'PG-DEV-02', 'problem_count': 85}
    ]
}
```

#### 2. **get_performance_trends()** - Tendências Temporais

Análise de tendências ao longo do tempo (ideal para gráficos).

```python
trends = analytics.get_performance_trends(
    days=7,
    granularity='day'  # 'hour', 'day', 'week'
)

# Retorna lista de pontos temporais
[
    {
        'time_bucket': '2025-12-22',
        'unique_queries': 45,
        'total_queries': 130,
        'avg_cpu_ms': 2500.3,
        'avg_duration_ms': 3200.1,
        'max_cpu_ms': 15000.0
    },
    ...
]
```

#### 3. **get_worst_performers()** - Top N Problemáticas

Identifica queries com pior performance.

```python
worst = analytics.get_worst_performers(
    metric='cpu_time_ms',  # ou 'duration_ms', 'logical_reads'
    hours=24,
    limit=10
)

# Retorna top 10 queries
[
    {
        'query_hash': 'a1b2c3...',
        'instance_name': 'SQL-PROD-01',
        'database_name': 'ERP',
        'table_name': 'Orders',
        'query_preview': 'SELECT * FROM Orders WHERE...',
        'avg_cpu_time_ms': 25000.0,
        'max_cpu_time_ms': 45000.0,
        'occurrences': 15,
        'severity': 'high',
        'has_analysis': True
    },
    ...
]
```

#### 4. **get_alert_hotspots()** - Pontos Críticos

Identifica tabelas/queries que geram mais alertas.

```python
hotspots = analytics.get_alert_hotspots(
    hours=24,
    min_alerts=3  # Mínimo de alertas para considerar
)

# Retorna hotspots críticos
[
    {
        'instance_name': 'SQL-PROD-01',
        'database_name': 'ERP',
        'table_name': 'Orders',
        'alert_count': 25,
        'affected_queries': 8,
        'last_alert': '2025-12-29T14:30:00',
        'alert_types': 'cpu_threshold, duration_threshold'
    },
    ...
]
```

#### 5. **get_cache_efficiency()** - ROI do Cache

Analisa eficiência do cache LLM e calcula economia.

```python
cache_stats = analytics.get_cache_efficiency(hours=24)

# Retorna métricas de ROI
{
    'period_hours': 24,
    'total_queries': 500,
    'new_analyses': 50,
    'cache_hits': 450,
    'cache_hit_rate_percent': 90.0,
    'avg_cycle_duration_ms': 2500.0,
    'estimated_tokens_saved': 225000,
    'estimated_cost_saved_usd': 0.0338  # ~3.4 centavos economizados
}
```

#### 6. **get_monitoring_health()** - Saúde do Sistema

Monitora a saúde do próprio monitor.

```python
health = analytics.get_monitoring_health(hours=24)

# Retorna métricas de saúde
{
    'period_hours': 24,
    'total_cycles': 48,
    'successful_cycles': 47,
    'failed_cycles': 1,
    'success_rate_percent': 97.92,
    'avg_cycle_duration_ms': 3200.5,
    'max_cycle_duration_ms': 8500.0,
    'active_instances': [
        {'name': 'SQL-PROD-01', 'type': 'sqlserver'},
        {'name': 'PG-DEV-02', 'type': 'postgresql'}
    ],
    'instances_with_errors': [
        {
            'instance': 'HANA-TEST',
            'total_errors': 5,
            'total_queries': 100,
            'error_rate_percent': 5.0
        }
    ]
}
```

### Métodos Adicionais

- `get_table_analysis_history()` - Histórico detalhado de uma tabela
- `get_recommendation_summary()` - Recomendações por prioridade
- Veja `sql_monitor/utils/query_analytics.py` para lista completa

---

## 💡 Exemplos de Uso

### Exemplo 1: Dashboard Simples em Terminal

```python
#!/usr/bin/env python3
from sql_monitor.utils.metrics_store import MetricsStore
from sql_monitor.utils.query_analytics import QueryAnalytics

# Inicializar
store = MetricsStore()
analytics = QueryAnalytics(store)

# Dashboard executivo
print("=" * 80)
print("DASHBOARD EXECUTIVO")
print("=" * 80)

summary = analytics.get_executive_summary(hours=24)
print(f"\nQueries detectadas: {summary['unique_queries']}")
print(f"Alertas críticos: {summary['alerts']['critical']}")
print(f"Custo LLM: ${summary['total_llm_tokens'] * 0.15 / 1_000_000:.4f}")

# Top 5 problemas
print("\nTOP 5 QUERIES MAIS LENTAS:")
worst = analytics.get_worst_performers(metric='duration_ms', limit=5)
for idx, q in enumerate(worst, 1):
    print(f"{idx}. {q['instance_name']}.{q['table_name']} - {q['avg_duration_ms']:.0f}ms")
```

### Exemplo 2: Exportar para Análise Externa

```python
# Exportar para Parquet (Power BI, Tableau, Spark)
store.export_to_parquet(
    table_name='queries_collected',
    output_path='exports/queries_last_week.parquet',
    hours=168  # 7 dias
)

# Importar no pandas
import pandas as pd
df = pd.read_parquet('exports/queries_last_week.parquet')
print(df.head())
```

### Exemplo 3: Monitoramento Contínuo

```python
import time

while True:
    health = analytics.get_monitoring_health(hours=1)

    if health['success_rate_percent'] < 95:
        print(f"⚠️  ALERTA: Taxa de sucesso baixa: {health['success_rate_percent']}%")

    if health['instances_with_errors']:
        for err in health['instances_with_errors']:
            if err['error_rate_percent'] > 10:
                print(f"🚨 {err['instance']}: {err['error_rate_percent']}% de erro!")

    time.sleep(300)  # Checar a cada 5 minutos
```

---

## 🔍 Queries Customizadas

Você pode executar SQL diretamente no DuckDB para análises específicas.

### Exemplo 1: Queries que Mais Cresceram em CPU

```python
conn = store._get_connection()

results = conn.execute("""
    SELECT
        qc.query_hash,
        qc.table_name,
        AVG(CASE WHEN qm.collected_at >= NOW() - INTERVAL '1 day'
                 THEN qm.cpu_time_ms END) as cpu_last_24h,
        AVG(CASE WHEN qm.collected_at < NOW() - INTERVAL '1 day'
                 AND qm.collected_at >= NOW() - INTERVAL '7 days'
                 THEN qm.cpu_time_ms END) as cpu_prev_6days,
        ((cpu_last_24h - cpu_prev_6days) / NULLIF(cpu_prev_6days, 0) * 100) as growth_percent
    FROM queries_collected qc
    JOIN query_metrics qm ON qc.query_hash = qm.query_hash
    WHERE qm.collected_at >= NOW() - INTERVAL '7 days'
    GROUP BY qc.query_hash, qc.table_name
    HAVING cpu_prev_6days IS NOT NULL
    ORDER BY growth_percent DESC
    LIMIT 10
""").fetchall()

print("Queries com maior crescimento de CPU:")
for row in results:
    print(f"{row[1]}: {row[4]:.1f}% de crescimento")
```

### Exemplo 2: Correlação Entre Tamanho de Tabela e Performance

```python
results = conn.execute("""
    SELECT
        tm.table_name,
        tm.row_count,
        tm.total_size_mb,
        AVG(qm.duration_ms) as avg_duration,
        COUNT(DISTINCT qc.query_hash) as problem_query_count
    FROM table_metadata tm
    JOIN queries_collected qc ON tm.table_name = qc.table_name
    JOIN query_metrics qm ON qc.query_hash = qm.query_hash
    WHERE tm.captured_at = (
        SELECT MAX(captured_at) FROM table_metadata WHERE table_name = tm.table_name
    )
    GROUP BY tm.table_name, tm.row_count, tm.total_size_mb
    ORDER BY problem_query_count DESC
    LIMIT 20
""").fetchall()
```

---

## 🔗 Export e Integração

### 1. Export para Parquet

Formato colunar otimizado para análise.

```python
# Export de queries
store.export_to_parquet(
    table_name='queries_collected',
    output_path='exports/queries.parquet',
    hours=168  # Última semana
)

# Export de métricas
store.export_to_parquet(
    table_name='query_metrics',
    output_path='exports/metrics.parquet',
    hours=168
)
```

### 2. Integração com Pandas

```python
import pandas as pd

# Ler parquet
df_queries = pd.read_parquet('exports/queries.parquet')
df_metrics = pd.read_parquet('exports/metrics.parquet')

# Análise com pandas
top_tables = df_queries.groupby('table_name').size().sort_values(ascending=False)
print(top_tables.head(10))

# Correlação entre métricas
correlation = df_metrics[['cpu_time_ms', 'duration_ms', 'logical_reads']].corr()
print(correlation)
```

### 3. Integração com Power BI

1. Export dados para Parquet
2. Power BI → Get Data → Parquet
3. Criar relacionamentos entre tabelas
4. Criar medidas DAX customizadas

### 4. Integração com Grafana (Futuro)

```python
# TODO: Implementar exporter Prometheus
# Será adicionado em próxima versão
```

---

## ⚡ Performance e Otimização

### Índices Criados

O MetricsStore cria automaticamente índices otimizados:

- `queries_collected`: query_hash, collected_at, instance_name
- `query_metrics`: query_hash, cpu_time_ms, duration_ms
- `llm_analyses`: query_hash, expires_at, severity
- `monitoring_cycles`: cycle_started_at, instance_name
- `performance_alerts`: alert_time, severity

### Compressão

DuckDB usa compressão automática:
- Dados colunares → ~5-10x menos espaço
- Queries analíticas → ~10-100x mais rápidas que row-based

### Retenção de Dados

Configure no `config.json`:

```json
{
  "metrics_store": {
    "retention_days": 30
  }
}
```

### Cleanup Manual

```python
# Remover análises LLM expiradas
removed = store.cleanup_expired_analyses()
print(f"Removidas {removed} análises expiradas")

# Vacuum (compactar banco)
store.vacuum_database()
```

---

## 🤔 FAQ

### Q: O MetricsStore substitui os logs TXT?

**A:** Não totalmente. Os logs TXT tradicionais continuam sendo gerados para compatibilidade, mas o MetricsStore é a fonte primária de análises.

### Q: Qual o overhead de performance?

**A:** Mínimo. DuckDB é muito rápido:
- Inserção: ~1-2ms por query
- Queries analíticas: sub-segundo
- Thread-safe com connection pooling

### Q: Posso usar com banco de dados remoto?

**A:** O DuckDB é embarcado (arquivo local). Para análises distribuídas, export para Parquet e use Apache Spark ou similar.

### Q: Como faço backup?

**A:** Simples! O banco é um único arquivo:

```bash
cp logs/metrics.duckdb backups/metrics_$(date +%Y%m%d).duckdb
```

### Q: Posso integrar com meu dashboard existente?

**A:** Sim! Use a API QueryAnalytics ou exporte para Parquet e importe na sua ferramenta de BI favorita.

### Q: E se o arquivo DuckDB corromper?

**A:** DuckDB é ACID-compliant (como PostgreSQL). Corrupção é rara, mas se ocorrer, delete o arquivo e ele será recriado automaticamente.

### Q: Posso fazer queries SQL diretamente?

**A:** Sim! DuckDB suporta SQL completo:

```python
conn = store._get_connection()
result = conn.execute("SELECT * FROM queries_collected LIMIT 10").fetchall()
```

### Q: Como monitorar o próprio MetricsStore?

**A:** Use `analytics.get_monitoring_health()` para métricas de saúde.

---

## 📚 Referências

- **DuckDB**: https://duckdb.org/
- **SQL Monitor**: [README.md](README.md)
- **Configuração**: [CONFIGURATION.md](CONFIGURATION.md)
- **Exemplos**: [examples_analytics.py](examples_analytics.py)

---

## ✅ Checklist de Implementação

- [x] Schema DuckDB com 6 tabelas
- [x] MetricsStore com 20+ métodos
- [x] QueryAnalytics com 12+ queries prontas
- [x] Integração em DatabaseMonitor
- [x] Integração em MultiDatabaseMonitor
- [x] Thread-safety completo
- [x] Script de exemplo (examples_analytics.py)
- [x] Configuração em config.json
- [x] Documentação completa
- [ ] Testes unitários (futuro)
- [ ] Export para Prometheus (futuro)

---

**Pronto para usar!** 🎉

Execute `python examples_analytics.py` para ver o sistema em ação.
