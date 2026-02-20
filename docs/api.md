[← Otimizacao](optimization.md) · [Back to README](../README.md)

# API REST

Base URL: `http://localhost:8080` (porta configuravel em `config.json → weekly_optimizer.api.port`).

Todos os endpoints retornam JSON. Nao ha autenticacao (ferramenta interna).

---

## Dashboard

### Resumo geral

```
GET /api/dashboard/summary
```

Retorna totais de queries, alertas ativos e instancias online.

### Instancias

```
GET /api/dashboard/instances
```

Lista todas as instancias monitoradas com status de conexao e ultima coleta.

### Queries

```
GET /api/dashboard/queries?period=1h&limit=50&db_type=SQLSERVER&min_duration_ms=0
```

| Parametro | Padrao | Descricao |
|-----------|--------|-----------|
| `period` | `1h` | Janela de tempo: `1h`, `6h`, `12h`, `24h`, `7d`, `30d` |
| `limit` | `50` | Numero maximo de resultados |
| `db_type` | todos | Filtrar por: `SQLSERVER`, `POSTGRESQL`, `HANA` |
| `min_duration_ms` | `0` | Duracao minima em ms |

### Texto completo de uma query

```
GET /api/queries/{query_hash}/full-text
```

### Timeline de execucoes

```
GET /api/queries/timeline?period=24h&query_hash={hash}
```

### Distribuicao de queries

```
GET /api/queries/distribution?period=24h
```

Retorna distribuicao por faixa de duracao (0-100ms, 100-500ms, etc.).

### Exportar queries

```
GET /api/queries/export?period=24h&format=csv
```

### Tendencias

```
GET /api/dashboard/trends?period=7d
```

### Cache efficiency

```
GET /api/dashboard/cache-efficiency
```

### Alertas

```
GET /api/dashboard/alerts?period=24h&severity=high
```

| Parametro | Descricao |
|-----------|-----------|
| `severity` | Filtrar por: `low`, `medium`, `high`, `critical` |

---

## Wait Stats

```
GET /api/wait-stats?instance=nome&period=1h
GET /api/wait-stats/timeline?instance=nome&period=24h
```

Retorna estatisticas de espera por categoria (lock, io, cpu, etc.).

---

## Analise LLM

### Analise em lote

```
POST /api/analyze/bulk
Content-Type: application/json

{
  "query_hashes": ["hash1", "hash2"],
  "force": false
}
```

| Campo | Descricao |
|-------|-----------|
| `query_hashes` | Lista de hashes das queries a analisar |
| `force` | `true` para reanalisar mesmo queries ja analisadas |

---

## Planos de Otimizacao

```
GET  /api/plans               Lista todos os planos
GET  /api/plans/{id}          Detalhe de um plano
GET  /api/plans/{id}/status   Status atual
POST /api/plans/generate      Gera novo plano manualmente
DELETE /api/plans/{id}        Remove um plano
```

### Gerar plano

```
POST /api/plans/generate
Content-Type: application/json

{
  "db_type": "SQLSERVER",
  "instance": "SQL Server - Producao"
}
```

---

## Configuracoes

Todos os endpoints de settings seguem o padrao `GET /api/settings/{grupo}` para leitura e `POST /api/settings/{grupo}` (ou `POST /api/settings/{grupo}/{db_type}`) para escrita.

### Thresholds de performance

```
GET  /api/settings/thresholds
POST /api/settings/thresholds/{db_type}
```

### Coleta

```
GET  /api/settings/collection
POST /api/settings/collection/{db_type}
```

### Cache de queries

```
GET  /api/settings/cache
POST /api/settings/cache
POST /api/settings/cache/clear    Limpa o cache de queries
```

### LLM

```
GET  /api/settings/llm
POST /api/settings/llm
```

### Monitor

```
GET  /api/settings/monitor
POST /api/settings/monitor
```

### Teams

```
GET  /api/settings/teams
POST /api/settings/teams
```

### Timeouts

```
GET  /api/settings/timeouts
POST /api/settings/timeouts
```

### Security

```
GET  /api/settings/security
POST /api/settings/security
```

### Logging

```
GET  /api/settings/logging
POST /api/settings/logging
```

### Metrics Store

```
GET  /api/settings/metrics_store
POST /api/settings/metrics_store
```

### Weekly Optimizer

```
GET  /api/settings/weekly_optimizer
POST /api/settings/weekly_optimizer
```

### Reset de configuracoes por tipo de banco

```
POST /api/settings/reset/{db_type}
```

### Auditoria

```
GET /api/settings/audit
```

---

## Prompts LLM

Os prompts usados pelo Gemini sao editaveis com historico de versoes.

```
GET    /api/prompts
GET    /api/prompts/{db_type}/{prompt_type}
POST   /api/prompts/{db_type}/{prompt_type}
GET    /api/prompts/{db_type}/{prompt_type}/history
POST   /api/prompts/{db_type}/{prompt_type}/rollback/{version}
DELETE /api/prompts/{db_type}/{prompt_type}
```

| Parametro | Valores validos |
|-----------|----------------|
| `db_type` | `sqlserver`, `postgresql`, `hana` |
| `prompt_type` | `analysis`, `optimization`, `index_suggestion` (varia por SGBD) |

---

## Paginas HTML

As paginas do dashboard sao servidas como HTML via Jinja2:

| URL | Pagina |
|-----|--------|
| `/` ou `/dashboard` | Home |
| `/dashboard/queries` | Lista de queries |
| `/dashboard/queries/{hash}` | Detalhe de query |
| `/dashboard/alerts` | Alertas |
| `/dashboard/alerts/{id}` | Detalhe de alerta |
| `/dashboard/instances` | Instancias |
| `/dashboard/trends` | Tendencias |
| `/dashboard/users` | Por usuario |
| `/dashboard/hosts` | Por host |
| `/dashboard/applications` | Por aplicacao |
| `/dashboard/llm` | Historico LLM |
| `/dashboard/duckdb` | Info DuckDB |
| `/settings` | Configuracoes |
| `/plans` | Planos de otimizacao |
| `/plans/{id}` | Detalhe de plano |

## See Also

- [Dashboard](dashboard.md) — como usar a interface web
- [Otimizacao](optimization.md) — endpoints de planos em detalhe
- [Configuracao](configuration.md) — habilitar a API no config.json
