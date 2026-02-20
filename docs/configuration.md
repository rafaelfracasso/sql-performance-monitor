[← Primeiros Passos](getting-started.md) · [Back to README](../README.md) · [Bancos de Dados →](databases.md)

# Configuracao

O SQL Monitor usa dois arquivos de configuracao estáticos (`config.json` e `config/databases.json`) para o bootstrap inicial, e um banco DuckDB para todas as configurações de runtime. Credenciais ficam em `.env`.

---

## config.json — Bootstrap

Lido apenas na inicialização, antes do DuckDB estar disponível. Serve exclusivamente para indicar onde está (ou onde criar) o arquivo DuckDB de métricas.

```json
{
  "metrics_store": {
    "db_path": "logs/metrics.duckdb",
    "enable_compression": true,
    "retention_days": 30
  }
}
```

| Campo | Padrão | Descricao |
|-------|--------|-----------|
| `db_path` | `logs/metrics.duckdb` | Caminho do arquivo DuckDB (criado automaticamente) |
| `enable_compression` | `true` | Compressão dos dados armazenados |
| `retention_days` | `30` | Dias de retenção de métricas |

**Todas as outras configurações** (LLM, thresholds, Teams, logging, weekly optimizer, etc.) são gerenciadas via API/Dashboard e persistidas no DuckDB. Não pertencem ao `config.json`.

---

## config/databases.json

Define quais bancos de dados monitorar. Copie de `config/databases.json.example`.

### Estrutura básica

```json
{
  "databases": [
    {
      "name": "Nome amigavel",
      "type": "SQLSERVER",
      "enabled": true,
      "credentials": {
        "server": "servidor.exemplo.com",
        "port": "1433",
        "database": "master",
        "username": "monitor_user",
        "password": "${NOME_DA_VARIAVEL}"
      }
    }
  ]
}
```

| Campo | Valores validos | Descricao |
|-------|----------------|-----------|
| `type` | `SQLSERVER`, `POSTGRESQL`, `HANA` | Tipo do banco |
| `enabled` | `true`/`false` | Se `false`, o banco é ignorado sem remover a configuracao |
| `password` | `${VAR}` ou texto | Use `${VAR}` para referenciar `.env` |

**Importante:** Nunca commite este arquivo com senhas em plaintext. O arquivo já está no `.gitignore`.

Veja [Bancos de Dados](databases.md) para configuracao detalhada de cada tipo.

---

## .env — Variaveis de Ambiente

Copie de `.env.example`. Nunca commite o arquivo `.env`.

### API Key do LLM

```env
GEMINI_API_KEY=sua_chave_aqui
```

Obtenha gratuitamente em [Google AI Studio](https://makersuite.google.com/app/apikey).

### Senhas dos bancos de dados

Referencie no `databases.json` como `"password": "${NOME_DA_VAR}"` e defina aqui:

```env
SQL_SERVER_PROD_PASSWORD=senha_aqui
SQL_SERVER_DEV_PASSWORD=senha_aqui
POSTGRESQL_PROD_PASSWORD=senha_aqui
HANA_PROD_PASSWORD=senha_aqui
```

### Variaveis para testes de integracao

Usadas pelos testes em `tests/integration/`:

```env
SQL_SERVER=localhost
SQL_PORT=1433
SQL_DATABASE=master
SQL_USERNAME=sa
SQL_PASSWORD=sua_senha_aqui
SQL_DRIVER=ODBC Driver 18 for SQL Server

PG_SERVER=localhost
PG_PORT=5432
PG_DATABASE=postgres
PG_USERNAME=postgres
PG_PASSWORD=sua_senha_aqui

HANA_SERVER=localhost
HANA_PORT=30015
HANA_DATABASE=SYSTEMDB
HANA_USERNAME=SYSTEM
HANA_PASSWORD=sua_senha_aqui
```

---

## Configuracoes em Runtime (Dashboard / API)

Após iniciar o monitor, todas as configurações de comportamento são gerenciadas pela interface em `/settings` ou diretamente via API. As alterações são persistidas no DuckDB e aplicadas sem reiniciar o processo.

### Thresholds de performance

Limites por tipo de banco para geração de alertas. Configurável por SGBD em `/settings` → aba **Performance**.

| Campo | Descricao |
|-------|-----------|
| `execution_time_ms` | Duração máxima de execução |
| `cpu_time_ms` | Tempo de CPU máximo |
| `logical_reads` | Leituras lógicas máximas |
| `physical_reads` | Leituras físicas máximas |
| `writes` | Escritas máximas |
| `wait_time_ms` | Tempo de espera máximo |
| `memory_mb` | Uso de memória máximo |
| `row_count` | Quantidade de linhas máxima |

API: `GET /api/settings/thresholds` · `POST /api/settings/thresholds/{db_type}`

### Coleta

Controla o que é coletado a cada ciclo, por tipo de banco. Aba **Infraestrutura**.

| Campo | Descricao |
|-------|-----------|
| `min_duration_seconds` | Duração mínima para capturar uma query |
| `collect_active_queries` | Capturar queries ativas no momento |
| `collect_expensive_queries` | Capturar queries com alto custo acumulado |
| `collect_table_scans` | Capturar table scans |
| `max_queries_per_cycle` | Limite de queries por ciclo de coleta |

API: `GET /api/settings/collection` · `POST /api/settings/collection/{db_type}`

### Monitor

| Campo | Descricao |
|-------|-----------|
| `interval_seconds` | Intervalo entre ciclos de coleta (padrão: 60s) |

API: `GET /api/settings/monitor` · `POST /api/settings/monitor`

### LLM

Configuracoes do Google Gemini. Aba **LLM & Prompts**.

| Campo | Descricao |
|-------|-----------|
| `model` | Modelo Gemini (ex: `gemini-flash-latest`) |
| `temperature` | Temperatura de geração (0.0–1.0) |
| `max_tokens` | Tokens máximos por resposta |
| `max_requests_per_day` | Limite diário de requisições ao LLM |
| `max_requests_per_minute` | Limite por minuto |
| `max_requests_per_cycle` | Limite por ciclo de monitoramento |
| `min_delay_between_requests` | Delay mínimo entre chamadas (segundos) |

API: `GET /api/settings/llm` · `POST /api/settings/llm`

### Notificacoes Teams

Aba **Notificacoes**.

| Campo | Descricao |
|-------|-----------|
| `enabled` | Habilita notificações |
| `webhook_url` | URL do webhook do canal Teams |
| `notify_on_cache_hit` | Notificar mesmo quando query já foi analisada |
| `priority_filter` | Lista de severidades a notificar |
| `timeout` | Timeout em segundos para envio |

API: `GET /api/settings/teams` · `POST /api/settings/teams`

### Timeouts

Aba **Infraestrutura**.

| Campo | Descricao |
|-------|-----------|
| `database_connect` | Timeout de conexão ao banco (segundos) |
| `database_query` | Timeout de execução de query (segundos) |
| `llm_analysis` | Timeout de análise LLM (segundos) |
| `thread_shutdown` | Timeout de encerramento de threads (segundos) |
| `circuit_breaker_recovery` | Tempo de recuperação do circuit breaker (segundos) |

API: `GET /api/settings/timeouts` · `POST /api/settings/timeouts`

### Logging

Aba **Infraestrutura** (requer reinício para mudanças no `level` e `format` terem efeito no terminal).

| Campo | Valores | Descricao |
|-------|---------|-----------|
| `level` | DEBUG, INFO, WARNING, ERROR | Nível de log |
| `format` | colored, json | Formato de saída |
| `log_file` | caminho | Arquivo de log |
| `enable_console` | true/false | Exibir logs no terminal |

API: `GET /api/settings/logging` · `POST /api/settings/logging`

### Seguranca e Auditoria

Aba **Security & Audit**.

| Campo | Descricao |
|-------|-----------|
| `sanitize_queries` | Substituir parâmetros das queries por placeholders |
| `placeholder_prefix` | Prefixo dos placeholders (padrão: `@p`) |
| `show_example_values` | Exibir valores exemplo nas queries sanitizadas |

API: `GET /api/settings/security` · `POST /api/settings/security`

### Weekly Optimizer

Aba **Weekly Optimizer**. Veja [Otimizacao](optimization.md) para detalhes do fluxo completo.

| Grupo | Campos principais |
|-------|------------------|
| `schedule` | `analysis_day`, `analysis_time`, `execution_day`, `execution_time`, `report_day`, `report_time` |
| `veto_window` | `hours` (janela para vetar planos), `check_before_execution` |
| `risk_thresholds` | `table_size_gb_medium/high/critical`, `index_fragmentation_percent`, `max_execution_time_minutes` |
| `auto_rollback` | `enabled`, `degradation_threshold_percent`, `wait_after_execution_minutes` |
| `api` | `enabled`, `host`, `port` |
| `analysis` | `days`, `min_occurrences`, `min_avg_duration_ms` |

API: `GET /api/settings/weekly_optimizer` · `POST /api/settings/weekly_optimizer`

### Reset para padrões

Restaura thresholds e configurações de coleta de um tipo de banco para os valores padrão:

```
POST /api/settings/reset/{db_type}
```

### Histórico de alterações

Todas as mudanças feitas via API são registradas automaticamente:

```
GET /api/settings/audit
```

## See Also

- [Primeiros Passos](getting-started.md) — instalacao e setup inicial
- [Bancos de Dados](databases.md) — credenciais por tipo de SGBD
- [Otimizacao](optimization.md) — configuracao do weekly optimizer
