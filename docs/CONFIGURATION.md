## Guia de Configuração - Multi-Database Monitor

Este documento descreve todas as opções de configuração disponíveis e como validá-las.

## 📋 Arquivos de Configuração

O projeto usa 2 arquivos principais de configuração:

1. **`config.json`** - Configurações gerais do monitor
2. **`config/databases.json`** - Configuração de databases a monitorar

## ✅ Validação de Configuração

O projeto inclui **validação automática com Pydantic** que garante que:
- Todos os campos obrigatórios estão presentes
- Tipos de dados estão corretos
- Valores estão dentro de limites válidos
- Configurações são consistentes

### Validar Antes de Executar

```bash
# Validar configurações
python validate_config.py

# Validar arquivo específico
python validate_config.py --config custom.json
python validate_config.py --databases custom_db.json
```

O validador mostra:
- ✅ Configurações válidas
- ❌ Erros encontrados
- ⚠️  Avisos de segurança (senhas em plaintext)
- 📊 Resumo da configuração

---

## 📄 config.json

### Estrutura Completa

```json
{
  "monitor": {
    "interval_seconds": 60
  },
  "performance_thresholds": {
    "execution_time_seconds": 30,
    "cpu_time_ms": 10000,
    "logical_reads": 50000,
    "physical_reads": 10000,
    "writes": 5000
  },
  "llm": {
    "provider": "gemini",
    "model": "gemini-2.0-flash-exp",
    "temperature": 0.1,
    "max_tokens": 8192,
    "max_retries": 3,
    "retry_delays": [3, 8, 15],
    "rate_limit": {
      "max_requests_per_day": 1500,
      "max_requests_per_minute": 60,
      "max_requests_per_cycle": 5,
      "min_delay_between_requests": 2
    }
  },
  "logging": {
    "log_directory": "logs",
    "include_execution_plan": true,
    "max_query_length": 10000
  },
  "security": {
    "sanitize_queries": true,
    "placeholder_prefix": "@p",
    "show_example_values": true
  },
  "query_cache": {
    "enabled": true,
    "ttl_hours": 24,
    "cache_file": "logs/query_cache.json",
    "auto_save_interval": 300
  },
  "teams": {
    "enabled": false,
    "webhook_url": null,
    "notify_on_cache_hit": true,
    "priority_filter": [],
    "timeout": 10
  },
  "timeouts": {
    "database_connect": 10,
    "database_query": 60,
    "llm_analysis": 30,
    "thread_shutdown": 90
  },
  "logging": {
    "level": "INFO",
    "format": "colored",
    "log_file": "logs/monitor.log",
    "enable_console": true
  }
}
```

### Seções Detalhadas

#### monitor
Configurações de monitoramento.

| Campo | Tipo | Padrão | Limites | Descrição |
|-------|------|--------|---------|-----------|
| `interval_seconds` | int | 60 | 10-3600 | Intervalo entre verificações |

#### performance_thresholds
Thresholds para identificar queries problemáticas. Queries que ultrapassarem **qualquer** destes valores serão analisadas.

| Campo | Tipo | Padrão | Descrição |
|-------|------|--------|-----------|
| `execution_time_seconds` | float | 30 | Tempo total de execução |
| `cpu_time_ms` | int | 10000 | Tempo de CPU em ms |
| `logical_reads` | int | 50000 | Leituras lógicas |
| `physical_reads` | int | 10000 | Leituras físicas |
| `writes` | int | 5000 | Operações de escrita |

#### llm
Configuração do Google Gemini.

| Campo | Tipo | Padrão | Limites | Descrição |
|-------|------|--------|---------|-----------|
| `provider` | string | "gemini" | - | Provider de LLM (apenas gemini) |
| `model` | string | "gemini-2.0-flash-exp" | - | Modelo do Gemini |
| `temperature` | float | 0.1 | 0.0-2.0 | Temperatura (criatividade) |
| `max_tokens` | int | 8192 | 256-32768 | Tokens máximos na resposta |
| `max_retries` | int | 3 | 1-10 | Tentativas em caso de erro |
| `retry_delays` | array | [3,8,15] | - | Delays entre retries (segundos) |

**rate_limit** (dentro de llm):

| Campo | Tipo | Padrão | Limites | Descrição |
|-------|------|--------|---------|-----------|
| `max_requests_per_day` | int | 1500 | 0+ | Máx requests/dia (0=ilimitado) |
| `max_requests_per_minute` | int | 60 | 1-1000 | Máx requests/minuto |
| `max_requests_per_cycle` | int | 5 | 1-100 | Máx requests/ciclo |
| `min_delay_between_requests` | float | 2 | 0-60 | Delay mínimo entre requests |

#### query_cache
Configuração do cache de queries analisadas.

| Campo | Tipo | Padrão | Limites | Descrição |
|-------|------|--------|---------|-----------|
| `enabled` | bool | true | - | Habilitar cache |
| `ttl_hours` | int | 24 | 1-168 | Time to live (horas) |
| `cache_file` | string | "logs/..." | - | Caminho do arquivo |
| `auto_save_interval` | int | 300 | 60-3600 | Intervalo de auto-save (seg) |

#### teams
Integração com Microsoft Teams via Power Automate.

| Campo | Tipo | Padrão | Descrição |
|-------|------|--------|-----------|
| `enabled` | bool | false | Habilitar integração |
| `webhook_url` | string | null | **Obrigatório se enabled=true** |
| `notify_on_cache_hit` | bool | true | Notificar em cache hits |
| `priority_filter` | array | [] | Filtro por prioridade (vazio=todas) |
| `timeout` | int | 10 | Timeout de requisição (seg) |

⚠️ **Importante**: Se `enabled: true`, então `webhook_url` é obrigatório.

#### timeouts
Timeouts de operações.

| Campo | Tipo | Padrão | Limites | Descrição |
|-------|------|--------|---------|-----------|
| `database_connect` | int | 10 | 1-60 | Timeout de conexão (seg) |
| `database_query` | int | 60 | 5-300 | Timeout de query (seg) |
| `llm_analysis` | int | 30 | 10-120 | Timeout de análise LLM (seg) |
| `thread_shutdown` | int | 90 | 30-300 | Timeout de shutdown (seg) |
| `circuit_breaker_recovery` | int | 60 | 10-300 | Timeout de recuperação do circuit breaker (seg) |

#### logging (structured logging)
Configuração de logging estruturado.

| Campo | Tipo | Padrão | Valores | Descrição |
|-------|------|--------|---------|-----------|
| `level` | string | "INFO" | DEBUG/INFO/WARNING/ERROR/CRITICAL | Nível de log |
| `format` | string | "colored" | colored/json/simple | Formato de saída |
| `log_file` | string | "logs/..." | - | Arquivo de log |
| `enable_console` | bool | true | - | Habilitar console |

**Formatos**:
- `colored` - Console com cores ANSI (desenvolvimento)
- `json` - JSON estruturado (produção/ELK)
- `simple` - Texto simples

---

## 📄 config/databases.json

### Estrutura

```json
{
  "databases": [
    {
      "name": "SQL Server - Produção",
      "type": "SQLSERVER",
      "enabled": true,
      "credentials": {
        "server": "sqlserver-prod.com",
        "port": "1433",
        "database": "master",
        "username": "monitor_user",
        "password": "${SQL_SERVER_PROD_PASSWORD}",
        "driver": "ODBC Driver 18 for SQL Server"
      }
    },
    {
      "name": "PostgreSQL - Produção",
      "type": "POSTGRESQL",
      "enabled": true,
      "credentials": {
        "server": "postgresql-prod.com",
        "port": "5432",
        "database": "postgres",
        "username": "monitor_user",
        "password": "${POSTGRESQL_PROD_PASSWORD}"
      }
    },
    {
      "name": "SAP HANA - Produção",
      "type": "HANA",
      "enabled": false,
      "credentials": {
        "server": "hana-prod.com",
        "port": "30015",
        "database": "SYSTEMDB",
        "username": "monitor_user",
        "password": "${HANA_PROD_PASSWORD}"
      }
    }
  ]
}
```

### Campos de Database Entry

| Campo | Tipo | Obrigatório | Valores | Descrição |
|-------|------|-------------|---------|-----------|
| `name` | string | Sim | - | Nome identificador |
| `type` | string | Sim | SQLSERVER/POSTGRESQL/HANA | Tipo de database |
| `enabled` | bool | Não (padrão: true) | - | Se deve monitorar |
| `credentials` | object | Sim | - | Credenciais de conexão |

### Campos de Credentials

| Campo | Tipo | Obrigatório | Descrição |
|-------|------|-------------|-----------|
| `server` | string | Sim | Endereço do servidor |
| `port` | string | Sim | Porta (1-65535) |
| `database` | string | Sim | Nome do database |
| `username` | string | Sim | Usuário |
| `password` | string | Sim | Senha (use ${VAR_NAME}) |
| `driver` | string | Não | Driver ODBC (apenas SQL Server) |

⚠️ **Importante**: Use variáveis de ambiente para senhas: `"password": "${SQL_SERVER_PROD_PASSWORD}"`

### Validações Automáticas

1. **Pelo menos 1 database habilitado**
   ```
   ❌ Erro: Pelo menos um database deve estar habilitado (enabled: true)
   ```

2. **Tipo válido**
   ```
   ❌ Erro: type deve ser SQLSERVER, POSTGRESQL ou HANA
   ```

3. **Porta válida**
   ```
   ❌ Erro: Porta deve estar entre 1-65535, recebido: 99999
   ```

4. **Campos obrigatórios**
   ```
   ❌ Erro: Field required [type=missing, input_value=...]
   ```

---

## 🧪 Exemplos de Validação

### Sucesso ✅

```bash
$ python validate_config.py

================================================================================
Validando: config.json
================================================================================

✅ config.json está válido!

📊 Resumo da Configuração:
  • Intervalo de monitoramento: 60s
  • Modelo LLM: gemini-2.0-flash-exp
  • Temperatura: 0.1
  • Max requests/dia: 1500
  • Cache habilitado: Sim
  • TTL do cache: 24h
  • Teams habilitado: Não
  • Nível de log: INFO
  • Formato de log: colored

================================================================================
Validando: config/databases.json
================================================================================

✅ databases.json está válido!

📊 Resumo de Databases:
  • Total configurados: 3
  • Habilitados: 2
  • Desabilitados: 1

📋 Databases Habilitados:
  🟢 🔒 SQL Server - Produção (SQLSERVER) - sqlserver-prod.com:1433
  🟢 🔒 PostgreSQL - Produção (POSTGRESQL) - postgresql-prod.com:5432

📋 Databases Desabilitados:
  🔴 SAP HANA - Produção (HANA)

================================================================================
RESULTADO DA VALIDAÇÃO
================================================================================

✅ Todas as configurações estão válidas!

🚀 Você está pronto para executar o monitor:
   python main.py
```

### Erro ❌

```bash
$ python validate_config.py

================================================================================
Validando: config.json
================================================================================

❌ Erro de validação:

1 validation error for Config
llm -> temperature
  Input should be less than or equal to 2.0 [type=less_than_equal]

💡 Dica: Corrija os erros acima e tente novamente
```

---

## 🔍 Dicas de Troubleshooting

### Erro: "Field required"

**Causa**: Campo obrigatório faltando
**Solução**: Adicione o campo ao arquivo JSON

### Erro: "Input should be less than or equal to X"

**Causa**: Valor acima do limite permitido
**Solução**: Ajuste o valor para estar dentro do range

### Erro: "Porta deve estar entre 1-65535"

**Causa**: Porta inválida
**Solução**: Use uma porta válida (1-65535)

### Aviso: "Senha em plaintext detectada"

**Causa**: Senha não usa variável de ambiente
**Solução**: Substitua por `"${VAR_NAME}"` e configure a variável no `.env`

### Erro: "webhook_url é obrigatório quando Teams está habilitado"

**Causa**: Teams habilitado mas sem webhook_url
**Solução**: Configure `webhook_url` ou desabilite Teams

---

## 📚 Referências

- [SECURITY.md](SECURITY.md) - Guia de segurança e secrets management
- [LOGGING.md](LOGGING.md) - Guia de logging estruturado
- [Pydantic Documentation](https://docs.pydantic.dev/)

---

**Dica**: Sempre execute `python validate_config.py` antes de fazer deploy para garantir que as configurações estão corretas!
