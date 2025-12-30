# Multi-Database Performance Monitor

Monitor inteligente de performance para bancos de dados relacionais com análise por LLM, dashboard web interativo e sistema de otimização semanal automatizada.

## Visão Geral

Sistema completo de monitoramento e otimização de performance que:
- Monitora múltiplas instâncias de bancos de dados simultaneamente
- Analisa queries problemáticas usando Google Gemini LLM
- Armazena métricas históricas em DuckDB analítico
- Oferece dashboard web interativo para observabilidade
- Executa otimizações automáticas semanais com aprovação via web
- Integra com Microsoft Teams via Adaptive Cards

## Bancos de Dados Suportados

- **SQL Server** - Suporte completo (2016+)
- **PostgreSQL** - Suporte completo (9.6+)
- **SAP HANA** - Em desenvolvimento

## Principais Funcionalidades

### 1. Monitoramento Multi-Database
- Monitoramento simultâneo de múltiplas instâncias
- Thread dedicada por tipo de banco (SQL Server, PostgreSQL, HANA)
- Processamento sequencial dentro de cada tipo
- Cache individual thread-safe por tipo

### 2. Análise Inteligente via LLM
- Análise de queries problemáticas via Google Gemini
- Sugestões de índices otimizados com justificativa técnica
- Sanitização automática de dados sensíveis
- Sistema de cache para evitar análises duplicadas
- Controle de rate limiting e retry com backoff exponencial

### 3. Métricas e Observabilidade (DuckDB)
- Armazenamento de métricas em DuckDB analítico embarcado
- Histórico de queries com 30 dias de retenção (configurável)
- Alertas automáticos baseados em thresholds
- Análise de tendências e performance
- Tracking de cache hits e análises LLM

### 4. Dashboard Web Interativo
- Interface web com FastAPI
- Visualização de métricas em tempo real
- Gráficos interativos com Chart.js
- Top queries problemáticas
- Timeline de alertas
- Status de instâncias monitoradas
- Análise de tendências (7/30 dias)

**Páginas disponíveis:**
- `/` - Dashboard principal com overview
- `/dashboard/queries` - Top queries por CPU/duração/reads
- `/dashboard/alerts` - Timeline e análise de alertas
- `/dashboard/instances` - Status e health das instâncias
- `/dashboard/trends` - Gráficos de tendência histórica
- `/plans` - Gerenciamento de planos de otimização

### 5. Weekly Optimization System
Sistema automatizado de otimização semanal que:

**Quinta-feira 18:00 - Geração do Plano:**
- Analisa últimos 7 dias de métricas no DuckDB
- Identifica queries problemáticas e missing indexes
- Gera plano de otimizações automatizado
- Classifica risco em 4 níveis (LOW/MEDIUM/HIGH/CRITICAL)
- Auto-aprova baseado em regras de risco
- Notifica Teams via Adaptive Card com link para revisão

**Quinta a Domingo (72h) - Janela de Veto:**
- DBA pode revisar plano via web interface
- Veto granular (plano completo ou itens individuais)
- Aprovação explícita para execução imediata
- Visualização de scripts SQL e metadados

**Domingo 02:00 - Execução Automática:**
- Verifica se não há vetos ativos
- Executa otimizações em ordem de prioridade
- Captura métricas antes/depois
- Rollback automático se degradação > 20%
- Aguarda 10 min entre otimizações

**Segunda 08:00 - Relatório de Impacto:**
- Compara métricas 7 dias antes vs depois
- Calcula ROI real (payback period)
- Identifica melhores/piores otimizações
- Gera recomendações para próximo ciclo
- Envia relatório executivo via Teams

**Características:**
- Classificação automática de risco
- Rollback automático em caso de degradação
- Veto granular via web interface
- Adaptive Cards no Teams com links
- Auditoria completa no DuckDB
- Dry-run mode para testes

Ver documentação completa em: [WEEKLY_OPTIMIZER.md](WEEKLY_OPTIMIZER.md)

### 6. Integração Microsoft Teams
- Notificações de queries problemáticas
- Adaptive Cards para planos de otimização
- Links para interface web interna
- Relatórios executivos de impacto
- Alertas críticos em tempo real

## Pré-requisitos

### Software Base
- Python 3.8 ou superior
- Google Gemini API key (gratuita: https://makersuite.google.com/app/apikey)

### Drivers de Banco de Dados

**SQL Server:**
- Driver ODBC 17 ou 18 para SQL Server
- SQL Server 2016 ou superior (DMVs habilitadas)

**PostgreSQL:**
- Biblioteca psycopg2-binary
- PostgreSQL 9.6 ou superior
- Extensão pg_stat_statements (recomendada)

**SAP HANA:**
- Biblioteca hdbcli (em desenvolvimento)

## Instalação

### 1. Clone o Projeto

```bash
git clone <repository_url>
cd check_sql_server_performance
```

### 2. Instale Drivers de Banco de Dados

#### SQL Server - Driver ODBC

**Ubuntu/Debian:**
```bash
# Remover chave antiga (se existir)
sudo rm -f /etc/apt/trusted.gpg.d/microsoft.asc

# Baixar e importar chave GPG da Microsoft
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg

# Adicionar repositório Microsoft
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list

# Atualizar e instalar driver ODBC 18
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Instalar ferramentas opcionais (recomendado)
sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18 unixodbc-dev

# Adicionar ferramentas ao PATH (opcional)
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
source ~/.bashrc
```

**Windows:**

Baixe e instale o driver oficial:
https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

#### PostgreSQL - Biblioteca Python

A biblioteca psycopg2-binary será instalada automaticamente com os requirements.

### 3. Configure Ambiente Virtual Python (Recomendado)

```bash
python3 -m venv .venv
source .venv/bin/activate  # Linux/Mac
# ou
.venv\Scripts\activate     # Windows
```

### 4. Instale Dependências Python

```bash
pip install -r requirements.txt
```

Principais dependências:
- `pyodbc` - SQL Server
- `psycopg2-binary` - PostgreSQL
- `duckdb` - Armazenamento analítico de métricas
- `google-genai` - API Google Gemini
- `fastapi` - API REST e Dashboard Web
- `uvicorn` - Servidor ASGI
- `jinja2` - Templates HTML
- `schedule` - Agendamento de tarefas

### 5. Configure Credenciais

Copie o arquivo de exemplo e configure suas credenciais:

```bash
cp .env.example .env
nano .env
```

**Exemplo de configuração:**

```env
# SQL Server
SQL_SERVER=seu_servidor
SQL_PORT=1433
SQL_DATABASE=master
SQL_USERNAME=seu_usuario
SQL_PASSWORD=sua_senha
SQL_DRIVER=ODBC Driver 18 for SQL Server

# PostgreSQL
PG_SERVER=localhost
PG_PORT=5432
PG_DATABASE=postgres
PG_USERNAME=postgres
PG_PASSWORD=sua_senha

# Google Gemini API
GEMINI_API_KEY=sua_chave_api_gemini
```

### 6. Configure Databases

Copie o arquivo de exemplo e configure suas instâncias:

```bash
cp config/databases.json.example config/databases.json
nano config/databases.json
```

**Exemplo:**

```json
{
  "databases": [
    {
      "instance_name": "SQL-PROD-01",
      "type": "sqlserver",
      "enabled": true,
      "connection": {
        "server": "sql-prod-01.company.com",
        "port": 1433,
        "database": "master",
        "username": "monitor_user",
        "password": "${SQL_PASSWORD}",
        "driver": "ODBC Driver 18 for SQL Server",
        "trust_server_certificate": true
      }
    },
    {
      "instance_name": "PG-PROD-01",
      "type": "postgresql",
      "enabled": true,
      "connection": {
        "server": "pg-prod-01.company.com",
        "port": 5432,
        "database": "postgres",
        "username": "monitor_user",
        "password": "${PG_PASSWORD}"
      }
    }
  ]
}
```

### 7. Configure Parâmetros de Monitoramento (Opcional)

Edite `config.json` para ajustar:
- Intervalo de verificação (padrão: 60 segundos)
- Thresholds de performance
- Configurações da LLM (modelo Gemini, rate limiting)
- Integração com Teams
- API REST e Dashboard (porta, CORS)
- Weekly Optimizer (horários, thresholds, rollback)

## Uso

### Iniciar Sistema Completo

```bash
python main.py
```

O sistema iniciará:
1. Monitoramento de todas as instâncias habilitadas
2. API REST na porta 8080 (se habilitada)
3. Dashboard web em http://localhost:8080
4. Weekly Optimizer scheduler (se habilitado)

### Acessar Dashboard Web

Abra navegador em: http://localhost:8080

**Páginas disponíveis:**
- Dashboard principal: http://localhost:8080/
- Top queries: http://localhost:8080/dashboard/queries
- Alertas: http://localhost:8080/dashboard/alerts
- Instâncias: http://localhost:8080/dashboard/instances
- Tendências: http://localhost:8080/dashboard/trends
- Planos de otimização: http://localhost:8080/plans

### Habilitar API REST e Dashboard

Edite `config.json`:

```json
{
  "weekly_optimizer": {
    "api": {
      "enabled": true,
      "host": "0.0.0.0",
      "port": 8080,
      "cors_enabled": true
    }
  }
}
```

### Habilitar Weekly Optimizer

Edite `config.json`:

```json
{
  "weekly_optimizer": {
    "enabled": true,
    "schedule": {
      "analysis_day": "thursday",
      "analysis_time": "18:00",
      "execution_day": "sunday",
      "execution_time": "02:00",
      "report_day": "monday",
      "report_time": "08:00"
    },
    "veto_window": {
      "hours": 72
    },
    "auto_rollback": {
      "enabled": true,
      "degradation_threshold_percent": 20,
      "wait_after_execution_minutes": 10
    }
  }
}
```

### Executar Job Manualmente (Testes)

Para testar o Weekly Optimizer sem aguardar o agendamento:

```python
from sql_monitor.optimization.scheduler import WeeklyOptimizerScheduler

# No código ou via Python shell
scheduler.run_job_now('generate')  # Gera plano
scheduler.run_job_now('execute')   # Executa plano
scheduler.run_job_now('report')    # Gera relatório
```

### Parar Sistema

Pressione `Ctrl+C` para encerrar graciosamente. O sistema irá:
- Parar threads de monitoramento
- Parar scheduler de otimizações
- Parar API REST
- Exibir estatísticas finais

## API REST

### Endpoints de Dashboard (JSON)

```
GET /api/dashboard/summary              - Métricas principais
GET /api/dashboard/queries?period=24h   - Top queries
GET /api/dashboard/alerts?period=7d     - Alertas recentes
GET /api/dashboard/instances            - Status das instâncias
GET /api/dashboard/trends?days=7        - Dados para gráficos
```

### Endpoints de Otimização (JSON)

```
GET    /api/plans                          - Lista planos
GET    /api/plans/{plan_id}                - Detalhes do plano
POST   /api/plans/{plan_id}/veto           - Veta plano
POST   /api/plans/{plan_id}/approve        - Aprova plano
POST   /api/plans/{plan_id}/items/{id}/veto - Veta item específico
DELETE /api/plans/{plan_id}/items/{id}/veto - Remove veto de item
```

### Endpoints Web (HTML)

```
GET /                                   - Dashboard principal
GET /dashboard/queries                  - Top queries
GET /dashboard/alerts                   - Timeline de alertas
GET /dashboard/instances                - Status das instâncias
GET /dashboard/trends                   - Gráficos de tendências
GET /plans                              - Lista de planos
GET /plans/{plan_id}                    - Detalhes e gerenciamento
```

## Estrutura de Dados (DuckDB)

O sistema armazena métricas em DuckDB com as seguintes tabelas:

### query_stats
Métricas de queries capturadas:
- query_hash, query_text, instance_name
- avg_cpu_time_ms, avg_duration_ms, avg_logical_reads
- occurrences, captured_at
- Retenção: 30 dias

### llm_analysis_results
Resultados de análises LLM:
- query_hash, instance_name
- analysis_text, recommendations, priority
- analyzed_at
- Retenção: 30 dias

### query_cache
Cache de análises para evitar duplicação:
- query_signature, cached_at
- analysis_result, hit_count
- TTL: 24 horas deslizante

### alerts
Alertas gerados por thresholds:
- alert_type, severity, instance_name
- threshold_value, actual_value
- message, triggered_at
- Retenção: 30 dias

### monitoring_cycles
Histórico de ciclos de monitoramento:
- instance_name, cycle_started_at, cycle_ended_at
- queries_found, queries_analyzed, cache_hits
- errors_count, status

### table_metadata
Metadados de tabelas monitoradas:
- instance_name, schema_name, table_name
- row_count, total_size_mb, index_count
- last_updated

### optimization_executions
Execuções de otimizações:
- plan_id, optimization_id, instance_name
- status, duration_seconds
- metrics_before_json, metrics_after_json
- improvement_percent, degradation_percent
- rolled_back, rollback_reason

## Arquitetura

```
sql_monitor/
├── core/                       # Classes base abstratas
│   ├── base_connection.py      # ABC para conexões
│   ├── base_collector.py       # ABC para collectors
│   ├── base_extractor.py       # ABC para extractors
│   └── database_types.py       # Enum de tipos de banco
├── connections/                # Implementações de conexão
│   ├── sqlserver_connection.py
│   └── postgresql_connection.py
├── collectors/                 # Implementações de collector
│   ├── sqlserver_collector.py
│   └── postgresql_collector.py
├── extractors/                 # Implementações de extractor
│   ├── sqlserver_extractor.py
│   └── postgresql_extractor.py
├── connectors/                 # Conectores específicos
│   ├── base_connector.py
│   ├── sqlserver_connector.py
│   └── postgresql_connector.py
├── monitor/                    # Orquestração
│   ├── database_monitor.py     # Monitor individual
│   └── multi_monitor.py        # Orquestrador multi-database
├── optimization/               # Sistema de otimização semanal
│   ├── weekly_planner.py       # Geração de planos
│   ├── risk_classifier.py      # Classificação de risco
│   ├── approval_engine.py      # Auto-aprovação
│   ├── veto_system.py          # Sistema de veto
│   ├── plan_state.py           # Gerenciamento de estado
│   ├── executor.py             # Execução segura
│   ├── impact_analyzer.py      # Análise de ROI
│   └── scheduler.py            # Agendamento semanal
├── api/                        # API REST e Dashboard Web
│   ├── app.py                  # Aplicação FastAPI
│   ├── routes.py               # Endpoints REST/HTML
│   ├── models.py               # Schemas Pydantic
│   ├── templates/              # Templates HTML
│   │   ├── base.html
│   │   ├── dashboard_home.html
│   │   ├── dashboard_queries.html
│   │   ├── dashboard_alerts.html
│   │   ├── dashboard_instances.html
│   │   ├── dashboard_trends.html
│   │   ├── plan_list.html
│   │   └── plan_detail.html
│   └── static/                 # Assets estáticos
│       ├── style.css
│       └── app.js
└── utils/                      # Utilitários compartilhados
    ├── llm_analyzer.py         # Análise via Gemini
    ├── query_sanitizer.py      # Sanitização de queries
    ├── query_cache.py          # Cache de análises
    ├── teams_notifier.py       # Notificações Teams
    ├── metrics_store.py        # Store DuckDB
    ├── query_analytics.py      # Análise de métricas
    └── structured_logger.py    # Sistema de logging
```

## Segurança

### Sanitização de Queries
- Valores literais substituídos por placeholders tipados
- Queries sanitizadas antes do envio à LLM
- Logs não contêm dados reais
- Credenciais apenas em variáveis de ambiente

### Exemplo de Sanitização

```sql
-- ANTES (com dados sensíveis):
SELECT * FROM Usuarios WHERE CPF = '12345678900' AND Saldo > 5000

-- DEPOIS (sanitizada):
SELECT * FROM Usuarios WHERE CPF = @p1_VARCHAR AND Saldo > @p2_INT
```

### API REST
- CORS configurável
- Sem autenticação por padrão (adicionar conforme necessidade)
- Validação de entrada via Pydantic
- Rate limiting via configuração

## Configuração Detalhada

### Performance Thresholds

```json
{
  "performance_thresholds": {
    "execution_time_seconds": 30,
    "cpu_time_ms": 10000,
    "logical_reads": 50000,
    "physical_reads": 10000,
    "writes": 5000
  }
}
```

### Rate Limiting (Gemini API)

```json
{
  "llm": {
    "rate_limit": {
      "max_requests_per_day": 1500,
      "max_requests_per_minute": 60,
      "max_requests_per_cycle": 5,
      "min_delay_between_requests": 2
    }
  }
}
```

### Weekly Optimizer Risk Thresholds

```json
{
  "weekly_optimizer": {
    "risk_thresholds": {
      "table_size_gb_medium": 100,
      "table_size_gb_high": 500,
      "table_size_gb_critical": 1000,
      "index_fragmentation_percent": 50,
      "max_execution_time_minutes": 240
    }
  }
}
```

## Troubleshooting

### Erro: "ODBC Driver not found"

**Linux (Ubuntu/Debian):**
```bash
# Instalar driver ODBC 18
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Verificar instalação
odbcinst -q -d
```

**Windows:**
Baixe em: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

### Erro: "GEMINI_API_KEY não encontrada"

1. Obtenha chave gratuita em: https://makersuite.google.com/app/apikey
2. Adicione no `.env`:
   ```
   GEMINI_API_KEY=sua_chave_aqui
   ```

### API REST não inicia

Verifique se FastAPI está instalado:
```bash
pip install fastapi uvicorn jinja2 python-multipart
```

Habilite no config.json:
```json
{
  "weekly_optimizer": {
    "api": {
      "enabled": true,
      "port": 8080
    }
  }
}
```

### DuckDB: Database is locked

DuckDB permite apenas uma conexão de escrita por vez. O sistema usa:
- Connection pooling thread-safe
- Locks para operações de escrita
- Múltiplas conexões read-only

Se persistir, verifique se não há outro processo usando o banco.

### Weekly Optimizer não executa

Verifique:
1. `weekly_optimizer.enabled = true` no config.json
2. Scheduler está rodando (logs mostrarão "Iniciando Weekly Optimizer Scheduler")
3. Horários configurados corretamente
4. Conectores disponíveis (pelo menos 1 banco habilitado)

### Queries não sendo detectadas

- Verifique thresholds (podem estar muito altos)
- Confirme que há queries rodando no momento
- Valide permissões do usuário (acesso a DMVs/system views)
- Reduza interval_seconds para capturar mais frequentemente

## Documentação Adicional

- [WEEKLY_OPTIMIZER.md](WEEKLY_OPTIMIZER.md) - Sistema de otimização semanal completo
- [sql_monitor/api/README.md](sql_monitor/api/README.md) - Documentação da API REST
- [FIXES_SUMMARY.md](FIXES_SUMMARY.md) - Histórico de correções e melhorias

## Licença

Este projeto é de uso livre para fins educacionais e profissionais.

## Contribuições

Sugestões e melhorias são bem-vindas através de pull requests ou issues.

## Suporte

Para dúvidas ou problemas:
- Verifique os logs estruturados no terminal
- Consulte métricas no DuckDB: `logs/metrics.duckdb`
- Acesse dashboard web para diagnóstico visual
- Revise configurações em `config.json` e `config/databases.json`

---

**Sistema completo de monitoramento, observabilidade e otimização automática para bancos de dados relacionais**
