# Weekly Optimization System - Documentação Completa

Sistema automatizado de otimização semanal de banco de dados com aprovação automática, veto granular e rollback inteligente.

## Visão Geral

O Weekly Optimizer é um sistema end-to-end que:
1. Analisa histórico de queries do DuckDB
2. Gera plano de otimização semanal (índices, estatísticas, manutenção)
3. Classifica risco automaticamente (LOW/MEDIUM/HIGH/CRITICAL)
4. Permite veto granular via API REST
5. Executa automaticamente com rollback se degradação >20%
6. Gera relatório de ROI real vs estimado

## Cronograma de Execução

```
Quinta-feira 18:00
├─ Gera plano semanal
├─ Classifica riscos
├─ Auto-aprova baseado em risco
└─ Notifica Teams com link para revisão

Quinta - Domingo (72h)
└─ Janela de veto via API REST
   ├─ DBA pode vetar plano completo
   ├─ DBA pode vetar itens específicos
   └─ Veto granular com auditoria

Domingo 02:00
├─ Verifica se há vetos ativos
├─ Executa otimizações não vetadas
├─ Captura métricas antes/depois
├─ Rollback automático se degradação >20%
└─ Notifica Teams com resultados

Segunda 08:00
├─ Compara métricas 7d antes vs 7d depois
├─ Calcula ROI real
├─ Gera relatório executivo
└─ Notifica Teams com análise completa
```

## Arquitetura de Componentes

### 1. RiskClassifier

Classifica otimizações em 4 níveis de risco:

**LOW** (Auto-aprova sem notificação):
- UPDATE STATISTICS
- VACUUM ANALYZE normal
- REORGANIZE INDEX
- DELTA MERGE (HANA) em tabelas pequenas

**MEDIUM** (Auto-aprova com notificação):
- CREATE INDEX em tabelas <100GB com ONLINE/CONCURRENTLY
- REBUILD INDEX (fragmentação >50%)
- Query rewrite suggestions (não executável)

**HIGH** (Auto-aprova com janela de veto 72h):
- CREATE INDEX em tabelas 100-500GB
- CREATE INDEX sem ONLINE/CONCURRENTLY
- VACUUM FULL (PostgreSQL)
- ALTER TABLE não destrutivo

**CRITICAL** (Bloqueado, requer aprovação manual):
- CREATE INDEX em tabelas >500GB
- DROP INDEX/TABLE
- ALTER TABLE destrutivo
- Operações em produção crítica

**Arquivo**: `sql_monitor/optimization/risk_classifier.py`

### 2. AutoApprovalEngine

Motor de decisão automática baseado em nível de risco.

**Regras de Aprovação**:
```python
{
    LOW: {
        'auto_approve': True,
        'notify': False,
        'veto_window': False,
        'execute_immediately': True
    },
    MEDIUM: {
        'auto_approve': True,
        'notify': True,
        'veto_window': False,
        'execute_immediately': True
    },
    HIGH: {
        'auto_approve': True,
        'notify': True,
        'veto_window': True,
        'execute_immediately': False  # Aguarda 72h
    },
    CRITICAL: {
        'auto_approve': False,
        'notify': True,
        'veto_window': True,
        'execute_immediately': False
    }
}
```

**Funcionalidades**:
- Calcula janela de execução (próximo domingo 02:00)
- Gera resumo para notificação Teams
- Estatísticas de aprovação por nível de risco

**Arquivo**: `sql_monitor/optimization/approval_engine.py`

### 3. VetoSystem

Sistema de veto granular com persistência em JSON.

**Tipos de Veto**:
- **Completo**: Veta plano inteiro
- **Parcial**: Veta itens específicos

**Estrutura de Veto**:
```json
{
  "veto_id": "20251229_180000_complete",
  "plan_id": "20251229_180000",
  "veto_type": "complete",
  "vetoed_at": "2025-12-27T14:30:00",
  "vetoed_by": "dba@empresa.com",
  "veto_reason": "Necessita mais análise",
  "vetoed_items": [],
  "veto_expires_at": "2025-12-29T02:00:00",
  "active": true
}
```

**APIs REST**:
```bash
# Vetar plano completo
POST /api/plans/{plan_id}/veto
{
  "reason": "Necessita mais análise antes da execução",
  "vetoed_by": "dba@empresa.com"
}

# Vetar item específico
POST /api/plans/{plan_id}/items/{item_id}/veto
{
  "reason": "Índice pode causar lock em horário comercial",
  "vetoed_by": "dba@empresa.com"
}

# Remover veto de item
DELETE /api/plans/{plan_id}/items/{item_id}/veto
```

**Arquivo**: `sql_monitor/optimization/veto_system.py`
**Armazenamento**: `sql_monitor_data/vetos.json`

### 4. PlanStateManager

Gerenciador de estado de planos com persistência em disco.

**Estrutura de Plano**:
```python
OptimizationPlan:
  - plan_id: str
  - generated_at: datetime
  - execution_scheduled_at: datetime
  - analysis_period_days: int
  - status: str  # pending, approved, vetoed, executing, completed
  - veto_window_expires_at: datetime
  - total_optimizations: int
  - auto_approved_count: int
  - requires_review_count: int
  - blocked_count: int
  - optimizations: List[OptimizationItem]
```

**Funcionalidades**:
- Cache em memória para performance
- Sincronização com VetoSystem
- Filtros e listagens
- Atualização de status granular
- Cleanup de planos antigos (90+ dias)

**Arquivo**: `sql_monitor/optimization/plan_state.py`
**Armazenamento**: `sql_monitor_data/plans/*.json`

### 5. OptimizationExecutor

Executor seguro com rollback automático.

**Fluxo de Execução**:
1. Captura métricas baseline (1h antes)
2. Executa SQL DDL
3. Aguarda estabilização (10 minutos)
4. Captura métricas pós-execução
5. Calcula impacto (melhoria vs degradação)
6. Se degradação >20%, executa rollback
7. Registra tudo no DuckDB

**Proteções**:
- Timeout configurável (padrão: 4 horas)
- Intervalo de 5 minutos entre otimizações
- Rollback automático se degradação >20%
- Dry-run mode para testes
- Execução por prioridade (critical → high → medium → low)

**Configuração**:
```json
{
  "auto_rollback": {
    "enabled": true,
    "degradation_threshold_percent": 20,
    "wait_after_execution_minutes": 10
  },
  "max_execution_time_minutes": 240
}
```

**Arquivo**: `sql_monitor/optimization/executor.py`

### 6. ImpactAnalyzer

Analisador de ROI e impacto real.

**Métricas Calculadas**:
- Comparação 7 dias antes vs 7 dias depois
- CPU reduction %
- Duration reduction %
- Logical reads reduction %
- Payback period em dias
- Top 5 melhorias
- Top 5 regressões (não revertidas)

**Exemplo de Relatório**:
```
========================================
RELATÓRIO DE IMPACTO - PLANO 20251229_180000
========================================

Data de Execução: 29/12/2024 02:00

RESUMO:
-------
- Total de Otimizações: 15
- Bem-sucedidas: 13 (86.7%)
- Falhadas: 1
- Revertidas (Rollback): 1

IMPACTO:
--------
- Melhoria Total: 45.3%
- Degradação Total: 5.2%
- Impacto Líquido: 40.1%

ROI:
----
- Tempo de Execução: 3.5 horas
- Economia por Dia: 2.1 horas
- Payback: 1.7 dias
```

**Arquivo**: `sql_monitor/optimization/impact_analyzer.py`

### 7. WeeklyOptimizerScheduler

Agendador central que integra todos os componentes.

**Jobs Agendados**:
```python
# Quinta-feira 18:00
schedule.every().thursday.at("18:00").do(job_generate_plan)

# Domingo 02:00
schedule.every().sunday.at("02:00").do(job_execute_plan)

# Segunda 08:00
schedule.every().monday.at("08:00").do(job_generate_report)
```

**Funcionalidades**:
- Execução em thread separada
- Notificação Teams em cada etapa
- Tratamento de erros robusto
- Execução manual de jobs (para testes)

**Arquivo**: `sql_monitor/optimization/scheduler.py`

## Schema DuckDB

Nova tabela `optimization_executions`:

```sql
CREATE TABLE IF NOT EXISTS optimization_executions (
    id INTEGER PRIMARY KEY,
    executed_at TIMESTAMP NOT NULL,
    plan_id VARCHAR(50) NOT NULL,
    optimization_id VARCHAR(50) NOT NULL,
    instance_name VARCHAR(100),

    -- Execução
    status VARCHAR(20),  -- success, failed, rolled_back, error
    duration_seconds DOUBLE,
    error_message TEXT,

    -- Métricas antes e depois
    metrics_before_json TEXT,
    metrics_after_json TEXT,

    -- Impacto
    improvement_percent DOUBLE,
    degradation_percent DOUBLE,

    -- Rollback
    rolled_back BOOLEAN DEFAULT FALSE,
    rollback_reason TEXT,

    -- Auditoria
    executed_by VARCHAR(100),
    approved_by VARCHAR(100),

    INDEX idx_plan_id_exec (plan_id),
    INDEX idx_executed_at_exec (executed_at)
);
```

## API REST

### Dashboard (HTML)

| URL | Descrição |
|-----|-----------|
| `/` | Dashboard principal com métricas |
| `/dashboard/queries` | Top queries problemáticas |
| `/dashboard/alerts` | Alertas de performance |
| `/dashboard/instances` | Status das instâncias |
| `/dashboard/trends` | Gráficos de tendências |
| `/plans` | Lista de planos de otimização |
| `/plans/{plan_id}` | Detalhes e gerenciamento do plano |

### Endpoints REST (JSON)

**Planos**:
```bash
GET  /api/plans                        # Lista planos
GET  /api/plans/{plan_id}              # Detalhes do plano
GET  /api/plans/{plan_id}/status       # Status em tempo real
POST /api/plans/{plan_id}/veto         # Veta plano completo
POST /api/plans/{plan_id}/approve      # Aprova plano
POST /api/plans/{plan_id}/items/{item_id}/veto  # Veta item
DELETE /api/plans/{plan_id}/items/{item_id}/veto # Remove veto
```

**Dashboard**:
```bash
GET /api/dashboard/summary             # Métricas principais
GET /api/dashboard/queries             # Top queries
GET /api/dashboard/alerts              # Alertas
GET /api/dashboard/instances           # Status instâncias
GET /api/dashboard/trends              # Dados de gráficos
GET /api/dashboard/cache-efficiency    # Eficiência do cache
```

## Integração com Teams

Sistema envia 3 tipos de Adaptive Cards:

### 1. Plano Gerado (Quinta 18:00)

```
┌─────────────────────────────────────┐
│ Plano Semanal de Otimização Gerado │
│                                     │
│ Plano ID: 20251229_180000           │
│                                     │
│ Gerado em: 29/12/2024 18:00         │
│ Execução: 01/01/2025 02:00          │
│ Veto expira: 01/01/2025 02:00       │
│                                     │
│ Total: 15 otimizações               │
│ Auto-aprovadas: 12                  │
│ Requer revisão: 2                   │
│ Bloqueadas: 1                       │
│                                     │
│ Você tem 72 horas para revisar      │
│                                     │
│ [Ver e Gerenciar Plano]             │
└─────────────────────────────────────┘
```

### 2. Plano Executado (Domingo 02:00)

```
┌─────────────────────────────────────┐
│ Plano de Otimização Executado    │
│                                     │
│ Executado em: 01/01/2025 04:30      │
│                                     │
│ Executadas: 14                      │
│ Bem-sucedidas: 13                   │
│ Falhadas: 0                         │
│ Revertidas: 1                       │
│ Taxa de sucesso: 92.9%              │
│                                     │
│ Relatório detalhado: Segunda 08:00  │
│                                     │
│ [Ver Detalhes da Execução]          │
└─────────────────────────────────────┘
```

### 3. Relatório de Impacto (Segunda 08:00)

```
┌─────────────────────────────────────┐
│ Relatório de Impacto Semanal     │
│                                     │
│ Taxa de sucesso: 92.9%              │
│                                     │
│ IMPACTO:                            │
│ Melhoria total: 45.3%               │
│ Degradação total: 5.2%              │
│ Impacto líquido: 40.1%              │
│                                     │
│ ROI:                                │
│ CPU Reduction: 38.5%                │
│ Duration Reduction: 42.1%           │
│ Payback: 2 dias                     │
│                                     │
│ RECOMENDAÇÕES:                      │
│ • Excelente ROI, continuar          │
│ • 1 degradação não revertida        │
│                                     │
│ [Ver Relatório Completo]            │
└─────────────────────────────────────┘
```

## Configuração

Arquivo `config.json`:

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
      "hours": 72,
      "check_before_execution": true
    },
    "risk_thresholds": {
      "table_size_gb_medium": 100,
      "table_size_gb_high": 500,
      "table_size_gb_critical": 1000,
      "index_fragmentation_percent": 50,
      "max_execution_time_minutes": 240
    },
    "auto_rollback": {
      "enabled": true,
      "degradation_threshold_percent": 20,
      "wait_after_execution_minutes": 10
    },
    "api": {
      "enabled": true,
      "host": "0.0.0.0",
      "port": 8080,
      "cors_enabled": true
    },
    "analysis": {
      "days": 7,
      "min_occurrences": 10,
      "min_avg_duration_ms": 1000
    }
  }
}
```

## Instalação e Setup

### 1. Dependências

```bash
pip install -r requirements.txt
```

Novas dependências adicionadas:
- `fastapi>=0.104.0` - API REST
- `uvicorn[standard]>=0.24.0` - ASGI server
- `jinja2>=3.1.0` - Template engine
- `python-multipart>=0.0.6` - Form data
- `schedule>=1.2.0` - Job scheduling (já existia)

### 2. Estrutura de Diretórios

O sistema cria automaticamente:

```
sql_monitor_data/
├── metrics.duckdb          # Métricas e histórico
├── vetos.json              # Vetos ativos
└── plans/                  # Planos gerados
    ├── 20251229_180000.json
    ├── 20260105_180000.json
    └── ...
```

### 3. Inicialização

**Opção 1: Integrado no main.py**

```python
from sql_monitor.optimization.scheduler import WeeklyOptimizerScheduler
from sql_monitor.api.app import app
from sql_monitor.api.routes import init_dependencies
import uvicorn
import threading

# Inicializar dependências da API
init_dependencies(metrics_store)

# Inicializar scheduler
scheduler = WeeklyOptimizerScheduler(
    metrics_store=metrics_store,
    connectors=connectors,  # Dict de BaseConnector
    teams_notifier=teams_notifier,
    config=config.get('weekly_optimizer', {})
)

# Iniciar scheduler
scheduler.start()

# Iniciar API em thread separada
api_thread = threading.Thread(
    target=lambda: uvicorn.run(app, host="0.0.0.0", port=8080),
    daemon=True
)
api_thread.start()

# ... resto do código
```

**Opção 2: API standalone**

```bash
python run_api.py
```

## Uso Avançado

### Executar Jobs Manualmente (Testes)

```python
# Gerar plano manualmente
scheduler.run_job_now('generate')

# Executar plano manualmente
scheduler.run_job_now('execute')

# Gerar relatório manualmente
scheduler.run_job_now('report')
```

### Dry-Run (Simular Execução)

```python
executor = OptimizationExecutor(
    metrics_store=metrics_store,
    plan_state_manager=plan_state_manager,
    config=config
)

# Simular sem executar SQL real
result = executor.execute_plan(
    plan_id='20251229_180000',
    connector=sql_server_connector,
    dry_run=True  # Não executa SQL
)
```

### Análise de Impacto Customizada

```python
analyzer = ImpactAnalyzer(metrics_store)

# Análise com período customizado
report = analyzer.analyze_plan_impact(
    plan_id='20251229_180000',
    days_before=14,  # 2 semanas antes
    days_after=14    # 2 semanas depois
)

# Resumo executivo
summary = analyzer.generate_executive_summary('20251229_180000')
print(summary)
```

## Segurança e Validações

### Pré-Execução
- Verificar conexões com todos os bancos
- Validar espaço em disco (mínimo configurável)
- Confirmar que não há vetos ativos
- Capturar baseline de métricas
- Verificar janela de veto expirada

### Durante Execução
- Timeout de segurança por otimização
- Intervalo de 5 minutos entre otimizações
- Monitoramento de métricas em tempo real
- Log detalhado de cada passo
- Execução por ordem de prioridade

### Pós-Execução
- Comparar métricas antes/depois
- Rollback automático se degradação >20%
- Validar integridade de dados (via connector)
- Notificar Teams com resultado
- Registro permanente no DuckDB

## Troubleshooting

### Problema: Plano não é executado no domingo

**Verificações**:
1. Scheduler está rodando? `scheduler._running == True`
2. Plano está com status 'pending'?
3. Janela de veto expirou?
4. Plano foi vetado?

**Debug**:
```python
# Verificar status do plano
plan = plan_state_manager.get_plan('20251229_180000', sync_vetos=True)
print(f"Status: {plan.status}")
print(f"Veto expires: {plan.veto_window_expires_at}")
print(f"Vetado: {veto_system.is_plan_vetoed(plan.plan_id)}")
```

### Problema: Rollback não funcionou

**Verificações**:
1. Auto-rollback está habilitado?
2. Otimização tem rollback_script?
3. Degradação excedeu threshold?

**Debug**:
```python
# Ver histórico de execução
executions = metrics_store.get_execution_history(plan_id='20251229_180000')
for exec in executions:
    print(f"{exec['optimization_id']}: {exec['status']}")
    if exec['rolled_back']:
        print(f"  Rollback reason: {exec['rollback_reason']}")
```

### Problema: API não inicializa

**Verificações**:
1. Port 8080 está livre?
2. Dependências instaladas (fastapi, uvicorn)?
3. `init_dependencies()` foi chamado?

**Debug**:
```bash
# Verificar porta
lsof -i :8080

# Testar API manualmente
python run_api.py

# Verificar logs
tail -f logs/sql_monitor.log
```

## Métricas e Monitoramento

### Estatísticas do Sistema

```python
# Estatísticas de vetos
stats = veto_system.get_statistics()
# {'total_vetos': 10, 'active_vetos': 2, 'complete_vetos': 1, ...}

# Estatísticas de notificações Teams
stats = teams_notifier.get_statistics()
# {'total_sent': 45, 'total_failed': 2, 'success_rate': 95.7}

# Histórico de execuções
executions = metrics_store.get_execution_history(limit=100)
success_rate = sum(1 for e in executions if e['status'] == 'success') / len(executions) * 100
```

### Queries Úteis (DuckDB)

```sql
-- Execuções com maior impacto
SELECT
    plan_id,
    optimization_id,
    improvement_percent,
    degradation_percent,
    status
FROM optimization_executions
WHERE improvement_percent > 30
ORDER BY improvement_percent DESC
LIMIT 10;

-- Taxa de rollback por plano
SELECT
    plan_id,
    COUNT(*) as total,
    SUM(CASE WHEN rolled_back THEN 1 ELSE 0 END) as rolled_back,
    AVG(improvement_percent) as avg_improvement
FROM optimization_executions
GROUP BY plan_id
ORDER BY plan_id DESC;

-- Otimizações mais problemáticas
SELECT
    optimization_id,
    COUNT(*) as executions,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failures,
    SUM(CASE WHEN rolled_back THEN 1 ELSE 0 END) as rollbacks
FROM optimization_executions
GROUP BY optimization_id
HAVING failures > 0 OR rollbacks > 0
ORDER BY (failures + rollbacks) DESC;
```

## Roadmap Futuro

### v2.0 - Machine Learning
- Predição de impacto baseada em histórico
- Ajuste automático de thresholds de risco
- Detecção de anomalias em métricas

### v2.1 - Multi-Region
- Execução coordenada em múltiplas regiões
- Rollback global se falha em qualquer região

### v2.2 - Análise Avançada
- Comparação de planos (qual teve melhor resultado)
- Sugestão de otimizações baseada em padrões
- Alertas proativos de regressão

## Suporte e Contribuição

Para dúvidas, sugestões ou bugs:
- GitHub Issues: https://github.com/your-org/sql-monitor/issues
- Documentação: https://docs.sql-monitor.local
- Email: dba-team@empresa.com

---

**Versão**: 1.0.0
**Última atualização**: Dezembro 2024
**Autores**: DBA Team + Claude Sonnet 4.5
