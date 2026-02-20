[← Dashboard](dashboard.md) · [Back to README](../README.md) · [API REST →](api.md)

# Motor de Otimizacao

O SQL Monitor inclui um motor de otimizacao automatica que analisa metricas historicas, gera planos de melhoria e pode executar scripts SQL de forma agendada e controlada.

## Visao Geral

```
Metricas DuckDB
      |
      v
WeeklyOptimizationPlanner  (analisa historico, identifica candidatos)
      |
      v
ImpactAnalyzer + RiskClassifier  (estima ROI e classifica risco)
      |
      v
ApprovalEngine  (aguarda aprovacao manual ou aprova automaticamente)
      |
      v
Executor  (executa scripts no banco de dados)
```

## Habilitando o Optimizer

O Weekly Optimizer é configurado via dashboard, sem edição de arquivos. Acesse `/settings` → aba **Weekly Optimizer** e habilite o toggle **Enabled**.

As principais configurações:

| Campo | Padrão | Descricao |
|-------|--------|-----------|
| `enabled` | `false` | Habilita o motor de otimizacao semanal |
| `analysis.min_occurrences` | `10` | Mínimo de execucoes para a query ser candidata |
| `analysis.min_avg_duration_ms` | `1000` | Duração média mínima em ms para ser candidata |
| `analysis.days` | `7` | Janela de análise histórica em dias |
| `schedule.execution_day` | `sunday` | Dia da semana para execucao dos planos |
| `schedule.execution_time` | `02:00` | Horário de execucao |
| `auto_rollback.enabled` | `true` | Reverte automaticamente se houver degradacao |

Veja todos os campos em [Configuracao](configuration.md#weekly-optimizer).

Via API:

```
GET  /api/settings/weekly_optimizer
POST /api/settings/weekly_optimizer
```

## O que o Planner Analisa

O `WeeklyOptimizationPlanner` roda uma vez por semana e identifica:

| Tipo de oportunidade | Criterio |
|----------------------|---------|
| Queries frequentes e lentas | > `min_occurrences` execucoes + > `min_avg_duration_ms` ms |
| Missing indexes | Padroes de acesso sem indice disponivel |
| Reescrita de queries | Subqueries, JOINs ineficientes detectados via LLM |
| Estatisticas desatualizadas | Tabelas com estatisticas antigas |
| Manutencao (REBUILD/REORG/VACUUM) | Fragmentacao detectada |

## Fluxo de um Plano

### 1. Geracao

O planner cria um plano com:
- Lista de acoes ordenadas por impacto estimado
- Scripts SQL prontos para execucao
- Estimativa de melhoria de performance (ROI)
- Classificacao de risco: `low`, `medium`, `high`, `critical`

### 2. Revisao (se `auto_approve: false`)

Acesse `/plans` no dashboard para ver planos pendentes. Para cada plano:

- **Aprovar** — coloca na fila de execucao agendada
- **Vetar** — cancela o plano com justificativa (registrado em auditoria)
- **Ver detalhes** — exibe scripts SQL, impacto estimado e historico da query

### 3. Execucao

Planos aprovados sao executados pelo `Executor` dentro da janela configurada (`execution_window`). O `VetoSystem` pode bloquear a execucao se detectar condicoes de risco (ex: banco sob carga alta).

Apos execucao:
- Status atualizado para `executado`
- Metricas de before/after coletadas para validacao de impacto
- Notificacao via Teams (se configurado)

## Classificacao de Risco

| Nivel | O que significa | Comportamento padrao |
|-------|----------------|----------------------|
| `low` | Scripts DDL/DML seguros, impacto minimo | Pode ser auto-executado |
| `medium` | Operacoes que bloqueiam brevemente | Requer aprovacao manual |
| `high` | Operacoes longas ou com bloqueio significativo | Requer aprovacao + janela de manutencao |
| `critical` | Risco de perda de dados ou indisponibilidade | Bloqueado pelo VetoSystem |

## API de Planos

| Endpoint | Descricao |
|----------|-----------|
| `GET /api/plans` | Lista todos os planos |
| `GET /api/plans/{id}` | Detalhe de um plano |
| `POST /api/plans/generate` | Gera novo plano manualmente |
| `GET /api/plans/{id}/status` | Status atual do plano |
| `DELETE /api/plans/{id}` | Remove um plano |

Veja [API REST](api.md) para parametros e exemplos.

## Auditoria

Todas as acoes (geracao, aprovacao, veto, execucao) sao registradas em log de auditoria acessivel em `/api/settings/audit`. O log inclui timestamp, usuario (quando disponivel), acao e resultado.

## See Also

- [Dashboard](dashboard.md) — pagina `/plans` para gestao de planos
- [Configuracao](configuration.md) — todas as opcoes do weekly_optimizer
- [API REST](api.md) — endpoints de planos e settings
