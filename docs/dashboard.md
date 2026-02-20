[← Bancos de Dados](databases.md) · [Back to README](../README.md) · [Otimizacao →](optimization.md)

# Dashboard

Interface web disponivel em `http://localhost:8080` (porta configuravel em `config.json`).

## Navegacao

O menu lateral exibe todas as paginas disponíveis. Em dispositivos moveis (< 768px), o menu e acessado pelo botao hamburguer no topo.

**Filtro de periodo:** A maioria das paginas tem um seletor de periodo no topo:

| Opcao | Janela de dados |
|-------|----------------|
| 1h | Ultima hora |
| 6h | Ultimas 6 horas |
| 12h | Ultimas 12 horas |
| 24h | Ultimo dia |
| 7d | Ultima semana |
| 30d | Ultimo mes |

---

## Paginas

### Home `/`

Visao geral do ambiente monitorado:

- **Cards de resumo:** total de queries, queries lentas, alertas ativos, instancias monitoradas
- **Top queries:** queries com maior tempo total de execucao no periodo
- **Distribuicao por banco:** proporcao de carga entre os SGBDs monitorados

### Queries `/dashboard/queries`

Lista todas as queries capturadas, agregadas por hash de texto:

- Ordenacao por: tempo total, tempo medio, numero de execucoes, frequencia
- Filtros: tipo de banco, threshold de duracao
- Busca por texto SQL
- Exportacao via `GET /api/queries/export`

**Detalhe de query** `/dashboard/queries/{query_hash}`:

- Texto completo da query (sanitizado)
- Grafico de execucoes ao longo do tempo
- Analise LLM com sugestoes de otimizacao (se configurado)
- Historico de planos de execucao (SQL Server)

### Alertas `/dashboard/alerts`

Queries e eventos que ultrapassaram os thresholds configurados:

- Severidade: low, medium, high, critical
- Filtros por banco e periodo
- Link para detalhe da query alertada

**Detalhe de alerta** `/dashboard/alerts/{alert_id}`:

- Contexto completo do alerta
- Metricas no momento do alerta
- Historico de ocorrencias da mesma query

### Instancias `/dashboard/instances`

Status de saude de cada banco monitorado:

- Estado de conexao (online/offline)
- Metricas de wait stats por categoria
- Utilizacao de recursos (CPU, memoria, I/O quando disponivel)
- Historico de disponibilidade

### Tendencias `/dashboard/trends`

Graficos historicos de metricas agregadas:

- Evolucao de queries por hora/dia
- Crescimento de dados armazenados
- Comparativo de performance entre periodos
- Top queries ao longo do tempo

### Usuarios, Hosts, Aplicacoes

Tres paginas de drilldown com visao por entidade:

- `/dashboard/users` — queries agrupadas por usuario de banco
- `/dashboard/hosts` — queries agrupadas por host de origem
- `/dashboard/applications` — queries agrupadas por nome de aplicacao

Cada pagina exibe:
- Cards de resumo (total de queries, tempo medio, pico)
- Grafico de tendencia por entidade
- Tabela detalhada com link para queries individuais

### LLM `/dashboard/llm`

Historico de analises realizadas pelo Google Gemini:

- Queries analisadas com timestamp
- Resposta completa do LLM
- Custo estimado de tokens por analise

### DuckDB `/dashboard/duckdb`

Informacoes sobre o banco de metricas interno:

- Tamanho do arquivo `.duckdb`
- Contagem de registros por tabela
- Configuracoes de retencao ativas
- Acao de compactacao manual

### Planos `/plans`

Lista de planos de otimizacao gerados pelo Weekly Optimizer:

- Status: pendente, aprovado, executado, vetado
- Impacto estimado e classificacao de risco
- Acoes: aprovar, vetar, executar manualmente

Veja [Otimizacao](optimization.md) para detalhes do motor de planos.

### Configuracoes `/settings`

Painel de configuracao em tempo real. Organizado em abas:

| Aba | O que configura |
|-----|----------------|
| Performance | Thresholds de duracao e frequencia por tipo de banco |
| LLM & Prompts | Modelo, temperatura, prompts por tipo de banco com historico de versoes |
| Notificacoes | Teams webhook, threshold de alertas |
| Infraestrutura | Intervalo de coleta, pool de conexoes, timeouts |
| Weekly Optimizer | Agendamento, horario de execucao, modo de aprovacao |
| Security & Audit | Log de auditoria, configuracoes de cache de queries |

---

## Temas

O botao no canto superior direito alterna entre tema claro e escuro. A preferencia e salva no `localStorage` do navegador.

---

## Notificacoes (Toasts)

Acoes como salvar configuracoes ou gerar planos exibem notificacoes temporarias no canto da tela. Tipos: sucesso (verde), erro (vermelho), aviso (amarelo), info (azul).

## See Also

- [Otimizacao](optimization.md) — como usar os planos gerados pelo dashboard
- [API REST](api.md) — endpoints que alimentam o dashboard
- [Configuracao](configuration.md) — opcoes do config.json que afetam o dashboard
