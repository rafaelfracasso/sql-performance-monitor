# SQL Monitor

> Monitor de performance multi-banco de dados com analise via LLM e dashboard interativo.

Ferramenta interna para monitoramento continuo de queries em SQL Server, PostgreSQL e SAP HANA. Coleta metricas em tempo real, armazena em DuckDB, exibe um dashboard web e utiliza Google Gemini para analise e sugestao de otimizacoes.

## Inicio Rapido

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Configurar bancos de dados
cp config/databases.json.example config/databases.json
# editar config/databases.json com suas credenciais

# 3. Configurar variaveis de ambiente
cp .env.example .env
# editar .env com GEMINI_API_KEY e senhas

# 4. Iniciar o monitor
python main.py
```

Dashboard disponivel em `http://localhost:8080` apos iniciar.

## Recursos

- **Monitoramento multi-SGBD** — SQL Server, PostgreSQL e SAP HANA simultaneamente
- **Dashboard interativo** — queries, alertas, usuarios, hosts, aplicacoes e tendencias
- **Analise LLM** — Google Gemini analisa queries e sugere otimizacoes
- **Motor de otimizacao** — planos semanais com workflow de aprovacao e execucao agendada
- **Alertas** — deteccao de queries lentas e anomalias com notificacao via Teams
- **Historico** — metricas armazenadas em DuckDB com retencao configuravel

## Paginas do Dashboard

| Pagina | URL | Descricao |
|--------|-----|-----------|
| Home | `/` | Resumo geral e top queries |
| Queries | `/dashboard/queries` | Lista e analise de queries |
| Alertas | `/dashboard/alerts` | Alertas ativos e historico |
| Instancias | `/dashboard/instances` | Status de cada banco |
| Tendencias | `/dashboard/trends` | Graficos historicos |
| Planos | `/plans` | Planos de otimizacao |
| Configuracoes | `/settings` | Ajustes do monitor |

## Documentacao

| Guia | Descricao |
|------|-----------|
| [Primeiros Passos](docs/getting-started.md) | Instalacao, prerequisitos e primeiro uso |
| [Configuracao](docs/configuration.md) | config.json, databases.json e variaveis de ambiente |
| [Bancos de Dados](docs/databases.md) | Como configurar cada SGBD |
| [Dashboard](docs/dashboard.md) | Paginas, filtros e como interpretar os dados |
| [Otimizacao](docs/optimization.md) | Motor de otimizacao automatica e planos semanais |
| [API REST](docs/api.md) | Referencia dos endpoints da API |

## Licenca

MIT — veja [LICENSE](LICENSE) para detalhes.
