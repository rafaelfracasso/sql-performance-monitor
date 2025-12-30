# SQL Monitor - API REST e Dashboard

API REST com interface web para visualização de métricas e gerenciamento de planos de otimização.

## Visão Geral

A API fornece:
- Dashboard interativo com métricas em tempo real
- Visualização de queries problemáticas
- Monitoramento de alertas e hotspots
- Gerenciamento de planos de otimização semanal
- Endpoints REST para integração

## Estrutura

```
sql_monitor/api/
├── app.py                 # Aplicação FastAPI principal
├── routes.py              # Endpoints REST e HTML
├── models.py              # Schemas Pydantic
├── templates/             # Templates Jinja2
│   ├── base.html
│   ├── dashboard_home.html
│   ├── dashboard_queries.html
│   ├── dashboard_alerts.html
│   ├── dashboard_instances.html
│   ├── dashboard_trends.html
│   ├── plan_list.html
│   └── plan_detail.html
└── static/                # Assets estáticos
    ├── style.css
    └── app.js
```

## Instalação

1. Instalar dependências:
```bash
pip install -r requirements.txt
```

2. Configurar variáveis de ambiente (opcional):
```bash
export API_HOST=0.0.0.0
export API_PORT=8080
export DUCKDB_PATH=sql_monitor_data/metrics.duckdb
```

## Execução

### Método 1: Script de inicialização
```bash
python run_api.py
```

### Método 2: Uvicorn direto
```bash
uvicorn sql_monitor.api.app:app --host 0.0.0.0 --port 8080
```

### Método 3: Integrado no main.py
```python
from sql_monitor.api.app import app
from sql_monitor.api.routes import init_dependencies
import uvicorn
import threading

# Inicializar dependências
init_dependencies(metrics_store)

# Rodar em thread separada
api_thread = threading.Thread(
    target=lambda: uvicorn.run(app, host="0.0.0.0", port=8080),
    daemon=True
)
api_thread.start()
```

## Endpoints

### Páginas HTML (Web Interface)

| URL | Descrição |
|-----|-----------|
| `/` | Dashboard principal |
| `/dashboard/queries` | Top queries problemáticas |
| `/dashboard/alerts` | Alertas de performance |
| `/dashboard/instances` | Status das instâncias |
| `/dashboard/trends` | Gráficos de tendências |
| `/plans` | Lista de planos de otimização |
| `/plans/{plan_id}` | Detalhes do plano |

### Endpoints REST (JSON)

| Método | URL | Descrição |
|--------|-----|-----------|
| GET | `/api/health` | Health check |
| GET | `/api/dashboard/summary` | Resumo de métricas |
| GET | `/api/dashboard/queries` | Top queries |
| GET | `/api/dashboard/alerts` | Alertas recentes |
| GET | `/api/dashboard/instances` | Status das instâncias |
| GET | `/api/dashboard/trends` | Dados de tendências |
| GET | `/api/dashboard/cache-efficiency` | Eficiência do cache |
| GET | `/api/plans` | Lista de planos |
| GET | `/api/plans/{plan_id}` | Detalhes do plano |
| GET | `/api/plans/{plan_id}/status` | Status do plano |
| POST | `/api/plans/{plan_id}/veto` | Vetar plano |
| POST | `/api/plans/{plan_id}/approve` | Aprovar plano |
| POST | `/api/plans/{plan_id}/items/{item_id}/veto` | Vetar item |
| DELETE | `/api/plans/{plan_id}/items/{item_id}/veto` | Remover veto |

## Exemplos de Uso

### Dashboard Principal
```bash
# Acessar no navegador
http://localhost:8080/
```

### API - Resumo de Métricas
```bash
curl http://localhost:8080/api/dashboard/summary?hours=24
```

### API - Top Queries
```bash
curl "http://localhost:8080/api/dashboard/queries?period=24h&metric=cpu_time_ms&limit=10"
```

### API - Alertas
```bash
curl "http://localhost:8080/api/dashboard/alerts?period=7d&severity=high"
```

### API - Vetar Plano
```bash
curl -X POST http://localhost:8080/api/plans/20251229_180000/veto \
  -H "Content-Type: application/json" \
  -d '{
    "reason": "Necessita mais análise antes da execução",
    "vetoed_by": "dba@empresa.com"
  }'
```

## Integração com Teams

A API pode ser acessada via links enviados pelo Teams:

1. Sistema gera plano semanal
2. Teams envia Adaptive Card com link para a API
3. DBA clica no link e abre a interface web
4. DBA pode vetar/aprovar via interface web
5. Sistema envia confirmação via Teams

## Configuração

A API lê configurações de:
- Variáveis de ambiente
- Arquivo `config.json` (quando integrado ao sistema principal)

Exemplo de configuração no `config.json`:
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

## Desenvolvimento

### Estrutura de Templates

Todos os templates estendem `base.html`:
```html
{% extends "base.html" %}

{% block content %}
<!-- Conteúdo da página -->
{% endblock %}

{% block extra_scripts %}
<!-- JavaScript adicional -->
{% endblock %}
```

### Adicionando Novos Endpoints

1. Adicionar schema no `models.py`:
```python
class NovoModel(BaseModel):
    campo: str
```

2. Adicionar rota no `routes.py`:
```python
@router.get("/api/novo-endpoint")
async def novo_endpoint():
    return {"resultado": "dados"}
```

3. Criar template se necessário em `templates/`

### Customização de CSS

Editar `static/style.css`:
```css
:root {
    --primary-color: #2563eb;
    --success-color: #10b981;
    /* ... */
}
```

## Troubleshooting

### Erro: "Analytics não inicializado"
Certifique-se de que `init_dependencies(metrics_store)` foi chamado antes de usar as rotas.

### Erro: "Template not found"
Verifique se os diretórios `templates/` e `static/` existem e contêm os arquivos necessários.

### Porta já em uso
Altere a porta com variável de ambiente:
```bash
export API_PORT=8081
python run_api.py
```

### CORS errors
Ajuste configuração CORS em `app.py` se necessário.

## Próximas Fases

### Fase 2: Sistema de Otimização
- Implementar RiskClassifier
- Criar AutoApprovalEngine
- Implementar VetoSystem completo
- Integrar com planos de otimização

### Fase 3: Execução e Análise
- OptimizationExecutor com rollback
- ImpactAnalyzer para ROI
- Dashboards avançados

### Fase 4: Automação
- Scheduler completo (quinta/domingo/segunda)
- Adaptive Cards para Teams
- Documentação completa

## Licença

Interno - Uso restrito à organização.
