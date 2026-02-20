[Back to README](../README.md) · [Configuracao →](configuration.md)

# Primeiros Passos

## Prerequisitos

| Requisito | Versao minima | Notas |
|-----------|--------------|-------|
| Python | 3.11+ | |
| Google Gemini API Key | — | Para analise LLM (gratuita em makersuite.google.com) |

Os drivers de banco de dados são instalados automaticamente pelo `start.sh` conforme os SGBDs selecionados. Para SQL Server, o script também instala o ODBC Driver 18 via `apt` ou `yum` (requer `sudo`). Veja [Bancos de Dados](databases.md) para detalhes de permissões por SGBD.

## Instalacao

```bash
git clone https://github.com/rafaelfracasso/sql-performance-monitor.git
cd sql-performance-monitor
./start.sh
```

O `start.sh` apresenta um menu para selecionar os SGBDs a monitorar, instala apenas os drivers necessários (incluindo o ODBC Driver 18 para SQL Server via apt/yum), copia os arquivos de configuração de exemplo e verifica se tudo está pronto antes de subir o processo.

Para setup manual sem o script:

```bash
python -m venv .venv
source .venv/bin/activate      # Linux/Mac
.venv\Scripts\activate         # Windows
pip install -r requirements.txt
```

## Configuracao Inicial

### 1. Configurar bancos de dados

```bash
cp config/databases.json.example config/databases.json
```

Editar `config/databases.json` com as credenciais dos bancos a monitorar. Veja [Bancos de Dados](databases.md) para detalhes por tipo de SGBD.

### 2. Configurar variaveis de ambiente

```bash
cp .env.example .env
```

Editar `.env` com:

```env
GEMINI_API_KEY=sua_chave_aqui

# Senhas referenciadas em databases.json
SQL_SERVER_PROD_PASSWORD=senha_aqui
POSTGRESQL_PROD_PASSWORD=senha_aqui
```

Veja [Configuracao](configuration.md) para todas as variaveis disponiveis.

### 3. Configurar o metrics store (opcional)

O `config.json` define onde o DuckDB será criado. Se o arquivo não existir, o monitor usa os valores padrão. Crie apenas se quiser mudar o caminho ou a retenção:

```json
{
  "metrics_store": {
    "db_path": "logs/metrics.duckdb",
    "enable_compression": true,
    "retention_days": 30
  }
}
```

O dashboard e todas as demais configurações (thresholds, LLM, Teams, weekly optimizer) são gerenciados via interface em `/settings` após o primeiro boot.

## Primeiro Uso

```bash
./start.sh
```

Nas execuções seguintes o script pula a instalação de dependências e vai direto para as verificações de configuração antes de subir o monitor. Para iniciar sem o script:

```bash
source .venv/bin/activate
python main.py
```

Saida esperada:

```
✓ Configuracao validada: 1 database(s) configurado(s)
================================================================================
MULTI-DATABASE PERFORMANCE MONITOR
================================================================================
✓ Monitoramento ativo!
  Dashboard: http://0.0.0.0:8080/
  Pressione Ctrl+C para parar e ver estatisticas.
```

Acesse `http://localhost:8080` para o dashboard.

## Verificar Funcionamento

Apos alguns minutos de execucao:

1. Acesse `/dashboard/instances` — deve mostrar o banco configurado como "online"
2. Acesse `/dashboard/queries` — queries capturadas aparecem aqui
3. Acesse `/dashboard/alerts` — alertas de queries lentas (se houver)

## Parar o Monitor

`Ctrl+C` — o monitor exibe estatisticas de coleta e encerra todas as threads corretamente.

## See Also

- [Configuracao](configuration.md) — config.json, databases.json e configuracoes via API
- [Bancos de Dados](databases.md) — detalhes de configuracao por SGBD
- [Dashboard](dashboard.md) — como usar o dashboard
