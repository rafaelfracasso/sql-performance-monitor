[← Configuracao](configuration.md) · [Back to README](../README.md) · [Dashboard →](dashboard.md)

# Bancos de Dados

Guia de configuracao para cada tipo de SGBD suportado: SQL Server, PostgreSQL e SAP HANA.

## SQL Server

### Prerequisitos

- **Driver ODBC:** Microsoft ODBC Driver 17 ou 18 for SQL Server instalado no sistema operacional
  - Linux: [Instalacao via apt/yum](https://learn.microsoft.com/pt-br/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
  - Windows: instalador disponivel no site da Microsoft

### Permissoes necessarias

O usuario de monitoramento precisa das seguintes permissoes no SQL Server:

```sql
-- Criar usuario dedicado para monitoramento
CREATE LOGIN monitor_user WITH PASSWORD = 'senha_segura';
CREATE USER monitor_user FOR LOGIN monitor_user;

-- Permissoes necessarias
GRANT VIEW SERVER STATE TO monitor_user;
GRANT VIEW DATABASE STATE TO monitor_user;

-- Opcional: para capturar texto completo das queries
ALTER SERVER CONFIGURATION SET PROCESS AFFINITY NUMANODE = AUTO;
```

### Configuracao em databases.json

```json
{
  "name": "SQL Server - Producao",
  "type": "SQLSERVER",
  "enabled": true,
  "credentials": {
    "server": "sqlserver-prod.exemplo.com",
    "port": "1433",
    "database": "master",
    "username": "monitor_user",
    "password": "${SQL_SERVER_PROD_PASSWORD}",
    "driver": "ODBC Driver 18 for SQL Server"
  }
}
```

| Campo | Descricao |
|-------|-----------|
| `server` | Hostname ou IP do servidor |
| `port` | Porta padrão: `1433` |
| `database` | Use `master` para capturar queries globais de todas as databases |
| `driver` | Nome exato do driver ODBC instalado |

### Verificar driver disponivel

```bash
# Linux
odbcinst -q -d

# Windows: verificar em Gerenciador de Fonte de Dados ODBC
```

---

## PostgreSQL

### Prerequisitos

Sem instalacao adicional. O `psycopg2-binary` incluido em `requirements.txt` ja cobre a conectividade.

### Permissoes necessarias

```sql
-- Criar usuario dedicado
CREATE USER monitor_user WITH PASSWORD 'senha_segura';

-- Permissoes necessarias (PostgreSQL 10+)
GRANT pg_monitor TO monitor_user;

-- Alternativa para versoes mais antigas:
GRANT CONNECT ON DATABASE postgres TO monitor_user;
GRANT SELECT ON pg_stat_activity TO monitor_user;
GRANT SELECT ON pg_stat_statements TO monitor_user;

-- Habilitar extensao pg_stat_statements (como superuser)
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

### Configuracao em databases.json

```json
{
  "name": "PostgreSQL - Producao",
  "type": "POSTGRESQL",
  "enabled": true,
  "credentials": {
    "server": "postgresql-prod.exemplo.com",
    "port": "5432",
    "database": "postgres",
    "username": "monitor_user",
    "password": "${POSTGRESQL_PROD_PASSWORD}"
  }
}
```

| Campo | Descricao |
|-------|-----------|
| `server` | Hostname ou IP do servidor |
| `port` | Porta padrão: `5432` |
| `database` | Use `postgres` para capturar stats globais |

---

## SAP HANA

### Prerequisitos

- `hdbcli` instalado via `pip install -r requirements.txt`
- Conectividade de rede com o servidor HANA (porta 30015 para SYSTEMDB ou 30013 para tenant)

### Permissoes necessarias

```sql
-- Criar usuario de monitoramento
CREATE USER MONITOR_USER PASSWORD "senha_segura";

-- Permissoes necessarias
GRANT MONITORING TO MONITOR_USER;
GRANT SELECT ON SYS.M_SQL_PLAN_CACHE TO MONITOR_USER;
GRANT SELECT ON SYS.M_EXPENSIVE_STATEMENTS TO MONITOR_USER;
GRANT SELECT ON SYS.M_CONNECTIONS TO MONITOR_USER;
```

### Configuracao em databases.json

```json
{
  "name": "SAP HANA - Producao",
  "type": "HANA",
  "enabled": true,
  "credentials": {
    "server": "hana-prod.exemplo.com",
    "port": "30015",
    "database": "SYSTEMDB",
    "username": "MONITOR_USER",
    "password": "${HANA_PROD_PASSWORD}"
  }
}
```

| Campo | Descricao |
|-------|-----------|
| `server` | Hostname ou IP do servidor |
| `port` | `30015` para SYSTEMDB, `30013` para tenant database |
| `database` | `SYSTEMDB` para acesso global ou nome do tenant |

---

## Multiplos Bancos Simultaneos

O monitor suporta N instancias de qualquer combinacao de tipos. Cada tipo usa uma thread dedicada que processa suas instancias sequencialmente:

```
Thread SQL Server:  sqlserver-prod → sqlserver-dev
Thread PostgreSQL:  postgres-prod → postgres-staging
Thread HANA:        hana-prod
```

Exemplo com 5 bancos:

```json
{
  "databases": [
    {"name": "SQL Server Prod", "type": "SQLSERVER", "enabled": true, "credentials": {...}},
    {"name": "SQL Server Dev",  "type": "SQLSERVER", "enabled": false, "credentials": {...}},
    {"name": "PostgreSQL Prod", "type": "POSTGRESQL", "enabled": true, "credentials": {...}},
    {"name": "PostgreSQL Dev",  "type": "POSTGRESQL", "enabled": false, "credentials": {...}},
    {"name": "HANA Prod",       "type": "HANA",       "enabled": true,  "credentials": {...}}
  ]
}
```

Use `"enabled": false` para desabilitar temporariamente sem remover a configuracao.

## Seguranca

- Nunca use senhas em plaintext em `databases.json` — use `"password": "${NOME_DA_VAR}"`
- Nunca commite `.env` ou `config/databases.json` com credenciais reais
- Use usuarios dedicados com permissoes minimas (principio do menor privilegio)
- Para producao, considere armazenar credenciais em AWS Secrets Manager ou Azure Key Vault

## See Also

- [Configuracao](configuration.md) — opcoes gerais do config.json
- [Primeiros Passos](getting-started.md) — instalacao e primeiro uso
- [API REST](api.md) — endpoints de instancias e metricas
