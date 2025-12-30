# Permissões Necessárias para Monitoramento

Este documento detalha as permissões mínimas necessárias para o usuário de monitoramento em cada tipo de banco de dados.

## Sumário

- [SQL Server](#sql-server)
- [PostgreSQL](#postgresql)
- [SAP HANA](#sap-hana)
- [Princípios de Segurança](#princípios-de-segurança)

---

## SQL Server

### Permissões Mínimas

O usuário de monitoramento precisa de:

1. **VIEW SERVER STATE** - Para acessar DMVs (Dynamic Management Views)
2. **VIEW DATABASE STATE** - Para acessar DMVs específicas do database
3. **VIEW DEFINITION** - Para extrair DDL de tabelas
4. **SELECT** em schemas monitorados - Para ler metadados

### Script de Criação do Usuário

```sql
-- ==========================================
-- Criar Login e Usuário de Monitoramento
-- ==========================================

USE [master];
GO

-- 1. Criar login no nível de servidor
CREATE LOGIN [db_monitor]
WITH PASSWORD = 'SuaSenhaSegura123!',
     DEFAULT_DATABASE = [master],
     CHECK_EXPIRATION = OFF,
     CHECK_POLICY = ON;
GO

-- 2. Conceder permissões no nível de servidor
GRANT VIEW SERVER STATE TO [db_monitor];
GO

-- 3. Criar usuário em cada database que será monitorado
USE [master];  -- Substituir pelo nome do database
GO

CREATE USER [db_monitor] FOR LOGIN [db_monitor];
GO

-- 4. Conceder permissões no nível de database
GRANT VIEW DATABASE STATE TO [db_monitor];
GRANT VIEW DEFINITION TO [db_monitor];
GO

-- 5. Conceder SELECT em schemas específicos (ajustar conforme necessário)
GRANT SELECT ON SCHEMA::dbo TO [db_monitor];
GO

-- ==========================================
-- Repetir passos 3-5 para cada database
-- ==========================================
```

### O Que Cada Permissão Permite

| Permissão | O Que Permite | Usado Para |
|-----------|---------------|------------|
| **VIEW SERVER STATE** | Acessar DMVs como `sys.dm_exec_requests`, `sys.dm_exec_sessions` | Coletar queries ativas e métricas de CPU/IO |
| **VIEW DATABASE STATE** | Acessar DMVs específicas do database | Coletar estatísticas de tabelas e índices |
| **VIEW DEFINITION** | Ler definição de objetos (tabelas, views, procedures) | Extrair DDL completo das tabelas |
| **SELECT on SCHEMA** | Ler metadados de tabelas e índices | Extrair informações de colunas e índices existentes |

### Verificar Permissões

```sql
-- Verificar permissões do usuário
SELECT
    prin.name AS UserName,
    perm.permission_name,
    perm.state_desc
FROM sys.database_permissions perm
INNER JOIN sys.database_principals prin ON perm.grantee_principal_id = prin.principal_id
WHERE prin.name = 'db_monitor'
ORDER BY perm.permission_name;

-- Verificar permissões no nível de servidor
SELECT
    prin.name AS LoginName,
    perm.permission_name,
    perm.state_desc
FROM sys.server_permissions perm
INNER JOIN sys.server_principals prin ON perm.grantee_principal_id = prin.principal_id
WHERE prin.name = 'db_monitor'
ORDER BY perm.permission_name;
```

### Troubleshooting

**Erro: "VIEW SERVER STATE permission was denied"**
- Solução: Executar `GRANT VIEW SERVER STATE TO [db_monitor];` como sysadmin

**Erro: "The SELECT permission was denied on the object"**
- Solução: Conceder SELECT no schema: `GRANT SELECT ON SCHEMA::dbo TO [db_monitor];`

---

## PostgreSQL

### Permissões Mínimas

O usuário de monitoramento precisa de:

1. **CONNECT** no database
2. **pg_read_all_stats** role - Para acessar `pg_stat_*` views
3. **USAGE** nos schemas monitorados
4. **SELECT** em `information_schema` e system catalogs

### Script de Criação do Usuário

```sql
-- ==========================================
-- Criar Usuário de Monitoramento
-- ==========================================

-- 1. Criar role de monitoramento
CREATE ROLE db_monitor WITH
    LOGIN
    PASSWORD 'SuaSenhaSegura123!'
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION
    CONNECTION LIMIT -1;

-- 2. Conceder acesso ao database
GRANT CONNECT ON DATABASE postgres TO db_monitor;  -- Substituir 'postgres' pelo nome do database

-- 3. Conceder role pg_read_all_stats (PostgreSQL 10+)
GRANT pg_read_all_stats TO db_monitor;

-- 4. Conceder USAGE nos schemas monitorados
GRANT USAGE ON SCHEMA public TO db_monitor;  -- Repetir para cada schema

-- 5. Conceder SELECT nas tabelas de sistema
GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO db_monitor;
GRANT SELECT ON ALL TABLES IN SCHEMA pg_catalog TO db_monitor;

-- 6. Para usar pg_stat_statements (recomendado)
-- Primeiro, habilite a extensão (requer superuser):
-- CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Depois, conceda acesso:
GRANT SELECT ON pg_stat_statements TO db_monitor;

-- ==========================================
-- Configuração Adicional (Opcional)
-- ==========================================

-- Permitir visualizar queries de outros usuários
-- ALTER ROLE db_monitor SET log_statement = 'none';  -- Evitar logs excessivos
```

### O Que Cada Permissão Permite

| Permissão | O Que Permite | Usado Para |
|-----------|---------------|------------|
| **CONNECT** | Conectar ao database | Acesso básico |
| **pg_read_all_stats** | Acessar views `pg_stat_*` de todos os usuários | Coletar queries ativas e estatísticas |
| **USAGE on SCHEMA** | Acessar objetos dentro do schema | Listar tabelas e views |
| **SELECT on information_schema** | Ler metadados de tabelas, colunas, constraints | Extrair DDL e estrutura de tabelas |
| **SELECT on pg_stat_statements** | Acessar histórico de queries executadas | Identificar expensive queries |

### Verificar Permissões

```sql
-- Verificar roles atribuídas
SELECT
    r.rolname,
    r.rolsuper,
    r.rolinherit,
    r.rolcreaterole,
    r.rolcreatedb,
    r.rolcanlogin,
    ARRAY(
        SELECT b.rolname
        FROM pg_catalog.pg_auth_members m
        JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
        WHERE m.member = r.oid
    ) as member_of
FROM pg_catalog.pg_roles r
WHERE r.rolname = 'db_monitor';

-- Verificar permissões em schemas
SELECT
    nspname AS schema_name,
    has_schema_privilege('db_monitor', nspname, 'USAGE') AS has_usage
FROM pg_namespace
WHERE nspname NOT LIKE 'pg_%'
    AND nspname != 'information_schema'
ORDER BY nspname;

-- Verificar se pg_stat_statements está habilitado
SELECT * FROM pg_extension WHERE extname = 'pg_stat_statements';
```

### Habilitar pg_stat_statements

A extensão `pg_stat_statements` é **altamente recomendada** para monitoramento de expensive queries.

```sql
-- 1. Conectar como superuser (postgres)
-- 2. Criar extensão
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- 3. Adicionar ao postgresql.conf
-- shared_preload_libraries = 'pg_stat_statements'

-- 4. Reiniciar PostgreSQL
-- sudo systemctl restart postgresql

-- 5. Verificar
SELECT * FROM pg_stat_statements LIMIT 5;
```

### Troubleshooting

**Erro: "permission denied for view pg_stat_activity"**
- Solução: `GRANT pg_read_all_stats TO db_monitor;`

**Erro: "extension pg_stat_statements does not exist"**
- Solução: Criar extensão e adicionar ao `shared_preload_libraries` (requer restart)

**Erro: "permission denied for schema"**
- Solução: `GRANT USAGE ON SCHEMA nome_schema TO db_monitor;`

---

## SAP HANA

### Permissões Mínimas

O usuário de monitoramento precisa de:

1. **MONITORING** system privilege - Para acessar system views (M_*)
2. **SELECT** privilege em `SYS` schema
3. **CATALOG READ** - Para ler metadados de tabelas

### Script de Criação do Usuário

```sql
-- ==========================================
-- Criar Usuário de Monitoramento
-- ==========================================

-- 1. Criar usuário (conectar como SYSTEM)
CREATE USER DB_MONITOR PASSWORD "SuaSenhaSegura123!" NO FORCE_FIRST_PASSWORD_CHANGE;

-- 2. Conceder privilégio de monitoramento
GRANT MONITORING TO DB_MONITOR;

-- 3. Conceder CATALOG READ (para metadados)
GRANT CATALOG READ TO DB_MONITOR;

-- 4. Conceder SELECT em system views
GRANT SELECT ON SCHEMA SYS TO DB_MONITOR;

-- 5. Conceder SELECT em schemas monitorados (opcional, ajustar conforme necessário)
-- GRANT SELECT ON SCHEMA "nome_schema" TO DB_MONITOR;

-- 6. Desativar expiração de senha (opcional, para usuário de serviço)
ALTER USER DB_MONITOR DISABLE PASSWORD LIFETIME;

-- ==========================================
-- Configuração Adicional (Opcional)
-- ==========================================

-- Permitir conexões simultâneas
-- ALTER USER DB_MONITOR SET PARAMETER 'max_connections' = '10';
```

### O Que Cada Permissão Permite

| Permissão | O Que Permite | Usado Para |
|-----------|---------------|------------|
| **MONITORING** | Acessar system views M_* (M_ACTIVE_STATEMENTS, M_CONNECTIONS, etc.) | Coletar queries ativas e métricas de performance |
| **CATALOG READ** | Ler metadados de objetos de database | Extrair DDL de tabelas, índices e constraints |
| **SELECT on SYS** | Acessar tabelas de sistema (TABLES, COLUMNS, INDEXES) | Extrair estrutura de tabelas e índices |

### Verificar Permissões

```sql
-- Verificar privilégios do usuário
SELECT
    GRANTEE,
    PRIVILEGE,
    IS_GRANTABLE
FROM SYS.GRANTED_PRIVILEGES
WHERE GRANTEE = 'DB_MONITOR'
ORDER BY PRIVILEGE;

-- Verificar privilégios de sistema
SELECT
    GRANTEE,
    PRIVILEGE
FROM SYS.GRANTED_PRIVILEGES
WHERE GRANTEE = 'DB_MONITOR'
    AND OBJECT_TYPE = 'SYSTEMPRIVILEGE';

-- Verificar acesso a monitoring views
SELECT
    VIEW_NAME,
    IS_VALID
FROM SYS.VIEWS
WHERE SCHEMA_NAME = 'SYS'
    AND VIEW_NAME LIKE 'M_%'
LIMIT 10;
```

### Troubleshooting

**Erro: "insufficient privilege: Not authorized"**
- Solução: `GRANT MONITORING TO DB_MONITOR;`

**Erro: "insufficient privilege: Detailed info for this error can be found with guid"**
- Solução: Verificar se `CATALOG READ` foi concedido

**Erro: "Connection failed (RTE:[-10104] Authentication failed)"**
- Solução: Verificar usuário/senha e se o usuário está ativo

---

## Princípios de Segurança

### Mínimo Privilégio

O usuário de monitoramento deve ter **apenas** as permissões necessárias:

- ✅ **Somente leitura** (SELECT, VIEW)
- ✅ **Sem permissões de escrita** (INSERT, UPDATE, DELETE)
- ✅ **Sem permissões administrativas** (DROP, ALTER, CREATE)
- ✅ **Sem acesso a dados sensíveis** (usar sanitização de queries)

### Isolamento

- Criar usuário dedicado exclusivamente para monitoramento
- Não reutilizar usuários de aplicação
- Não usar usuários administrativos (sa, postgres, SYSTEM)

### Auditoria

- Revisar logs de acesso periodicamente
- Monitorar uso de permissões
- Desabilitar usuário quando não estiver em uso

### Rotação de Senhas

```bash
# SQL Server
ALTER LOGIN [db_monitor] WITH PASSWORD = 'NovaSenha123!';

# PostgreSQL
ALTER ROLE db_monitor WITH PASSWORD 'NovaSenha123!';

# SAP HANA
ALTER USER DB_MONITOR PASSWORD "NovaSenha123!";
```

---

## Checklist de Implementação

### SQL Server
- [ ] Criar login `db_monitor`
- [ ] Conceder `VIEW SERVER STATE`
- [ ] Criar usuário em cada database
- [ ] Conceder `VIEW DATABASE STATE` e `VIEW DEFINITION`
- [ ] Conceder `SELECT` nos schemas necessários
- [ ] Testar conexão e queries

### PostgreSQL
- [ ] Criar role `db_monitor`
- [ ] Conceder `pg_read_all_stats`
- [ ] Conceder `CONNECT` no database
- [ ] Conceder `USAGE` nos schemas
- [ ] Criar extensão `pg_stat_statements` (se possível)
- [ ] Testar conexão e queries

### SAP HANA
- [ ] Criar usuário `DB_MONITOR`
- [ ] Conceder `MONITORING` privilege
- [ ] Conceder `CATALOG READ`
- [ ] Conceder `SELECT ON SCHEMA SYS`
- [ ] Desabilitar expiração de senha
- [ ] Testar conexão e queries

---

## Referências

### SQL Server
- [Dynamic Management Views](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/system-dynamic-management-views)
- [Server-Level Permissions](https://learn.microsoft.com/en-us/sql/relational-databases/security/permissions-database-engine)

### PostgreSQL
- [System Catalogs](https://www.postgresql.org/docs/current/catalogs.html)
- [Statistics Views](https://www.postgresql.org/docs/current/monitoring-stats.html)
- [pg_stat_statements](https://www.postgresql.org/docs/current/pgstatstatements.html)

### SAP HANA
- [Monitoring Views](https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/d3c10d23e8334a35afa8d9bdbc102366.html)
- [System Privileges](https://help.sap.com/docs/SAP_HANA_PLATFORM/b3ee5778bc2e4a089d3299b82ec762a7/20a8da4a75191014ba00bd8cfb5ddab5.html)
