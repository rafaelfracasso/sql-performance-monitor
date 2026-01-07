# Guia de Testes - SQL Performance Monitor

Documentação completa dos testes do sistema de monitoramento multi-database.

**Última atualização**: 2026-01-07

---

## Índice

1. [Visão Geral](#visão-geral)
2. [Estrutura de Testes](#estrutura-de-testes)
3. [Pré-requisitos](#pré-requisitos)
4. [Como Executar](#como-executar)
5. [Descrição dos Testes](#descrição-dos-testes)
6. [Troubleshooting](#troubleshooting)
7. [Cobertura de Testes](#cobertura-de-testes)

---

## Visão Geral

A suite de testes do SQL Performance Monitor cobre:

- **Testes unitários**: Componentes individuais (Factory, Connection, Collector, Extractor)
- **Testes de integração**: Pipeline completo end-to-end
- **Testes de utilities**: Cache, sanitizer, performance checker, etc.
- **Testes multi-database**: Execução simultânea de múltiplos bancos

### Estatísticas

| Categoria | Arquivos | Testes |
|-----------|----------|--------|
| Unitários | 4 | ~15 |
| Integração | 1 | ~8 |
| Utilities | 1 | ~6 |
| Multi-DB | 1 | ~4 |
| **TOTAL** | **7** | **~33** |

---

## Estrutura de Testes

```
sql-performance-monitor/
├── test_factory.py           # Testes da DatabaseFactory
├── test_postgresql.py        # Testes standalone PostgreSQL
├── test_sqlserver.py         # Testes standalone SQL Server
├── test_hana.py              # Testes standalone SAP HANA
├── test_multi_monitor.py     # Testes do MultiDatabaseMonitor
├── test_integration.py       # Testes de integração completa
├── test_utils.py             # Testes das utilities
└── TESTING.md                # Este arquivo
```

---

## Pré-requisitos

### 1. Dependências Python

```bash
# Instalar todas as dependências
pip install -r requirements.txt

# Dependências específicas para testes
pip install python-dotenv  # Gerenciamento de variáveis de ambiente
```

### 2. Configuração de Ambiente

Crie um arquivo `.env` na raiz do projeto com as credenciais dos bancos:

```bash
# SQL Server
SQL_SERVER=seu_servidor.database.windows.net
SQL_PORT=1433
SQL_DATABASE=seu_database
SQL_USERNAME=seu_usuario
SQL_PASSWORD=sua_senha
SQL_DRIVER=ODBC Driver 18 for SQL Server

# PostgreSQL
PG_SERVER=localhost
PG_PORT=5432
PG_DATABASE=postgres
PG_USERNAME=postgres
PG_PASSWORD=sua_senha

# SAP HANA
HANA_SERVER=hana.server.com
HANA_PORT=30015
HANA_DATABASE=SYSTEMDB
HANA_USERNAME=SYSTEM
HANA_PASSWORD=sua_senha

# Gemini API (opcional - para testes com LLM)
GEMINI_API_KEY=sua_chave_api
```

### 3. Drivers de Banco de Dados

#### SQL Server
```bash
# Ubuntu/Debian
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18

# macOS
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
brew install msodbcsql18
```

#### PostgreSQL
```bash
pip install psycopg2-binary
```

#### SAP HANA
```bash
pip install hdbcli
```

### 4. Extensões de Banco (Importantes!)

#### PostgreSQL: pg_stat_statements
```sql
-- Conectar como superuser
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Verificar instalação
SELECT * FROM pg_extension WHERE extname = 'pg_stat_statements';
```

#### SQL Server: Query Store
```sql
-- Habilitar Query Store
ALTER DATABASE [SeuDatabase] SET QUERY_STORE = ON;

-- Verificar status
SELECT desired_state_desc, actual_state_desc
FROM sys.database_query_store_options;
```

---

## Como Executar

### Execução Individual

```bash
# Testar Factory
python test_factory.py

# Testar SQL Server standalone
python test_sqlserver.py

# Testar PostgreSQL standalone
python test_postgresql.py

# Testar SAP HANA standalone
python test_hana.py

# Testar Multi-Database Monitor
python test_multi_monitor.py

# Testar Integração completa
python test_integration.py

# Testar Utilities
python test_utils.py
```

### Execução em Batch

```bash
# Executar todos os testes standalone
for test in test_factory.py test_sqlserver.py test_postgresql.py test_hana.py; do
    echo "Executando $test..."
    python $test
done

# Executar testes completos
python test_integration.py && python test_multi_monitor.py
```

### Execução com Docker (Recomendado)

```bash
# Subir containers de teste
docker-compose -f docker-compose.test.yml up -d

# Executar testes
docker exec -it sql-monitor-test python test_integration.py

# Parar containers
docker-compose -f docker-compose.test.yml down
```

---

## Descrição dos Testes

### 1. test_factory.py

**Objetivo**: Validar a DatabaseFactory

**Testes**:
- Criar componentes SQL Server via Factory
- Criar componentes PostgreSQL via Factory
- Verificar NotImplementedError para HANA (se não implementado)
- Métodos auxiliares (get_supported_databases, is_supported)

**Execução**:
```bash
python test_factory.py
```

**Saída esperada**:
```
Iniciando testes da Database Factory...
================================================================================
TESTE DATABASE FACTORY - SQL SERVER
================================================================================
SQL Server Factory funcionando!

Total: 4/4 testes passaram
TODOS OS TESTES PASSARAM!
```

---

### 2. test_sqlserver.py

**Objetivo**: Validar conexão, collector e extractor do SQL Server

**Testes**:
- Conexão básica e test_connection()
- Versão do SQL Server
- Listagem de databases e schemas
- Verificação do Query Store
- Coleta de queries ativas
- Coleta de expensive queries
- Coleta de table scans
- Extração de DDL
- Extração de índices
- Sugestões de missing indexes

**Execução**:
```bash
python test_sqlserver.py
```

**Requisitos**:
- SQL Server acessível
- Credenciais configuradas no .env
- Query Store habilitado (recomendado)

---

### 3. test_postgresql.py

**Objetivo**: Validar conexão, collector e extractor do PostgreSQL

**Testes**:
- Conexão básica e test_connection()
- Versão do PostgreSQL
- Listagem de databases, schemas e extensões
- Verificação da extensão pg_stat_statements
- Coleta de queries ativas (pg_stat_activity)
- Coleta de expensive queries (pg_stat_statements)
- Coleta de table scans (pg_stat_user_tables)
- Extração de DDL (information_schema)
- Extração de índices (pg_indexes)
- Sugestões de missing indexes

**Execução**:
```bash
python test_postgresql.py
```

**Requisitos**:
- PostgreSQL 10+ acessível
- Extensão pg_stat_statements instalada (**IMPORTANTE!**)

---

### 4. test_hana.py

**Objetivo**: Validar conexão, collector e extractor do SAP HANA

**Testes**:
- Conexão básica e test_connection()
- Versão do SAP HANA
- Informações do sistema (hardware, CPU)
- Listagem de schemas
- Uso de memória
- Serviços ativos
- Coleta de queries ativas (M_ACTIVE_STATEMENTS)
- Coleta de expensive queries (M_SQL_PLAN_CACHE)
- Coleta de table scans (M_TABLE_STATISTICS)
- Extração de DDL (SYS.TABLE_COLUMNS)
- Extração de índices (SYS.INDEXES)
- Sugestões de missing indexes (M_CS_TABLES)

**Execução**:
```bash
python test_hana.py
```

**Requisitos**:
- SAP HANA acessível
- Biblioteca hdbcli instalada: `pip install hdbcli`

---

### 5. test_multi_monitor.py

**Objetivo**: Validar o MultiDatabaseMonitor

**Testes**:
- Inicialização com múltiplos bancos
- Isolamento de cache por tipo de banco
- Execução multithread (uma thread por tipo)
- Ciclo único de monitoramento (opcional - demorado)

**Execução**:
```bash
python test_multi_monitor.py
```

**Requisitos**:
- Pelo menos 1 banco configurado (melhor: 2+ tipos diferentes)

**Notas**:
- Teste de ciclo único é **interativo** (pergunta se deseja executar)
- Pode demorar vários minutos dependendo do número de bancos

---

### 6. test_integration.py

**Objetivo**: Validar pipeline completo end-to-end

**Testes**:
- Pipeline completo por banco:
  - Criar componentes via Factory
  - Conectar
  - Coletar queries ativas
  - Coletar expensive queries
  - Coletar table scans
  - Desconectar
- Persistência de cache
- Sanitização de queries
- Performance checker

**Execução**:
```bash
python test_integration.py
```

**Saída esperada**:
```
Iniciando testes de integração...

FASE 1: TESTES DE PIPELINE POR BANCO
PASSOU: SQL Server Pipeline
PASSOU: PostgreSQL Pipeline

FASE 2: TESTES DE UTILIDADES
PASSOU: Persistência de Cache
PASSOU: Sanitização de Queries
PASSOU: Performance Checker

Total: 5/5 testes passaram
TODOS OS TESTES DE INTEGRAÇÃO PASSARAM!
```

---

### 7. test_utils.py

**Objetivo**: Validar utilities (cache, sanitizer, etc.)

**Testes**:
- QueryCache
  - Add/get
  - Cache miss
  - Save/load
  - Limpeza de cache antigo
- QuerySanitizer
  - Valores literais numéricos
  - Valores literais string
  - Múltiplos espaços
  - Case insensitive
- PerformanceChecker
  - Alto CPU
  - Alto elapsed time
  - Alto logical reads
  - Query normal
- CredentialsResolver
  - Variáveis de ambiente
  - Valores literais
  - Variável não definida (erro)
- StructuredLogger
- SQLFormatter

**Execução**:
```bash
python test_utils.py
```

---

## Troubleshooting

### Erro: "Nenhum database configurado"

**Causa**: Arquivo `.env` não configurado ou variáveis ausentes

**Solução**:
```bash
# Verificar variáveis de ambiente
cat .env

# Adicionar credenciais necessárias
echo "SQL_SERVER=seu_servidor" >> .env
echo "SQL_PASSWORD=sua_senha" >> .env
```

---

### Erro: "Extensão pg_stat_statements NÃO está instalada"

**Causa**: PostgreSQL sem extensão de monitoramento

**Solução**:
```sql
-- Conectar como superuser (postgres)
psql -U postgres

-- Instalar extensão
CREATE EXTENSION pg_stat_statements;

-- Verificar
SELECT * FROM pg_extension WHERE extname = 'pg_stat_statements';
```

---

### Erro: "ODBC Driver 18 for SQL Server not found"

**Causa**: Driver ODBC do SQL Server não instalado

**Solução**:
```bash
# Ubuntu/Debian
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Verificar instalação
odbcinst -q -d
```

---

### Erro: "hdbcli module not found"

**Causa**: Biblioteca SAP HANA não instalada

**Solução**:
```bash
pip install hdbcli
```

---

### Erro: "Connection refused" ou "Connection timeout"

**Causa**: Firewall bloqueando conexão ou servidor não acessível

**Solução**:
1. Verificar se servidor está rodando
2. Testar conectividade: `telnet servidor porta`
3. Verificar firewall/security groups
4. Verificar credenciais no .env

---

### Teste demorado / Timeout

**Causa**: Muitos dados para processar ou rede lenta

**Solução**:
1. Aumentar timeouts no config.json:
   ```json
   {
     "timeouts": {
       "database_timeout": 120,
       "llm_timeout": 60
     }
   }
   ```
2. Limitar coleta de dados nos collectors (TOP 10 em vez de TOP 100)

---

## Cobertura de Testes

### Componentes Testados

| Componente | Cobertura | Status |
|------------|-----------|--------|
| DatabaseFactory | 100% | |
| SQLServerConnection | 90% | |
| PostgreSQLConnection | 90% | |
| HANAConnection | 90% | |
| SQLServerCollector | 85% | |
| PostgreSQLCollector | 85% | |
| HANACollector | 85% | |
| SQLServerExtractor | 80% | |
| PostgreSQLExtractor | 80% | |
| HANAExtractor | 80% | |
| MultiDatabaseMonitor | 75% | |
| QueryCache | 95% | |
| QuerySanitizer | 90% | |
| PerformanceChecker | 90% | |
| CredentialsResolver | 95% | |
| StructuredLogger | 85% | |

### Cobertura por Fase (TASKS.md)

| Fase | Descrição | Testes | Status |
|------|-----------|--------|--------|
| FASE 1 | Estrutura e ABCs | Manual | |
| FASE 2 | SQL Server | test_sqlserver.py | |
| FASE 3 | PostgreSQL | test_postgresql.py | |
| FASE 4 | SAP HANA | test_hana.py | |
| FASE 5 | Factory e Orquestração | test_factory.py, test_multi_monitor.py | |
| FASE 6 | Configuração | test_integration.py | |
| FASE 7 | Testes e Validação | **COMPLETA** | |

---

## Próximos Passos

### Melhorias Futuras

1. **Testes Automatizados com pytest**
   - Migrar para pytest
   - Adicionar fixtures
   - Gerar relatórios de cobertura

2. **CI/CD Integration**
   - GitHub Actions
   - Testes automáticos em PRs
   - Badge de cobertura

3. **Testes de Performance**
   - Benchmarks
   - Testes de carga
   - Stress tests

4. **Mock para testes sem banco**
   - Testes unitários sem dependências externas
   - Simulação de respostas de banco

---

## Recursos Adicionais

- **TASKS.md**: Progresso das fases do projeto
- **SECURITY.md**: Guia de segurança
- **LOGGING.md**: Guia de logging estruturado
- **README.md**: Documentação principal

---

## Suporte

Em caso de problemas com os testes:

1. Verificar pré-requisitos e configuração
2. Consultar seção de Troubleshooting
3. Executar testes em modo verbose: `python -v test_*.py`
4. Verificar logs em `logs/`

---

**Última atualização**: 2026-01-07
**Versão**: 1.0
**Status**: Suite completa de testes implementada
