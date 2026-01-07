# Testes - SQL Performance Monitor

Este diretório contém todos os testes do projeto, organizados por tipo.

## Estrutura

```
tests/
├── __init__.py
├── README.md (este arquivo)
├── unit/                    # Testes unitários
│   ├── test_factory.py      # Testes da DatabaseFactory
│   └── test_utils.py         # Testes das utilities
├── integration/             # Testes de integração com bancos
│   ├── test_sqlserver.py    # Testes SQL Server
│   ├── test_postgresql.py   # Testes PostgreSQL
│   ├── test_hana.py         # Testes SAP HANA
│   └── test_multi_monitor.py # Testes MultiDatabaseMonitor
└── e2e/                     # Testes end-to-end
    └── test_integration.py  # Testes do pipeline completo
```

## Tipos de Testes

### Unit Tests (unit/)
Testes de componentes individuais, sem dependências externas.
- Rápidos de executar
- Não requerem banco de dados
- Testam lógica isolada

**Executar**:
```bash
python -m pytest tests/unit/
```

### Integration Tests (integration/)
Testes que verificam integração com bancos de dados reais.
- Requerem credenciais configuradas no `.env`
- Testam conexões, collectors e extractors
- Validam queries e extração de metadados

**Executar**:
```bash
# Todos os testes de integração
python -m pytest tests/integration/

# Teste específico
python tests/integration/test_sqlserver.py
```

### End-to-End Tests (e2e/)
Testes do fluxo completo do sistema.
- Pipeline completo: Factory → Conexão → Coleta → Análise
- Testam múltiplos componentes integrados
- Validam comportamento real do sistema

**Executar**:
```bash
python -m pytest tests/e2e/
# ou
python tests/e2e/test_integration.py
```

## Configuração

### Pré-requisitos

1. **Dependências**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Drivers de Banco** (para testes de integração):
   - **SQL Server**: ODBC Driver 18
   - **PostgreSQL**: psycopg2-binary
   - **SAP HANA**: hdbcli

3. **Credenciais** (para testes de integração):
   ```bash
   cp .env.example .env
   # Edite .env com credenciais reais
   ```

### Extensões de Banco

#### PostgreSQL
```sql
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

#### SQL Server
```sql
ALTER DATABASE [SeuDatabase] SET QUERY_STORE = ON;
```

## Executando os Testes

### Todos os testes
```bash
python -m pytest tests/
```

### Por categoria
```bash
# Apenas unit tests
python -m pytest tests/unit/

# Apenas integration tests
python -m pytest tests/integration/

# Apenas e2e tests
python -m pytest tests/e2e/
```

### Teste específico
```bash
python tests/unit/test_factory.py
python tests/integration/test_sqlserver.py
```

### Com verbose
```bash
python -m pytest tests/ -v
```

### Com coverage
```bash
python -m pytest tests/ --cov=sql_monitor --cov-report=html
```

## Cobertura de Testes

| Componente | Cobertura | Testes |
|------------|-----------|--------|
| DatabaseTypes | 100% | unit/test_factory.py |
| DatabaseFactory | 100% | unit/test_factory.py |
| CredentialsResolver | 100% | unit/test_utils.py |
| QueryCache | 90% | unit/test_utils.py |
| QuerySanitizer | 90% | unit/test_utils.py |
| Connections | 90% | integration/test_*.py |
| Collectors | 85% | integration/test_*.py |
| Extractors | 80% | integration/test_*.py |

**Total**: ~90% de cobertura

## Troubleshooting

### "No module named 'pyodbc'"
```bash
# Instalar drivers
pip install pyodbc psycopg2-binary hdbcli
```

### "libodbc.so.2: cannot open shared object file"
```bash
# Ubuntu/Debian
sudo apt-get install unixodbc unixodbc-dev

# Verificar instalação
odbcinst -q -d
```

### "pg_stat_statements not found"
```sql
-- PostgreSQL
CREATE EXTENSION pg_stat_statements;
```

### Testes de integração pulados
Configure credenciais no `.env`:
```bash
# SQL Server
SQL_SERVER=seu_servidor
SQL_DATABASE=seu_database
SQL_USERNAME=seu_usuario
SQL_PASSWORD=sua_senha

# PostgreSQL
PG_SERVER=localhost
PG_DATABASE=postgres
PG_USERNAME=postgres
PG_PASSWORD=sua_senha

# SAP HANA
HANA_SERVER=hana.server.com
HANA_DATABASE=SYSTEMDB
HANA_USERNAME=SYSTEM
HANA_PASSWORD=sua_senha
```

## Documentação

Para mais detalhes, consulte:
- `docs/TESTING.md` - Guia completo de testes
- `docs/TEST_RESULTS.md` - Resultados da última execução
- `README.md` - Documentação principal do projeto

---

**Última atualização**: 2026-01-07
