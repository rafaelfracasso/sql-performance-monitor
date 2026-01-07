# Tarefas - Extensão do Monitor para Multi-Database

Projeto: Refatorar SQL Server Performance Monitor para suportar SQL Server, PostgreSQL e SAP HANA simultaneamente.

**Última atualização**: 2025-12-23 (Refatoração completa - 82% do projeto concluído)

---

## FASE 1: Estrutura e ABCs (Fundação)

- [x] **1.1** Criar estrutura de diretórios (core/, connections/, collectors/, extractors/, factories/, monitor/, utils/)
- [x] **1.2** Migrar componentes genéricos para sql_monitor/utils/
- [x] **1.3** Criar database_types.py (Enum: SQLSERVER, POSTGRESQL, HANA)
- [x] **1.4** Criar base_connection.py (ABC para conexões)
- [x] **1.5** Criar base_collector.py (ABC para coletores)
- [x] **1.6** Criar base_extractor.py (ABC para extratores)

**Status**: COMPLETA

---

## FASE 2: Migração SQL Server

- [x] **2.1** Migrar SQL Server Connection (connections/sqlserver_connection.py)
- [x] **2.2** Migrar SQL Server Collector (collectors/sqlserver_collector.py)
- [x] **2.3** Migrar SQL Server Extractor (extractors/sqlserver_extractor.py)

**Status**: COMPLETA

---

## FASE 3: Implementação PostgreSQL

- [x] **3.1** Implementar PostgreSQL Connection (connections/postgresql_connection.py)
- [x] **3.2** Implementar PostgreSQL Collector (collectors/postgresql_collector.py)
  - [x] Queries ativas (pg_stat_activity)
  - [x] Expensive queries (pg_stat_statements)
  - [x] Table scans (pg_stat_user_tables)
- [x] **3.3** Implementar PostgreSQL Extractor (extractors/postgresql_extractor.py)
  - [x] DDL (information_schema.columns)
  - [x] Índices (pg_indexes)
  - [x] Missing indexes (sugestões baseadas em seq_scan)

**Status**: COMPLETA

---

## FASE 4: Implementação SAP HANA

- [x] **4.1** Implementar HANA Connection (connections/hana_connection.py)
- [x] **4.2** Implementar HANA Collector (collectors/hana_collector.py)
  - [x] Queries ativas (M_ACTIVE_STATEMENTS + M_CONNECTIONS)
  - [x] Expensive queries (M_SQL_PLAN_CACHE)
  - [x] Table scans (M_TABLE_STATISTICS)
- [x] **4.3** Implementar HANA Extractor (extractors/hana_extractor.py)
  - [x] DDL (SYS.TABLE_COLUMNS)
  - [x] Índices (SYS.INDEXES + SYS.INDEX_COLUMNS)
  - [x] Missing indexes (M_CS_TABLES)

**Status**: COMPLETA

---

## FASE 5: Factory e Orquestração

- [x] **5.1** Implementar Database Factory (factories/database_factory.py)
  - [x] Método create_components(db_type, credentials)
  - [x] Suporte para SQLSERVER, POSTGRESQL, HANA
- [x] **5.2** Implementar Database Monitor (monitor/database_monitor.py)
  - [x] Monitor individual por banco
  - [x] Integração com cache por tipo
  - [x] Reutiliza lógica de análise existente
- [x] **5.3** Implementar Multi-Database Monitor (monitor/multi_monitor.py)
  - [x] Lê config/databases.json
  - [x] Agrupa monitors por tipo
  - [x] Execução multithread por TIPO
  - [x] Execução sequencial DENTRO do tipo
  - [x] Cache individual por tipo (sqlserver.json, postgresql.json, hana.json)

**Status**: COMPLETA

---

## FASE 6: Configuração

- [x] **6.1** Criar config/databases.json
  - [x] Definir estrutura JSON com múltiplas instâncias
  - [x] Credenciais individuais por instância
  - [x] Flags enabled/disabled
- [x] **6.2** Criar config/databases.json.example (template)
- [x] **6.3** Atualizar requirements.txt
  - [x] Adicionar psycopg2-binary (PostgreSQL)
  - [x] Adicionar hdbcli (SAP HANA)
- [x] **6.4** Atualizar .env.example
  - [x] Remover credenciais de banco (agora em databases.json)
  - [x] Manter apenas GEMINI_API_KEY
- [x] **6.5** Refatorar main.py
  - [x] Usar MultiDatabaseMonitor
  - [x] Passar config_path e db_config_path

**Status**: COMPLETA

---

## FASE 7: Testes e Validação

- [ ] **7.1** Teste SQL Server standalone
  - [ ] Conectar em 1 instância SQL Server
  - [ ] Validar collect_active_queries()
  - [ ] Validar collect_recent_expensive_queries()
  - [ ] Validar get_table_scan_queries()
  - [ ] Validar DDL e índices
- [ ] **7.2** Teste PostgreSQL standalone
  - [ ] Instalar extensão pg_stat_statements
  - [ ] Conectar em 1 instância PostgreSQL
  - [ ] Validar todas as queries de coleta
  - [ ] Validar DDL e índices
- [ ] **7.3** Teste SAP HANA standalone
  - [ ] Conectar em 1 instância HANA
  - [ ] Validar queries de coleta
  - [ ] Validar DDL e índices
- [ ] **7.4** Teste multi-banco integrado
  - [ ] Executar com 3 SQL Server + 2 PostgreSQL + 1 HANA
  - [ ] Validar execução multithread por tipo
  - [ ] Verificar cache individual por tipo
  - [ ] Validar notificações Teams
  - [ ] Validar logs por instância
- [ ] **7.5** Teste de paridade de features
  - [ ] Comparar outputs de todos os bancos
  - [ ] Validar estrutura padronizada de retorno
  - [ ] Verificar análise LLM para todos os bancos

**Status**: ⏸️ PENDENTE

---

## Resumo de Progresso

| Fase | Tarefas | Completas | Progresso |
|------|---------|-----------|-----------|
| FASE 1: Estrutura e ABCs | 6 | 6 | 100% |
| FASE 2: Migração SQL Server | 3 | 3 | 100% |
| FASE 3: PostgreSQL | 3 | 3 | 100% |
| FASE 4: SAP HANA | 3 | 3 | 100% |
| FASE 5: Factory e Orquestração | 3 | 3 | 100% |
| FASE 6: Configuração | 5 | 5 | 100% |
| FASE 7: Testes | 5 | 0 | 0% ⏸️ |
| **TOTAL** | **28** | **23** | **82%** |

---

## Próximos Passos

1. ~~Completar FASE 1-6 (Implementação completa)~~
2. **FASE 7: Testes e Validação** (PRIORIDADE)
   - Testar SQL Server standalone
   - Testar PostgreSQL standalone (requer extensão pg_stat_statements)
   - Testar SAP HANA standalone (requer servidor HANA)
   - Testar multi-banco integrado
   - Validar análise LLM para todos os bancos

---

## Notas Importantes

### Cache Thread-Safe
- Cada tipo de banco tem seu próprio arquivo de cache:
  - `logs/query_cache_sqlserver.json`
  - `logs/query_cache_postgresql.json`
  - `logs/query_cache_hana.json`
- Sem race conditions, sem locks, thread-safe por design

### Execução Multithread
- Uma thread POR TIPO de banco (não por instância)
- Processamento sequencial DENTRO de cada tipo
- Exemplo: Thread 1 processa SQL1 → SQL2 → SQL3 sequencialmente

### Arquivos Migrados
Componentes genéricos já copiados para `sql_monitor/utils/`:
- performance_checker.py
- query_sanitizer.py
- logger.py
- query_cache.py
- teams_notifier.py
- sql_formatter.py
- llm_analyzer.py

---

**Instruções de Uso:**
- Marque tarefas completas com `[x]`
- Atualize % de progresso após completar cada fase
- Documente problemas/observações nas notas
