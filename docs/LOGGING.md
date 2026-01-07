# Guia de Logging Estruturado

Este documento explica o sistema de logging estruturado implementado no projeto.

## 📊 Visão Geral

O projeto implementa um sistema de logging estruturado com:
- ✅ Múltiplos níveis (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- ✅ Múltiplos formatos (colored, json, simple)
- ✅ Suporte a structured logging (campos extras)
- ✅ Saída para console e arquivo
- ✅ Cores ANSI no console para melhor legibilidade
- ✅ Formato JSON para integração com ELK, Splunk, etc.

## ⚙️ Configuração

### config.json

```json
{
  "logging": {
    "level": "INFO",
    "format": "colored",
    "log_file": "logs/monitor.log",
    "enable_console": true
  }
}
```

**Opções**:
- `level`: DEBUG, INFO, WARNING, ERROR, CRITICAL
- `format`:
  - `colored` - Console com cores ANSI (desenvolvimento)
  - `json` - JSON estruturado (produção)
  - `simple` - Texto simples
- `log_file`: Caminho para arquivo de log (sempre usa JSON)
- `enable_console`: true/false

## 🚀 Como Usar

### 1. Logger Simples (compatível com logging padrão)

```python
from sql_monitor.utils.structured_logger import create_logger

logger = create_logger(__name__, structured=False)

logger.debug("Mensagem de debug")
logger.info("Informação")
logger.warning("Aviso")
logger.error("Erro")
logger.critical("Crítico")
```

### 2. Logger Estruturado (RECOMENDADO)

```python
from sql_monitor.utils.structured_logger import create_logger

logger = create_logger(__name__)  # structured=True por padrão

# Log simples
logger.info("Query executada")

# Log com campos estruturados
logger.info("Query executada",
           query_hash="abc123",
           duration_ms=150,
           database="production")

# Log de erro com exception
try:
    # código
    pass
except Exception as e:
    logger.error("Falha ao conectar",
                database="prod",
                error=str(e),
                exc_info=True)  # Inclui traceback
```

### 3. Contexto Global

Use contexto para adicionar campos que aparecem em todos os logs:

```python
logger = create_logger(__name__)

# Define contexto (aparece em todos os logs)
logger.set_context(
    monitor_id="sql-server-prod",
    environment="production"
)

# Estes logs incluem o contexto automaticamente
logger.info("Iniciando ciclo", cycle=1)
# Output: {..., "monitor_id": "sql-server-prod", "environment": "production", "cycle": 1}

logger.info("Ciclo completo", queries_found=10)
# Output: {..., "monitor_id": "sql-server-prod", "environment": "production", "queries_found": 10}

# Remove contexto
logger.clear_context()
```

## 📝 Formatos de Saída

### Colored (Console - Desenvolvimento)

```
[2025-12-23 10:30:45] INFO     [multi_monitor.initialize   ] Monitor inicializado
[2025-12-23 10:30:46] WARNING  [connection.connect         ] Conexão lenta
[2025-12-23 10:30:47] ERROR    [llm_analyzer.analyze       ] API timeout
```

### JSON (Arquivo - Produção)

```json
{
  "timestamp": "2025-12-23T10:30:45.123Z",
  "level": "INFO",
  "logger": "sql_monitor.monitor.multi_monitor",
  "message": "Monitor inicializado",
  "module": "multi_monitor",
  "function": "initialize",
  "line": 125,
  "extra_fields": {
    "databases": 3,
    "threads": 3
  }
}
```

### Simple (Texto Puro)

```
2025-12-23 10:30:45 - sql_monitor.monitor.multi_monitor - INFO - Monitor inicializado
```

## 🎯 Quando Usar Cada Nível

### DEBUG
Informações detalhadas para diagnóstico:
```python
logger.debug("Tentando conectar",
            host="sqlserver-prod.com",
            port=1433,
            attempt=1)
```

### INFO
Eventos normais importantes:
```python
logger.info("Monitor iniciado",
           databases=3,
           threads=3)

logger.info("Query analisada",
           query_hash="abc123",
           priority="ALTO")
```

### WARNING
Situações que precisam atenção mas não impedem funcionamento:
```python
logger.warning("Cache hit ratio baixo",
              hit_ratio=0.15,
              threshold=0.50)

logger.warning("Circuit breaker OPEN",
              failures=5,
              recovery_timeout=60)
```

### ERROR
Erros que impedem operação específica:
```python
logger.error("Falha ao conectar database",
            database="sql-prod",
            error=str(e),
            exc_info=True)

logger.error("API quota exceeded",
            requests_today=1500,
            limit=1500)
```

### CRITICAL
Erros que podem parar o sistema inteiro:
```python
logger.critical("Todos os databases falharam",
               failed_count=3,
               total_count=3,
               exc_info=True)
```

## 📊 Padrões de Uso

### 1. Performance Logging

```python
import time

start = time.time()

# ... operação ...

duration_ms = (time.time() - start) * 1000

logger.info("Operação concluída",
           operation="analyze_query",
           duration_ms=round(duration_ms, 2),
           success=True)
```

### 2. Error Handling

```python
try:
    result = api_call()
except TimeoutError as e:
    logger.error("API timeout",
                api="gemini",
                timeout_seconds=30,
                error=str(e))
    # Fallback
except APIError as e:
    logger.error("API error",
                api="gemini",
                status_code=e.status_code,
                error=str(e),
                exc_info=True)
    raise
```

### 3. Circuit Breaker State Changes

```python
if circuit_state == CircuitState.OPEN:
    logger.warning("Circuit breaker opened",
                  component="llm_api",
                  consecutive_failures=5,
                  recovery_timeout=60)
elif circuit_state == CircuitState.HALF_OPEN:
    logger.info("Circuit breaker testing recovery",
               component="llm_api")
elif circuit_state == CircuitState.CLOSED:
    logger.info("Circuit breaker closed",
               component="llm_api",
               consecutive_successes=2)
```

### 4. Database Operations

```python
logger.debug("Executando query",
            database="sql-prod",
            query_hash="abc123")

result = execute_query(...)

if result:
    logger.info("Query executada com sucesso",
               database="sql-prod",
               query_hash="abc123",
               rows_returned=len(result),
               duration_ms=150)
else:
    logger.error("Query retornou vazio",
                database="sql-prod",
                query_hash="abc123")
```

## 🔍 Análise de Logs

### Grep no formato colored

```bash
# Erros nas últimas 24h
grep "ERROR" logs/monitor.log

# Warnings de um módulo específico
grep "llm_analyzer" logs/monitor.log | grep "WARNING"
```

### jq no formato JSON

```bash
# Todos os logs de ERROR
jq 'select(.level == "ERROR")' logs/monitor.log

# Logs de um database específico
jq 'select(.extra_fields.database == "sql-prod")' logs/monitor.log

# Operações lentas (>1000ms)
jq 'select(.extra_fields.duration_ms > 1000)' logs/monitor.log

# Count de logs por level
jq -s 'group_by(.level) | map({level: .[0].level, count: length})' logs/monitor.log
```

### Integração com ELK Stack

1. Configure Filebeat para ler `logs/monitor.log`
2. Parse JSON automaticamente
3. Crie dashboards no Kibana:
   - Queries analisadas por minuto
   - Taxa de cache hit
   - Circuit breaker state changes
   - Erros por database
   - Performance por operação

## 🚀 Migration Guide

### De print() para logger

**Antes**:
```python
print(f"✓ Monitor iniciado: {name}")
print(f"⚠️  Cache hit ratio baixo: {ratio}")
print(f"✗ Erro ao conectar: {e}")
```

**Depois**:
```python
logger.info("Monitor iniciado", monitor_name=name)
logger.warning("Cache hit ratio baixo", hit_ratio=ratio, threshold=0.50)
logger.error("Erro ao conectar", database=name, error=str(e))
```

### Mantém print() para UI

Para saídas de interface com usuário, mantenha `print()`:
```python
# UI output (mantém)
print("\n" + "=" * 80)
print("MULTI-DATABASE MONITOR")
print("=" * 80)

# Logging estruturado (adiciona)
logger.info("Sistema iniciado",
           version="2.1.0",
           databases_configured=3)
```

## 📈 Best Practices

1. ✅ **Use structured fields** em vez de strings interpoladas
2. ✅ **Inclua contexto** relevante (database, query_hash, etc)
3. ✅ **Use níveis apropriados** (não use ERROR para warnings)
4. ✅ **Adicione exc_info=True** em logs de exception
5. ✅ **Evite logar dados sensíveis** (senhas, PII)
6. ✅ **Use logger.set_context()** para campos repetidos
7. ❌ **Não logue em loops intensivos** (use sampling)
8. ❌ **Não logue queries completas** (use query_hash)

## 🔒 Segurança

**NUNCA logue**:
- Senhas
- Tokens de API
- Dados pessoais (CPF, email, etc)
- Queries com valores literais sensíveis

**Use**:
- Query hashes
- IDs numéricos
- Nomes de usuário (não senhas)
- Dados agregados/sanitizados

---

**Conclusão**: O sistema de logging estruturado fornece observabilidade profunda do monitor, facilitando debug, monitoramento e análise de performance em produção.
