"""
Testes unitários para o sistema de configurações estruturadas no DuckDB.

Testa:
- PerformanceChecker carregando do DuckDB
- MetadataCache
- MetricsStore.init_config_defaults()
"""
import pytest
import tempfile
import os
from datetime import datetime, timedelta

from sql_monitor.utils.metrics_store import MetricsStore
from sql_monitor.utils.performance_checker import PerformanceChecker
from sql_monitor.utils.metadata_cache import MetadataCache


@pytest.fixture
def temp_metrics_store():
    """Cria um MetricsStore temporário para testes."""
    # Criar path temporário sem criar o arquivo (DuckDB vai criar)
    fd, db_path = tempfile.mkstemp(suffix='.duckdb')
    os.close(fd)  # Fechar file descriptor
    os.unlink(db_path)  # Remover arquivo vazio

    store = MetricsStore(db_path=db_path)
    store.init_config_defaults()

    yield store

    # Cleanup
    store.close()
    if os.path.exists(db_path):
        os.unlink(db_path)


class TestMetricsStoreConfigDefaults:
    """Testes para inicialização de configurações padrão."""

    def test_init_config_defaults_creates_thresholds(self, temp_metrics_store):
        """Testa se init_config_defaults cria thresholds para todos os dbtypes."""
        store = temp_metrics_store

        # Verificar se criou para hana, sqlserver, postgresql
        results = store.execute_query(
            "SELECT db_type FROM performance_thresholds_by_dbtype ORDER BY db_type"
        )

        db_types = [r[0] for r in results]
        assert 'hana' in db_types
        assert 'sqlserver' in db_types
        assert 'postgresql' in db_types

    def test_init_config_defaults_hana_values(self, temp_metrics_store):
        """Testa se HANA tem thresholds adequados (5000ms, cpu/reads desabilitados)."""
        store = temp_metrics_store

        result = store.execute_query(
            "SELECT execution_time_ms, cpu_time_ms, logical_reads, memory_mb FROM performance_thresholds_by_dbtype WHERE db_type = 'hana'"
        )

        assert len(result) == 1
        execution_time_ms, cpu_time_ms, logical_reads, memory_mb = result[0]

        assert execution_time_ms == 5000.0  # 5s para HANA
        assert cpu_time_ms == -1.0  # Desabilitado (não disponível no HANA)
        assert logical_reads == -1  # Desabilitado (proxy impreciso para HANA)
        assert memory_mb == 500.0  # Memória é métrica principal para HANA

    def test_init_config_defaults_sqlserver_values(self, temp_metrics_store):
        """Testa se SQL Server tem thresholds padrão (30000ms)."""
        store = temp_metrics_store

        result = store.execute_query(
            "SELECT execution_time_ms, cpu_time_ms FROM performance_thresholds_by_dbtype WHERE db_type = 'sqlserver'"
        )

        assert len(result) == 1
        execution_time_ms, cpu_time_ms = result[0]

        assert execution_time_ms == 30000.0  # 30s para SQL Server
        assert cpu_time_ms == 10000.0

    def test_init_config_defaults_collection_settings(self, temp_metrics_store):
        """Testa se collection settings são criados."""
        store = temp_metrics_store

        results = store.execute_query(
            "SELECT db_type, collect_active_queries FROM collection_settings_by_dbtype ORDER BY db_type"
        )

        assert len(results) == 3

        # HANA não deve coletar active queries
        hana_result = [r for r in results if r[0] == 'hana'][0]
        assert hana_result[1] is False  # collect_active_queries = False

        # SQL Server deve coletar
        sqlserver_result = [r for r in results if r[0] == 'sqlserver'][0]
        assert sqlserver_result[1] is True

    def test_init_config_defaults_cache_config(self, temp_metrics_store):
        """Testa se cache config é criado."""
        store = temp_metrics_store

        result = store.execute_query(
            "SELECT enabled, ttl_hours, max_entries FROM metadata_cache_config WHERE id = 1"
        )

        assert len(result) == 1
        enabled, ttl_hours, max_entries = result[0]

        assert enabled is True
        assert ttl_hours == 24
        assert max_entries == 1000

    def test_init_config_defaults_idempotent(self, temp_metrics_store):
        """Testa se init_config_defaults é idempotente (pode ser chamado múltiplas vezes)."""
        store = temp_metrics_store

        # Chamar novamente
        store.init_config_defaults()

        # Não deve duplicar
        results = store.execute_query(
            "SELECT COUNT(*) FROM performance_thresholds_by_dbtype"
        )

        assert results[0][0] == 3  # Apenas 3 dbtypes


class TestPerformanceCheckerWithDuckDB:
    """Testes para PerformanceChecker carregando do DuckDB."""

    def test_performance_checker_loads_from_duckdb(self, temp_metrics_store):
        """Testa se PerformanceChecker carrega thresholds do DuckDB."""
        store = temp_metrics_store

        checker = PerformanceChecker(metrics_store=store, db_type='hana')

        # Verificar se carregou corretamente
        assert checker.execution_time_seconds == 5.0  # 5000ms -> 5.0s
        assert checker.cpu_time_ms == -1.0  # Desabilitado para HANA
        assert checker.logical_reads == -1  # Desabilitado para HANA
        assert checker.memory_mb == 500.0

    def test_performance_checker_different_dbtypes(self, temp_metrics_store):
        """Testa se diferentes dbtypes carregam thresholds corretos."""
        store = temp_metrics_store

        checker_hana = PerformanceChecker(metrics_store=store, db_type='hana')
        checker_sqlserver = PerformanceChecker(metrics_store=store, db_type='sqlserver')

        # HANA deve ter 5000ms, SQL Server 30000ms
        assert checker_hana.execution_time_seconds == 5.0
        assert checker_sqlserver.execution_time_seconds == 30.0

    def test_performance_checker_invalid_dbtype(self, temp_metrics_store):
        """Testa se levanta erro para dbtype inexistente."""
        store = temp_metrics_store

        with pytest.raises(ValueError, match="Nenhum threshold encontrado"):
            PerformanceChecker(metrics_store=store, db_type='oracle')

    def test_performance_checker_is_problematic(self, temp_metrics_store):
        """Testa se is_problematic funciona com thresholds do DuckDB."""
        store = temp_metrics_store
        checker = PerformanceChecker(metrics_store=store, db_type='hana')

        # Query que excede threshold de HANA (5000ms = 5s)
        query_info = {
            'duration_seconds': 6.0,  # 6s > 5s threshold
            'cpu_time_ms': 0,
            'logical_reads': 0,
            'memory_mb': 100
        }

        assert checker.is_problematic(query_info) is True

        # Query que excede threshold de memória HANA (500MB)
        query_info_memory = {
            'duration_seconds': 1.0,
            'cpu_time_ms': 0,
            'logical_reads': 0,
            'memory_mb': 600  # > 500MB threshold
        }

        assert checker.is_problematic(query_info_memory) is True

        # Query dentro do threshold
        query_info_ok = {
            'duration_seconds': 1.0,  # 1s < 5s threshold
            'cpu_time_ms': 0,
            'logical_reads': 0,
            'memory_mb': 100
        }

        assert checker.is_problematic(query_info_ok) is False

    def test_performance_checker_reload_thresholds(self, temp_metrics_store):
        """Testa se reload_thresholds atualiza valores."""
        store = temp_metrics_store
        checker = PerformanceChecker(metrics_store=store, db_type='hana')

        old_value = checker.execution_time_seconds

        # Atualizar threshold no DuckDB
        store.execute(
            "UPDATE performance_thresholds_by_dbtype SET execution_time_ms = 200 WHERE db_type = 'hana'"
        )

        # Recarregar
        checker.reload_thresholds()

        assert checker.execution_time_seconds == 0.2  # 200ms
        assert checker.execution_time_seconds != old_value

    def test_performance_checker_requires_parameters(self):
        """Testa se levanta erro quando não recebe parâmetros."""
        with pytest.raises(TypeError):
            PerformanceChecker()


class TestMetadataCache:
    """Testes para cache de metadados."""

    def test_cache_initialization(self, temp_metrics_store):
        """Testa inicialização do cache."""
        cache = MetadataCache(temp_metrics_store)

        assert cache.enabled is True
        assert cache.ttl_hours == 24
        assert cache.max_entries == 1000

    def test_cache_get_or_fetch_miss(self, temp_metrics_store):
        """Testa cache miss (primeira chamada)."""
        cache = MetadataCache(temp_metrics_store)

        call_count = 0

        def fetch_func():
            nonlocal call_count
            call_count += 1
            return "fetched_value"

        result = cache.get_or_fetch("test_key", fetch_func)

        assert result == "fetched_value"
        assert call_count == 1

    def test_cache_get_or_fetch_hit(self, temp_metrics_store):
        """Testa cache hit (segunda chamada não executa fetch)."""
        cache = MetadataCache(temp_metrics_store)

        call_count = 0

        def fetch_func():
            nonlocal call_count
            call_count += 1
            return "fetched_value"

        # Primeira chamada
        result1 = cache.get_or_fetch("test_key", fetch_func)

        # Segunda chamada (deve usar cache)
        result2 = cache.get_or_fetch("test_key", fetch_func)

        assert result1 == result2
        assert call_count == 1  # Chamado apenas 1 vez

    def test_cache_disabled(self, temp_metrics_store):
        """Testa cache desabilitado (sempre executa fetch)."""
        # Desabilitar cache
        temp_metrics_store.execute(
            "UPDATE metadata_cache_config SET enabled = false WHERE id = 1"
        )

        cache = MetadataCache(temp_metrics_store)

        call_count = 0

        def fetch_func():
            nonlocal call_count
            call_count += 1
            return "fetched_value"

        # Múltiplas chamadas
        cache.get_or_fetch("test_key", fetch_func)
        cache.get_or_fetch("test_key", fetch_func)

        assert call_count == 2  # Chamado 2 vezes (cache desabilitado)

    def test_cache_clear(self, temp_metrics_store):
        """Testa limpeza do cache."""
        cache = MetadataCache(temp_metrics_store)

        cache.get_or_fetch("key1", lambda: "value1")
        cache.get_or_fetch("key2", lambda: "value2")

        assert len(cache.cache) == 2

        cache.clear()

        assert len(cache.cache) == 0

    def test_cache_clear_prefix(self, temp_metrics_store):
        """Testa limpeza parcial do cache por prefixo."""
        cache = MetadataCache(temp_metrics_store)

        cache.get_or_fetch("HANA.SCHEMA.TABLE1:ddl", lambda: "ddl1")
        cache.get_or_fetch("HANA.SCHEMA.TABLE2:ddl", lambda: "ddl2")
        cache.get_or_fetch("SQL.SCHEMA.TABLE1:ddl", lambda: "ddl3")

        assert len(cache.cache) == 3

        # Limpar apenas HANA
        cache.clear_prefix("HANA")

        assert len(cache.cache) == 1
        assert "SQL.SCHEMA.TABLE1:ddl" in cache.cache

    def test_cache_max_entries(self, temp_metrics_store):
        """Testa limite máximo de entries."""
        # Configurar limite baixo
        temp_metrics_store.execute(
            "UPDATE metadata_cache_config SET max_entries = 2 WHERE id = 1"
        )

        cache = MetadataCache(temp_metrics_store)

        # Adicionar 3 entries (deve remover o mais antigo)
        cache.get_or_fetch("key1", lambda: "value1")
        cache.get_or_fetch("key2", lambda: "value2")
        cache.get_or_fetch("key3", lambda: "value3")

        assert len(cache.cache) == 2
        assert "key1" not in cache.cache  # Mais antigo foi removido

    def test_cache_get_stats(self, temp_metrics_store):
        """Testa estatísticas do cache."""
        cache = MetadataCache(temp_metrics_store)

        cache.get_or_fetch("key1", lambda: "value1")
        cache.get_or_fetch("key2", lambda: "value2")

        stats = cache.get_stats()

        assert stats['enabled'] is True
        assert stats['ttl_hours'] == 24
        assert stats['current_entries'] == 2

    def test_cache_reload_config(self, temp_metrics_store):
        """Testa reload de configuração."""
        cache = MetadataCache(temp_metrics_store)

        assert cache.ttl_hours == 24

        # Atualizar config no DuckDB
        temp_metrics_store.execute(
            "UPDATE metadata_cache_config SET ttl_hours = 48 WHERE id = 1"
        )

        # Recarregar
        cache.reload_config()

        assert cache.ttl_hours == 48


class TestConfigAudit:
    """Testes para auditoria de configurações."""

    def test_audit_log_records_changes(self, temp_metrics_store):
        """Testa se mudanças são registradas no audit log."""
        store = temp_metrics_store

        # Registrar mudança
        store.execute("""
            INSERT INTO config_audit_log (changed_by, config_table, config_key, old_value, new_value)
            VALUES ('test_user', 'thresholds', 'hana', 'old', 'new')
        """)

        # Verificar
        result = store.execute_query(
            "SELECT changed_by, config_table, config_key FROM config_audit_log"
        )

        assert len(result) == 1
        assert result[0][0] == 'test_user'
        assert result[0][1] == 'thresholds'
        assert result[0][2] == 'hana'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
