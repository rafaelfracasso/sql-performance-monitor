"""
Testes de integração para os endpoints de configuração.

Testa:
- API REST de thresholds
- API REST de collection settings
- API REST de cache config
- API REST de auditoria
"""
import pytest
import tempfile
import os
import json
from fastapi.testclient import TestClient

from sql_monitor.api.app import app
from sql_monitor.api.routes import init_dependencies
from sql_monitor.utils.metrics_store import MetricsStore


@pytest.fixture
def test_metrics_store():
    """Cria um MetricsStore temporário para testes."""
    # Criar path temporário sem criar o arquivo (DuckDB vai criar)
    fd, db_path = tempfile.mkstemp(suffix='.duckdb')
    os.close(fd)  # Fechar file descriptor
    os.unlink(db_path)  # Remover arquivo vazio

    store = MetricsStore(db_path=db_path)
    store.init_config_defaults()

    # Inicializar dependências da API
    init_dependencies(store)

    yield store

    # Cleanup
    store.close()
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def client(test_metrics_store):
    """Cliente de teste FastAPI."""
    return TestClient(app)


class TestThresholdsAPI:
    """Testes para API de thresholds."""

    def test_get_thresholds(self, client):
        """Testa listagem de thresholds."""
        response = client.get("/api/settings/thresholds")

        assert response.status_code == 200
        data = response.json()

        assert isinstance(data, list)
        assert len(data) == 3  # hana, sqlserver, postgresql

        # Verificar estrutura
        hana = [t for t in data if t['db_type'] == 'hana'][0]
        assert 'execution_time_ms' in hana
        assert 'cpu_time_ms' in hana
        assert hana['execution_time_ms'] == 5000.0

    def test_update_thresholds(self, client):
        """Testa atualização de thresholds."""
        new_thresholds = {
            'execution_time_ms': 200.0,
            'cpu_time_ms': 100.0,
            'logical_reads': 20000,
            'physical_reads': 2000,
            'writes': 1000,
            'wait_time_ms': 2000,
            'memory_mb': 400,
            'row_count': 60000
        }

        response = client.post("/api/settings/thresholds/hana", json=new_thresholds)

        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'updated'
        assert data['db_type'] == 'hana'

        # Verificar se foi atualizado
        response = client.get("/api/settings/thresholds")
        thresholds = response.json()

        hana = [t for t in thresholds if t['db_type'] == 'hana'][0]
        assert hana['execution_time_ms'] == 200.0
        assert hana['cpu_time_ms'] == 100.0

    def test_update_thresholds_invalid_dbtype(self, client):
        """Testa atualização com dbtype inválido."""
        response = client.post("/api/settings/thresholds/oracle", json={})

        assert response.status_code == 400
        assert 'inválido' in response.json()['detail']

    def test_update_thresholds_negative_value(self, client):
        """Testa atualização com valor negativo."""
        response = client.post("/api/settings/thresholds/hana", json={
            'execution_time_ms': -100
        })

        assert response.status_code == 400
        assert 'deve ser >= -1' in response.json()['detail']


class TestCollectionSettingsAPI:
    """Testes para API de collection settings."""

    def test_get_collection_settings(self, client):
        """Testa listagem de collection settings."""
        response = client.get("/api/settings/collection")

        assert response.status_code == 200
        data = response.json()

        assert isinstance(data, list)
        assert len(data) == 3

        # Verificar HANA não coleta active queries
        hana = [s for s in data if s['db_type'] == 'hana'][0]
        assert hana['collect_active_queries'] is False
        assert hana['min_duration_seconds'] == 0.001

    def test_update_collection_settings(self, client):
        """Testa atualização de collection settings."""
        new_settings = {
            'min_duration_seconds': 0.5,
            'collect_active_queries': True,
            'collect_expensive_queries': True,
            'collect_table_scans': False,
            'max_queries_per_cycle': 20
        }

        response = client.post("/api/settings/collection/hana", json=new_settings)

        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'updated'

        # Verificar atualização
        response = client.get("/api/settings/collection")
        settings = response.json()

        hana = [s for s in settings if s['db_type'] == 'hana'][0]
        assert hana['min_duration_seconds'] == 0.5
        assert hana['collect_active_queries'] is True


class TestCacheConfigAPI:
    """Testes para API de cache config."""

    def test_get_cache_config(self, client):
        """Testa obtenção de cache config."""
        response = client.get("/api/settings/cache")

        assert response.status_code == 200
        data = response.json()

        assert data['enabled'] is True
        assert data['ttl_hours'] == 24
        assert data['max_entries'] == 1000

    def test_update_cache_config(self, client):
        """Testa atualização de cache config."""
        new_config = {
            'enabled': False,
            'ttl_hours': 48,
            'max_entries': 500,
            'cache_ddl': False,
            'cache_indexes': True
        }

        response = client.post("/api/settings/cache", json=new_config)

        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'updated'

        # Verificar atualização
        response = client.get("/api/settings/cache")
        config = response.json()

        assert config['enabled'] is False
        assert config['ttl_hours'] == 48
        assert config['max_entries'] == 500

    def test_clear_cache(self, client):
        """Testa limpeza de cache."""
        response = client.post("/api/settings/cache/clear")

        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'cleared'


class TestResetToDefaultsAPI:
    """Testes para reset de configurações."""

    def test_reset_to_defaults(self, client):
        """Testa reset para defaults."""
        # Primeiro modificar
        client.post("/api/settings/thresholds/hana", json={
            'execution_time_ms': 500.0,
            'cpu_time_ms': 200.0,
            'logical_reads': 50000,
            'physical_reads': 5000,
            'writes': 2000,
            'wait_time_ms': 3000,
            'memory_mb': 600,
            'row_count': 80000
        })

        # Verificar modificação
        response = client.get("/api/settings/thresholds")
        hana = [t for t in response.json() if t['db_type'] == 'hana'][0]
        assert hana['execution_time_ms'] == 500.0

        # Resetar
        response = client.post("/api/settings/reset/hana")
        assert response.status_code == 200

        # Verificar reset
        response = client.get("/api/settings/thresholds")
        hana = [t for t in response.json() if t['db_type'] == 'hana'][0]
        assert hana['execution_time_ms'] == 5000.0  # Valor padrão

    def test_reset_invalid_dbtype(self, client):
        """Testa reset com dbtype inválido."""
        response = client.post("/api/settings/reset/oracle")

        assert response.status_code == 400
        assert 'inválido' in response.json()['detail']


class TestAuditLogAPI:
    """Testes para API de audit log."""

    def test_get_audit_log_empty(self, client):
        """Testa audit log vazio."""
        response = client.get("/api/settings/audit")

        assert response.status_code == 200
        data = response.json()

        assert isinstance(data, list)
        # Pode ter alguns registros do init_config_defaults, mas deve ser lista

    def test_audit_log_records_changes(self, client, test_metrics_store):
        """Testa se mudanças são registradas no audit log."""
        # Fazer uma mudança
        client.post("/api/settings/thresholds/hana", json={
            'execution_time_ms': 300.0,
            'cpu_time_ms': 150.0,
            'logical_reads': 15000,
            'physical_reads': 1500,
            'writes': 750,
            'wait_time_ms': 1500,
            'memory_mb': 300,
            'row_count': 55000
        })

        # Verificar audit log
        response = client.get("/api/settings/audit")
        audit_log = response.json()

        # Deve ter pelo menos 1 registro
        assert len(audit_log) > 0

        # Verificar estrutura
        latest = audit_log[0]
        assert 'changed_at' in latest
        assert 'changed_by' in latest
        assert 'config_table' in latest

    def test_audit_log_limit(self, client, test_metrics_store):
        """Testa limite de registros no audit log."""
        # Criar múltiplas mudanças
        for i in range(5):
            client.post("/api/settings/thresholds/hana", json={
                'execution_time_ms': 100.0 + i,
                'cpu_time_ms': 50.0,
                'logical_reads': 10000,
                'physical_reads': 1000,
                'writes': 500,
                'wait_time_ms': 1000,
                'memory_mb': 200,
                'row_count': 50000
            })

        # Buscar com limite
        response = client.get("/api/settings/audit?limit=3")
        audit_log = response.json()

        assert len(audit_log) <= 3


class TestSettingsDashboard:
    """Testes para página HTML de configurações."""

    def test_settings_page_loads(self, client):
        """Testa se página de configurações carrega."""
        response = client.get("/settings")

        assert response.status_code == 200
        assert 'text/html' in response.headers['content-type']

        # Verificar conteúdo básico
        html = response.text
        assert 'Configuracoes' in html
        assert 'Thresholds' in html or 'HANA' in html


class TestAPIIntegration:
    """Testes de integração completos."""

    def test_full_workflow(self, client):
        """Testa workflow completo: listar -> modificar -> verificar -> resetar."""
        # 1. Listar thresholds iniciais
        response = client.get("/api/settings/thresholds")
        initial_thresholds = response.json()
        hana_initial = [t for t in initial_thresholds if t['db_type'] == 'hana'][0]

        # 2. Modificar
        new_values = {
            'execution_time_ms': 250.0,
            'cpu_time_ms': 125.0,
            'logical_reads': 12500,
            'physical_reads': 1250,
            'writes': 625,
            'wait_time_ms': 1250,
            'memory_mb': 250,
            'row_count': 52500
        }
        client.post("/api/settings/thresholds/hana", json=new_values)

        # 3. Verificar modificação
        response = client.get("/api/settings/thresholds")
        modified_thresholds = response.json()
        hana_modified = [t for t in modified_thresholds if t['db_type'] == 'hana'][0]

        assert hana_modified['execution_time_ms'] == 250.0
        assert hana_modified['execution_time_ms'] != hana_initial['execution_time_ms']

        # 4. Resetar
        client.post("/api/settings/reset/hana")

        # 5. Verificar reset
        response = client.get("/api/settings/thresholds")
        reset_thresholds = response.json()
        hana_reset = [t for t in reset_thresholds if t['db_type'] == 'hana'][0]

        assert hana_reset['execution_time_ms'] == hana_initial['execution_time_ms']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
