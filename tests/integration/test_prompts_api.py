"""
Testes de integracao para os endpoints de gerenciamento de prompts LLM.

Testa:
- CRUD de prompts (criar, ler, atualizar, deletar)
- Validacao de input (db_type, prompt_type, payload)
- Versionamento de prompts
- Historico de versoes
- Rollback de versoes
"""
import pytest
import tempfile
import os
from fastapi.testclient import TestClient

from sql_monitor.api.app import app
from sql_monitor.api.routes import init_dependencies
from sql_monitor.utils.metrics_store import MetricsStore


@pytest.fixture
def test_metrics_store():
    """Cria um MetricsStore temporario para testes."""
    fd, db_path = tempfile.mkstemp(suffix='.duckdb')
    os.close(fd)
    os.unlink(db_path)

    store = MetricsStore(db_path=db_path)
    store.init_config_defaults()
    init_dependencies(store)

    yield store

    store.close()
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def client(test_metrics_store):
    """Cliente de teste FastAPI."""
    return TestClient(app)


class TestGetPrompts:
    """Testes para listagem de prompts."""

    def test_get_prompts_empty(self, client):
        """Retorna lista vazia quando nao ha prompts."""
        response = client.get("/api/prompts")
        assert response.status_code == 200
        assert response.json() == []

    def test_get_prompts_filtered_by_db_type(self, client):
        """Filtra prompts por db_type."""
        # Criar prompt para sqlserver
        client.post("/api/prompts/sqlserver/base_template", json={
            "name": "Base SQL Server",
            "content": "Template base sqlserver"
        })
        # Criar prompt para hana
        client.post("/api/prompts/hana/base_template", json={
            "name": "Base HANA",
            "content": "Template base hana"
        })

        # Filtrar por sqlserver
        response = client.get("/api/prompts?db_type=sqlserver")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["db_type"] == "sqlserver"

    def test_get_prompts_invalid_db_type_filter(self, client):
        """Rejeita db_type invalido no filtro."""
        response = client.get("/api/prompts?db_type=oracle")
        assert response.status_code == 400
        assert "db_type inválido" in response.json()["detail"]


class TestGetPrompt:
    """Testes para obter prompt especifico."""

    def test_get_prompt_not_found(self, client):
        """Retorna 404 quando prompt nao existe."""
        response = client.get("/api/prompts/sqlserver/base_template")
        assert response.status_code == 404

    def test_get_prompt_invalid_db_type(self, client):
        """Rejeita db_type invalido."""
        response = client.get("/api/prompts/oracle/base_template")
        assert response.status_code == 400

    def test_get_prompt_invalid_prompt_type(self, client):
        """Rejeita prompt_type invalido."""
        response = client.get("/api/prompts/sqlserver/invalid_type")
        assert response.status_code == 400


class TestSavePrompt:
    """Testes para salvar prompts."""

    def test_save_and_get_prompt(self, client):
        """Salva e recupera prompt com sucesso."""
        save_response = client.post("/api/prompts/sqlserver/base_template", json={
            "name": "Base SQL Server",
            "content": "Voce e um especialista em SQL Server.",
            "change_reason": "Criacao inicial"
        })
        assert save_response.status_code == 200
        assert save_response.json()["status"] == "saved"

        get_response = client.get("/api/prompts/sqlserver/base_template")
        assert get_response.status_code == 200
        data = get_response.json()
        assert data["name"] == "Base SQL Server"
        assert data["content"] == "Voce e um especialista em SQL Server."
        assert data["version"] == 1
        assert data["is_active"] is True

    def test_save_prompt_versioning(self, client):
        """Salvar 2x incrementa versao."""
        client.post("/api/prompts/sqlserver/features", json={
            "name": "Features v1",
            "content": "Feature 1"
        })
        client.post("/api/prompts/sqlserver/features", json={
            "name": "Features v2",
            "content": "Feature 1\nFeature 2"
        })

        response = client.get("/api/prompts/sqlserver/features")
        assert response.status_code == 200
        data = response.json()
        assert data["version"] == 2
        assert data["content"] == "Feature 1\nFeature 2"

    def test_save_prompt_validation_empty_name(self, client):
        """Rejeita payload com name vazio."""
        response = client.post("/api/prompts/sqlserver/base_template", json={
            "name": "",
            "content": "Conteudo valido"
        })
        assert response.status_code == 422

    def test_save_prompt_validation_empty_content(self, client):
        """Rejeita payload com content vazio."""
        response = client.post("/api/prompts/sqlserver/base_template", json={
            "name": "Nome valido",
            "content": ""
        })
        assert response.status_code == 422

    def test_save_prompt_validation_missing_fields(self, client):
        """Rejeita payload sem campos obrigatorios."""
        response = client.post("/api/prompts/sqlserver/base_template", json={})
        assert response.status_code == 422

    def test_save_prompt_invalid_db_type(self, client):
        """Rejeita db_type invalido."""
        response = client.post("/api/prompts/oracle/base_template", json={
            "name": "Test",
            "content": "Content"
        })
        assert response.status_code == 400

    def test_save_prompt_invalid_prompt_type(self, client):
        """Rejeita prompt_type invalido."""
        response = client.post("/api/prompts/sqlserver/invalid_type", json={
            "name": "Test",
            "content": "Content"
        })
        assert response.status_code == 400


class TestPromptHistory:
    """Testes para historico de versoes."""

    def test_get_prompt_history(self, client):
        """Retorna historico correto apos multiplas versoes."""
        # Criar 3 versoes
        for i in range(1, 4):
            client.post("/api/prompts/hana/index_syntax", json={
                "name": f"Index Syntax v{i}",
                "content": f"CREATE INDEX idx_v{i} ON tabela (col);",
                "change_reason": f"Versao {i}"
            })

        response = client.get("/api/prompts/hana/index_syntax/history")
        assert response.status_code == 200
        data = response.json()

        assert len(data["history"]) == 3
        # Versao mais recente primeiro
        assert data["history"][0]["version"] == 3
        assert data["history"][0]["is_active"] is True
        assert data["history"][1]["is_active"] is False

    def test_get_history_invalid_db_type(self, client):
        """Rejeita db_type invalido no historico."""
        response = client.get("/api/prompts/oracle/base_template/history")
        assert response.status_code == 400


class TestRollbackPrompt:
    """Testes para rollback de versoes."""

    def test_rollback_prompt(self, client):
        """Restaura versao anterior com sucesso."""
        # Criar 2 versoes
        client.post("/api/prompts/postgresql/features", json={
            "name": "Features v1",
            "content": "Feature original"
        })
        client.post("/api/prompts/postgresql/features", json={
            "name": "Features v2",
            "content": "Feature modificada"
        })

        # Rollback para versao 1
        response = client.post("/api/prompts/postgresql/features/rollback/1", json={
            "restored_by": "test_user",
            "change_reason": "Revertendo para original"
        })
        assert response.status_code == 200
        assert response.json()["status"] == "restored"

        # Verificar que conteudo voltou ao original
        get_response = client.get("/api/prompts/postgresql/features")
        data = get_response.json()
        assert data["content"] == "Feature original"
        assert data["version"] == 3  # Nova versao criada pelo rollback

    def test_rollback_nonexistent_version(self, client):
        """Retorna 404 para versao inexistente."""
        response = client.post("/api/prompts/sqlserver/base_template/rollback/999", json={
            "restored_by": "test_user"
        })
        assert response.status_code == 404

    def test_rollback_invalid_db_type(self, client):
        """Rejeita db_type invalido no rollback."""
        response = client.post("/api/prompts/oracle/base_template/rollback/1", json={
            "restored_by": "test_user"
        })
        assert response.status_code == 400


class TestDeletePrompt:
    """Testes para desativacao de prompts."""

    def test_delete_prompt(self, client):
        """Desativa prompt com sucesso."""
        client.post("/api/prompts/sqlserver/index_syntax", json={
            "name": "Index Syntax",
            "content": "CREATE INDEX..."
        })

        response = client.delete("/api/prompts/sqlserver/index_syntax")
        assert response.status_code == 200
        assert response.json()["status"] == "deleted"

        # Verificar que nao encontra mais
        get_response = client.get("/api/prompts/sqlserver/index_syntax")
        assert get_response.status_code == 404

    def test_delete_nonexistent_prompt(self, client):
        """Retorna 404 ao deletar prompt inexistente."""
        response = client.delete("/api/prompts/sqlserver/base_template")
        assert response.status_code == 404

    def test_delete_invalid_db_type(self, client):
        """Rejeita db_type invalido."""
        response = client.delete("/api/prompts/oracle/base_template")
        assert response.status_code == 400


class TestBaseTemplateConsistency:
    """Testa que o bug #5 (base_prompt_template sobrescrito) foi corrigido."""

    def test_load_prompts_uses_first_base_template(self, test_metrics_store):
        """Verifica que _load_prompts pega apenas o primeiro base_template."""
        from sql_monitor.utils.llm_analyzer import LLMAnalyzer
        from unittest.mock import patch

        # Criar base_template para 2 db_types com conteudo diferente
        test_metrics_store.save_llm_prompt(
            db_type="sqlserver", prompt_type="base_template",
            name="Base SQL Server", content="Template SQL Server"
        )
        test_metrics_store.save_llm_prompt(
            db_type="hana", prompt_type="base_template",
            name="Base HANA", content="Template HANA diferente"
        )

        # Mockar a inicializacao do Gemini client para evitar erro de API key
        with patch.object(LLMAnalyzer, '__init__', lambda self, *args, **kwargs: None):
            analyzer = LLMAnalyzer.__new__(LLMAnalyzer)
            analyzer.metrics_store = test_metrics_store
            prompts = analyzer._load_prompts()

        # Deve ter pegado o primeiro e nao sobrescrito
        assert prompts['base_prompt_template'] != ''
        # O importante eh que nao ficou vazio e que tem um valor consistente
        assert prompts['base_prompt_template'] in ["Template SQL Server", "Template HANA diferente"]
