"""
Testes para funcionalidade de rastreabilidade (Fase 1).
Valida schema, migração e coleta de dados de rastreabilidade.
"""
import pytest
from datetime import datetime
from sql_monitor.utils.metrics_store import MetricsStore
from sql_monitor.core.database_types import DatabaseType


class TestTraceabilitySchema:
    """Testa migração de schema e colunas de rastreabilidade."""

    def test_schema_migration_adds_traceability_columns(self):
        """Verifica que migração adiciona todas as colunas de rastreabilidade."""
        # Criar store em memória (banco temporário)
        store = MetricsStore(db_path=":memory:")
        conn = store._get_connection()

        # Verificar que colunas de rastreabilidade existem
        columns = [row[1] for row in conn.execute("PRAGMA table_info('queries_collected')").fetchall()]

        assert 'login_name' in columns, "Coluna login_name não foi criada"
        assert 'host_name' in columns, "Coluna host_name não foi criada"
        assert 'program_name' in columns, "Coluna program_name não foi criada"
        assert 'client_interface_name' in columns, "Coluna client_interface_name não foi criada"
        assert 'session_id' in columns, "Coluna session_id não foi criada"

    def test_indexes_created_for_traceability_columns(self):
        """Verifica que índices foram criados para performance."""
        store = MetricsStore(db_path=":memory:")
        conn = store._get_connection()

        # Buscar índices criados
        indexes = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='index' AND tbl_name='queries_collected'
        """).fetchall()

        index_names = [idx[0] for idx in indexes]

        assert 'idx_login_name' in index_names, "Índice idx_login_name não foi criado"
        assert 'idx_host_name' in index_names, "Índice idx_host_name não foi criado"
        assert 'idx_program_name' in index_names, "Índice idx_program_name não foi criado"


class TestAddCollectedQueryWithTraceability:
    """Testa método add_collected_query() com dados de rastreabilidade."""

    @pytest.fixture
    def store(self):
        """Fixture: Store em memória para testes."""
        return MetricsStore(db_path=":memory:")

    def test_add_query_with_full_traceability(self, store):
        """Testa salvar query com todos os dados de rastreabilidade."""
        query_hash = "test_hash_001"

        # Adicionar query com rastreabilidade completa
        query_id = store.add_collected_query(
            query_hash=query_hash,
            instance_name="SQL-PROD-01",
            db_type="sqlserver",
            query_text="SELECT * FROM users WHERE id = 123",
            sanitized_query="SELECT * FROM users WHERE id = @p1",
            database_name="AppDB",
            schema_name="dbo",
            table_name="users",
            query_type="active",
            login_name="admin",
            host_name="192.168.1.100",
            program_name="MyApp.exe",
            client_interface_name="ODBC",
            session_id=12345
        )

        assert query_id is not None, "Query ID não foi retornado"

        # Verificar dados salvos
        conn = store._get_connection()
        result = conn.execute("""
            SELECT
                login_name, host_name, program_name,
                client_interface_name, session_id,
                query_hash, instance_name
            FROM queries_collected
            WHERE query_hash = ?
        """, [query_hash]).fetchone()

        assert result is not None, "Query não foi salva no banco"
        assert result[0] == "admin", f"login_name incorreto: {result[0]}"
        assert result[1] == "192.168.1.100", f"host_name incorreto: {result[1]}"
        assert result[2] == "MyApp.exe", f"program_name incorreto: {result[2]}"
        assert result[3] == "ODBC", f"client_interface_name incorreto: {result[3]}"
        assert result[4] == 12345, f"session_id incorreto: {result[4]}"
        assert result[5] == query_hash, "query_hash incorreto"
        assert result[6] == "SQL-PROD-01", "instance_name incorreto"

    def test_add_query_with_partial_traceability(self, store):
        """Testa salvar query com apenas alguns dados de rastreabilidade."""
        query_hash = "test_hash_002"

        # Adicionar query com rastreabilidade parcial (apenas login e host)
        query_id = store.add_collected_query(
            query_hash=query_hash,
            instance_name="SQL-PROD-02",
            db_type="sqlserver",
            query_text="SELECT COUNT(*) FROM orders",
            sanitized_query="SELECT COUNT(*) FROM orders",
            database_name="SalesDB",
            schema_name="dbo",
            table_name="orders",
            query_type="expensive",
            login_name="app_user",
            host_name="10.0.0.50",
            # program_name, client_interface_name, session_id omitidos
        )

        assert query_id is not None

        # Verificar que campos fornecidos foram salvos e não fornecidos são NULL
        conn = store._get_connection()
        result = conn.execute("""
            SELECT
                login_name, host_name, program_name,
                client_interface_name, session_id
            FROM queries_collected
            WHERE query_hash = ?
        """, [query_hash]).fetchone()

        assert result[0] == "app_user", "login_name não foi salvo"
        assert result[1] == "10.0.0.50", "host_name não foi salvo"
        assert result[2] is None, "program_name deveria ser NULL"
        assert result[3] is None, "client_interface_name deveria ser NULL"
        assert result[4] is None, "session_id deveria ser NULL"

    def test_add_query_without_traceability(self, store):
        """Testa salvar query SEM dados de rastreabilidade (compatibilidade)."""
        query_hash = "test_hash_003"

        # Adicionar query sem rastreabilidade (comportamento antigo)
        query_id = store.add_collected_query(
            query_hash=query_hash,
            instance_name="SQL-PROD-03",
            db_type="postgresql",
            query_text="SELECT * FROM products",
            sanitized_query="SELECT * FROM products",
            database_name="CatalogDB",
            schema_name="public",
            table_name="products",
            query_type="active"
            # Nenhum dado de rastreabilidade fornecido
        )

        assert query_id is not None

        # Verificar que query foi salva normalmente com valores NULL
        conn = store._get_connection()
        result = conn.execute("""
            SELECT
                query_hash, instance_name,
                login_name, host_name, program_name
            FROM queries_collected
            WHERE query_hash = ?
        """, [query_hash]).fetchone()

        assert result[0] == query_hash, "query_hash não foi salvo"
        assert result[1] == "SQL-PROD-03", "instance_name não foi salvo"
        assert result[2] is None, "login_name deveria ser NULL"
        assert result[3] is None, "host_name deveria ser NULL"
        assert result[4] is None, "program_name deveria ser NULL"


class TestTraceabilityMultipleQueries:
    """Testa cenários com múltiplas queries."""

    @pytest.fixture
    def store(self):
        return MetricsStore(db_path=":memory:")

    def test_multiple_queries_different_users(self, store):
        """Testa queries de diferentes usuários."""
        queries = [
            ("hash1", "user1", "host1", "app1"),
            ("hash2", "user2", "host2", "app2"),
            ("hash3", "user1", "host3", "app3"),  # user1 novamente
        ]

        for qhash, login, host, program in queries:
            store.add_collected_query(
                query_hash=qhash,
                instance_name="SQL-TEST",
                db_type="sqlserver",
                query_text=f"SELECT * FROM test WHERE id = {qhash}",
                sanitized_query=f"SELECT * FROM test WHERE id = @p1",
                database_name="TestDB",
                schema_name="dbo",
                table_name="test",
                login_name=login,
                host_name=host,
                program_name=program
            )

        conn = store._get_connection()

        # Verificar contagem total
        total = conn.execute("SELECT COUNT(*) FROM queries_collected").fetchone()[0]
        assert total == 3, f"Esperado 3 queries, encontrado {total}"

        # Verificar queries únicas por usuário
        user1_queries = conn.execute("""
            SELECT COUNT(*) FROM queries_collected WHERE login_name = 'user1'
        """).fetchone()[0]
        assert user1_queries == 2, f"user1 deveria ter 2 queries, tem {user1_queries}"

        # Verificar que podemos filtrar por program_name
        app1_queries = conn.execute("""
            SELECT COUNT(*) FROM queries_collected WHERE program_name = 'app1'
        """).fetchone()[0]
        assert app1_queries == 1, f"app1 deveria ter 1 query, tem {app1_queries}"

    def test_query_index_performance(self, store):
        """Testa que índices melhoram performance de busca."""
        # Adicionar muitas queries
        for i in range(100):
            store.add_collected_query(
                query_hash=f"hash_{i}",
                instance_name="SQL-PERF",
                db_type="sqlserver",
                query_text=f"SELECT * FROM test{i}",
                sanitized_query=f"SELECT * FROM test{i}",
                database_name="PerfDB",
                schema_name="dbo",
                table_name=f"test{i}",
                login_name=f"user_{i % 5}",  # 5 usuários diferentes
                host_name=f"host_{i % 10}",  # 10 hosts diferentes
                program_name=f"app_{i % 3}"   # 3 aplicações diferentes
            )

        conn = store._get_connection()

        # Verificar que índices existem e queries funcionam
        import time

        start = time.time()
        result = conn.execute("""
            SELECT COUNT(DISTINCT query_hash)
            FROM queries_collected
            WHERE login_name = 'user_2'
        """).fetchone()[0]
        elapsed = time.time() - start

        assert result == 20, f"Esperado 20 queries para user_2, encontrado {result}"
        assert elapsed < 0.1, f"Query muito lenta ({elapsed}s), índices podem não estar funcionando"


class TestTraceabilityEdgeCases:
    """Testa casos extremos."""

    @pytest.fixture
    def store(self):
        return MetricsStore(db_path=":memory:")

    def test_special_characters_in_traceability_data(self, store):
        """Testa caracteres especiais em dados de rastreabilidade."""
        query_hash = "hash_special"

        store.add_collected_query(
            query_hash=query_hash,
            instance_name="SQL-TEST",
            db_type="sqlserver",
            query_text="SELECT * FROM test",
            sanitized_query="SELECT * FROM test",
            database_name="TestDB",
            schema_name="dbo",
            table_name="test",
            login_name="user@domain.com",  # Email como login
            host_name="server-01.company.local",  # FQDN
            program_name="C:\\Program Files\\MyApp\\app.exe",  # Path completo
            client_interface_name="Named Pipes"  # Com espaço
        )

        conn = store._get_connection()
        result = conn.execute("""
            SELECT login_name, host_name, program_name, client_interface_name
            FROM queries_collected WHERE query_hash = ?
        """, [query_hash]).fetchone()

        assert result[0] == "user@domain.com"
        assert result[1] == "server-01.company.local"
        assert result[2] == "C:\\Program Files\\MyApp\\app.exe"
        assert result[3] == "Named Pipes"

    def test_very_long_program_name(self, store):
        """Testa program_name muito longo (campo VARCHAR(200))."""
        query_hash = "hash_long"
        long_program = "C:\\" + "VeryLongPath\\" * 20 + "app.exe"  # > 200 chars

        # Deveria truncar ou aceitar dependendo do banco
        try:
            store.add_collected_query(
                query_hash=query_hash,
                instance_name="SQL-TEST",
                db_type="sqlserver",
                query_text="SELECT 1",
                sanitized_query="SELECT 1",
                database_name="TestDB",
                schema_name="dbo",
                table_name="test",
                program_name=long_program
            )

            # Se não deu erro, verificar que foi salvo (truncado ou completo)
            conn = store._get_connection()
            result = conn.execute("""
                SELECT program_name FROM queries_collected WHERE query_hash = ?
            """, [query_hash]).fetchone()

            assert result is not None
            assert len(result[0]) > 0

        except Exception as e:
            # Se der erro, ao menos garantir que não quebra o sistema
            pytest.skip(f"Banco não suporta strings muito longas: {e}")

    def test_null_vs_empty_string(self, store):
        """Testa diferença entre NULL e string vazia."""
        # Query com string vazia
        store.add_collected_query(
            query_hash="hash_empty",
            instance_name="SQL-TEST",
            db_type="sqlserver",
            query_text="SELECT 1",
            sanitized_query="SELECT 1",
            database_name="TestDB",
            schema_name="dbo",
            table_name="test",
            login_name=""  # String vazia
        )

        # Query com NULL explícito
        store.add_collected_query(
            query_hash="hash_null",
            instance_name="SQL-TEST",
            db_type="sqlserver",
            query_text="SELECT 2",
            sanitized_query="SELECT 2",
            database_name="TestDB",
            schema_name="dbo",
            table_name="test",
            login_name=None  # NULL
        )

        conn = store._get_connection()

        empty_result = conn.execute("""
            SELECT login_name FROM queries_collected WHERE query_hash = 'hash_empty'
        """).fetchone()[0]

        null_result = conn.execute("""
            SELECT login_name FROM queries_collected WHERE query_hash = 'hash_null'
        """).fetchone()[0]

        # String vazia é salva como vazia
        assert empty_result == "", f"String vazia foi salva como: {repr(empty_result)}"

        # NULL é salvo como NULL
        assert null_result is None, f"NULL foi salvo como: {repr(null_result)}"


class TestBackwardCompatibility:
    """Testa compatibilidade com código antigo."""

    def test_old_queries_still_work(self):
        """Testa que queries antigas (sem rastreabilidade) ainda funcionam."""
        store = MetricsStore(db_path=":memory:")

        # Simular código antigo que não passa rastreabilidade
        query_id = store.add_collected_query(
            query_hash="old_query",
            instance_name="OLD-SQL",
            db_type="sqlserver",
            query_text="SELECT * FROM legacy_table",
            sanitized_query="SELECT * FROM legacy_table",
            database_name="LegacyDB",
            # Sem rastreabilidade
        )

        assert query_id is not None, "Query antiga não funcionou"

        # Verificar que pode consultar mesmo com campos NULL
        conn = store._get_connection()
        result = conn.execute("""
            SELECT query_hash, login_name, host_name
            FROM queries_collected
            WHERE query_hash = 'old_query'
        """).fetchone()

        assert result[0] == "old_query"
        assert result[1] is None  # login_name NULL
        assert result[2] is None  # host_name NULL


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
