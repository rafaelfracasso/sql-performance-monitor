"""
Testes para verificar o fix N+1 no HANA extractor.

Valida que get_table_indexes executa apenas 1 query em vez de N+1.
"""
import pytest
from unittest.mock import Mock, MagicMock, call

from sql_monitor.extractors.hana_extractor import HANAExtractor


class TestHANAIndexesN1Fix:
    """Testes para fix N+1 em get_table_indexes."""

    def test_get_table_indexes_single_query(self):
        """Testa se get_table_indexes executa apenas 1 query (não N+1)."""
        # Mock connection
        mock_connection = Mock()

        # Simular resultado com 3 índices agregados
        mock_connection.execute_query.return_value = [
            ('PK_USERS', 'BTREE', 'PRIMARY KEY', 'ID'),
            ('IDX_EMAIL', 'BTREE', 'UNIQUE', 'EMAIL'),
            ('IDX_NAME', 'BTREE', None, 'FIRST_NAME, LAST_NAME')
        ]

        extractor = HANAExtractor(mock_connection)

        # Executar
        result = extractor.get_table_indexes('TESTDB', 'SCHEMA', 'USERS')

        # Verificar que execute_query foi chamado APENAS 1 VEZ
        assert mock_connection.execute_query.call_count == 1

        # Verificar que a query usa STRING_AGG (agregação)
        query_call = mock_connection.execute_query.call_args
        query_text = query_call[0][0]

        assert 'STRING_AGG' in query_text
        assert 'GROUP BY' in query_text
        assert 'LEFT JOIN SYS.INDEX_COLUMNS' in query_text

        # Verificar resultado
        assert len(result) == 3

        assert result[0]['name'] == 'PK_USERS'
        assert result[0]['is_primary_key'] is True
        assert result[0]['key_columns'] == 'ID'

        assert result[1]['name'] == 'IDX_EMAIL'
        assert result[1]['is_unique'] is True

        assert result[2]['name'] == 'IDX_NAME'
        assert result[2]['key_columns'] == 'FIRST_NAME, LAST_NAME'

    def test_get_table_indexes_no_indexes(self):
        """Testa comportamento quando não há índices."""
        mock_connection = Mock()
        mock_connection.execute_query.return_value = []

        extractor = HANAExtractor(mock_connection)

        result = extractor.get_table_indexes('TESTDB', 'SCHEMA', 'TABLE')

        assert result == []
        assert mock_connection.execute_query.call_count == 1

    def test_get_table_indexes_null_columns(self):
        """Testa comportamento quando índice não tem colunas."""
        mock_connection = Mock()

        # Índice sem colunas (edge case)
        mock_connection.execute_query.return_value = [
            ('IDX_EMPTY', 'BTREE', None, None)
        ]

        extractor = HANAExtractor(mock_connection)

        result = extractor.get_table_indexes('TESTDB', 'SCHEMA', 'TABLE')

        assert len(result) == 1
        assert result[0]['key_columns'] == ''

    def test_get_table_indexes_parameters(self):
        """Testa se parâmetros são passados corretamente."""
        mock_connection = Mock()
        mock_connection.execute_query.return_value = []

        extractor = HANAExtractor(mock_connection)

        extractor.get_table_indexes('TESTDB', 'MY_SCHEMA', 'MY_TABLE')

        # Verificar parâmetros passados (schema_name, table_name)
        query_call = mock_connection.execute_query.call_args
        params = query_call[0][1]

        assert params == ('MY_SCHEMA', 'MY_TABLE')

    def test_get_table_indexes_performance_comparison(self):
        """Testa que nova implementação é mais eficiente que N+1."""
        mock_connection = Mock()

        # Simular 10 índices
        mock_connection.execute_query.return_value = [
            (f'IDX_{i}', 'BTREE', None, f'COL_{i}')
            for i in range(10)
        ]

        extractor = HANAExtractor(mock_connection)

        result = extractor.get_table_indexes('TESTDB', 'SCHEMA', 'BIG_TABLE')

        # Com implementação N+1 antiga: 1 query para listar + 10 queries para colunas = 11 queries
        # Com nova implementação: 1 query apenas
        assert mock_connection.execute_query.call_count == 1

        # Deve retornar todos os 10 índices
        assert len(result) == 10


class TestHANAIndexesResultFormat:
    """Testes para formato do resultado."""

    def test_index_result_structure(self):
        """Testa estrutura do resultado."""
        mock_connection = Mock()

        mock_connection.execute_query.return_value = [
            ('PK_ID', 'BTREE', 'PRIMARY KEY', 'ID')
        ]

        extractor = HANAExtractor(mock_connection)
        result = extractor.get_table_indexes('DB', 'SCHEMA', 'TABLE')

        # Verificar estrutura
        index = result[0]

        assert 'name' in index
        assert 'type' in index
        assert 'is_unique' in index
        assert 'is_primary_key' in index
        assert 'key_columns' in index
        assert 'included_columns' in index

    def test_index_types_mapping(self):
        """Testa mapeamento de tipos de índice."""
        mock_connection = Mock()

        mock_connection.execute_query.return_value = [
            ('PK', 'BTREE', 'PRIMARY KEY', 'ID'),
            ('UQ', 'BTREE', 'UNIQUE', 'EMAIL'),
            ('IDX', 'BTREE', None, 'NAME')
        ]

        extractor = HANAExtractor(mock_connection)
        result = extractor.get_table_indexes('DB', 'SCHEMA', 'TABLE')

        # Primary key
        assert result[0]['is_primary_key'] is True
        assert result[0]['is_unique'] is False

        # Unique
        assert result[1]['is_unique'] is True
        assert result[1]['is_primary_key'] is False

        # Regular index
        assert result[2]['is_unique'] is False
        assert result[2]['is_primary_key'] is False

    def test_included_columns_always_empty(self):
        """Testa que included_columns é sempre vazio (HANA não usa INCLUDE)."""
        mock_connection = Mock()

        mock_connection.execute_query.return_value = [
            ('IDX', 'BTREE', None, 'COL1, COL2')
        ]

        extractor = HANAExtractor(mock_connection)
        result = extractor.get_table_indexes('DB', 'SCHEMA', 'TABLE')

        # HANA não usa INCLUDE, sempre vazio
        assert result[0]['included_columns'] == ''


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
