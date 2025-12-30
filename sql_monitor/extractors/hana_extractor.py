"""
Extração de metadados de tabelas do SAP HANA: DDL, índices, constraints, etc.
"""
import re
from typing import Dict, List, Optional
from ..core.base_extractor import BaseMetadataExtractor


class HANAExtractor(BaseMetadataExtractor):
    """Extrai metadados de tabelas e schemas do SAP HANA."""

    def __init__(self, connection):
        """
        Inicializa extrator.

        Args:
            connection: Conexão ativa com SAP HANA.
        """
        super().__init__(connection)

    def extract_table_info_from_query(self, query_text: str) -> List[Dict[str, str]]:
        """
        Extrai nomes de tabelas mencionadas na query.

        Args:
            query_text: Texto da query SQL.

        Returns:
            Lista de dicts com schema e nome da tabela.
        """
        # Regex para capturar schema.table ou table (HANA usa aspas duplas ou sem delimitadores)
        pattern = r'\b(?:FROM|JOIN|INTO|UPDATE|DELETE\s+FROM)\s+(?:"?(\w+)"?\.)?"?(\w+)"?'
        matches = re.findall(pattern, query_text, re.IGNORECASE)

        tables = []
        for schema, table in matches:
            if table.upper() not in ('SELECT', 'WHERE', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS', 'DUMMY'):
                tables.append({
                    'schema': schema if schema else 'PUBLIC',  # Schema padrão pode variar
                    'table': table
                })

        # Remove duplicatas
        unique_tables = []
        seen = set()
        for table in tables:
            key = f"{table['schema']}.{table['table']}"
            if key not in seen:
                seen.add(key)
                unique_tables.append(table)

        return unique_tables

    def get_table_ddl(self, database_name: str, schema_name: str, table_name: str) -> Optional[str]:
        """
        Gera DDL da tabela usando SYS.TABLE_COLUMNS.

        Args:
            database_name: Nome do database (ignorado, HANA usa database atual)
            schema_name: Nome do schema
            table_name: Nome da tabela

        Returns:
            String com CREATE TABLE ou None.
        """
        query = """
        SELECT
            COLUMN_NAME,
            DATA_TYPE_NAME,
            LENGTH,
            SCALE,
            IS_NULLABLE,
            DEFAULT_VALUE,
            POSITION
        FROM SYS.TABLE_COLUMNS
        WHERE SCHEMA_NAME = ?
            AND TABLE_NAME = ?
        ORDER BY POSITION
        """

        results = self.connection.execute_query(query, (schema_name, table_name))

        if not results:
            return None

        # Construir DDL
        ddl_lines = [f'CREATE TABLE "{schema_name}"."{table_name}" (']

        for row in results:
            column_name = row[0]
            data_type = row[1]
            length = row[2]
            scale = row[3]
            is_nullable = row[4]
            default_value = row[5]

            # Construir definição de coluna
            col_def = f'    "{column_name}" {data_type}'

            # Adicionar length/scale se aplicável
            if data_type in ('VARCHAR', 'NVARCHAR', 'VARBINARY'):
                col_def += f'({length})'
            elif data_type in ('DECIMAL', 'DEC'):
                if scale:
                    col_def += f'({length},{scale})'
                else:
                    col_def += f'({length})'

            # Nullable
            if is_nullable == 'FALSE':
                col_def += ' NOT NULL'

            # Default
            if default_value:
                col_def += f' DEFAULT {default_value}'

            ddl_lines.append(col_def + ',')

        # Remover última vírgula
        if ddl_lines[-1].endswith(','):
            ddl_lines[-1] = ddl_lines[-1][:-1]

        ddl_lines.append(');')

        return '\n'.join(ddl_lines)

    def get_table_indexes(self, database_name: str, schema_name: str, table_name: str) -> List[Dict]:
        """
        Retorna índices existentes na tabela via SYS.INDEXES.

        Args:
            database_name: Nome do database (ignorado)
            schema_name: Nome do schema
            table_name: Nome da tabela

        Returns:
            Lista de dicts padronizada.
        """
        # Primeiro, buscar índices
        query_indexes = """
        SELECT
            INDEX_NAME,
            INDEX_TYPE,
            CONSTRAINT
        FROM SYS.INDEXES
        WHERE SCHEMA_NAME = ?
            AND TABLE_NAME = ?
        ORDER BY INDEX_NAME
        """

        index_results = self.connection.execute_query(query_indexes, (schema_name, table_name))

        if not index_results:
            return []

        indexes = []

        for idx_row in index_results:
            index_name = idx_row[0]
            index_type = idx_row[1]
            constraint_type = idx_row[2]

            # Buscar colunas do índice
            query_columns = """
            SELECT COLUMN_NAME, POSITION
            FROM SYS.INDEX_COLUMNS
            WHERE SCHEMA_NAME = ?
                AND TABLE_NAME = ?
                AND INDEX_NAME = ?
            ORDER BY POSITION
            """

            col_results = self.connection.execute_query(query_columns, (schema_name, table_name, index_name))

            if col_results:
                key_columns = ', '.join([col[0] for col in col_results])
            else:
                key_columns = ''

            index_info = {
                'name': index_name,
                'type': index_type,
                'is_unique': constraint_type == 'UNIQUE' if constraint_type else False,
                'is_primary_key': constraint_type == 'PRIMARY KEY' if constraint_type else False,
                'key_columns': key_columns,
                'included_columns': ''  # HANA não usa INCLUDE
            }

            indexes.append(index_info)

        return indexes

    def get_missing_indexes(self, database_name: str) -> List[Dict]:
        """
        Retorna sugestões de índices baseadas em column store statistics.

        Args:
            database_name: Nome do database (ignorado)

        Returns:
            Lista de sugestões de índices.
        """
        # Para column store, tabelas sem índices podem se beneficiar
        query = """
        SELECT
            t.SCHEMA_NAME,
            t.TABLE_NAME,
            t.RECORD_COUNT,
            'BTREE' as SUGGESTED_INDEX_TYPE,
            'Consider adding index based on query patterns' as REASON
        FROM SYS.M_CS_TABLES t
        LEFT JOIN SYS.INDEXES i ON t.SCHEMA_NAME = i.SCHEMA_NAME
            AND t.TABLE_NAME = i.TABLE_NAME
        WHERE t.RECORD_COUNT > 10000
            AND i.INDEX_NAME IS NULL
            AND t.SCHEMA_NAME NOT LIKE 'SYS%'
            AND t.SCHEMA_NAME NOT LIKE '_SYS_%'
        ORDER BY t.RECORD_COUNT DESC
        LIMIT 10
        """

        results = self.connection.execute_query(query)

        if not results:
            return []

        suggestions = []
        for row in results:
            suggestion = {
                'schema': row[0],
                'table': row[1],
                'record_count': row[2],
                'impact': 'HIGH' if row[2] > 1000000 else 'MEDIUM',
                'reason': row[4],
                'suggestion': f"-- Consider creating index on {row[0]}.{row[1]} (RECORD_COUNT: {row[2]:,})\n"
                             f"-- Analyze query patterns and add appropriate column index"
            }
            suggestions.append(suggestion)

        return suggestions

    def get_query_plan(self, query_text: str) -> Optional[str]:
        """
        Obtém plano de execução via EXPLAIN PLAN.

        Args:
            query_text: Texto da query

        Returns:
            Plano de execução ou None.
        """
        try:
            # HANA usa EXPLAIN PLAN para obter plano
            explain_query = f"EXPLAIN PLAN FOR {query_text}"
            result = self.connection.execute_query(explain_query)

            if result:
                # Formatar resultado do EXPLAIN PLAN
                plan_lines = []
                for row in result:
                    plan_lines.append(' | '.join(str(col) for col in row))
                return '\n'.join(plan_lines)

            return None

        except Exception as e:
            print(f"⚠️  Erro ao obter plano de execução: {e}")
            return None
