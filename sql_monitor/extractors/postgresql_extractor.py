"""
Extração de metadados de tabelas do PostgreSQL: DDL, índices, constraints, etc.
"""
import re
from typing import Dict, List, Optional
from ..core.base_extractor import BaseMetadataExtractor


class PostgreSQLExtractor(BaseMetadataExtractor):
    """Extrai metadados de tabelas e schemas do PostgreSQL."""

    def __init__(self, connection):
        """
        Inicializa extrator.

        Args:
            connection: Conexão ativa com PostgreSQL.
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
        # Regex para capturar schema.table ou table (PostgreSQL usa aspas duplas ou sem delimitadores)
        pattern = r'\b(?:FROM|JOIN|INTO|UPDATE|DELETE\s+FROM)\s+(?:"?(\w+)"?\.)?"?(\w+)"?'
        matches = re.findall(pattern, query_text, re.IGNORECASE)

        tables = []
        for schema, table in matches:
            if table.upper() not in ('SELECT', 'WHERE', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS'):
                tables.append({
                    'schema': schema if schema else 'public',  # Schema padrão PostgreSQL
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
        Gera DDL da tabela (CREATE TABLE) usando information_schema.

        Args:
            database_name: Nome do database (ignorado, PostgreSQL usa database atual).
            schema_name: Nome do schema.
            table_name: Nome da tabela.

        Returns:
            String com DDL ou None se não encontrado.
        """
        query = """
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = %s
            AND table_name = %s
        ORDER BY ordinal_position
        """

        results = self.connection.execute_query(query, (schema_name, table_name))

        if not results:
            return None

        # Constrói DDL
        ddl_lines = [f'CREATE TABLE "{schema_name}"."{table_name}" (']

        columns = []
        for row in results:
            col_name = row[0]
            data_type = row[1]
            char_max_length = row[2]
            numeric_precision = row[3]
            numeric_scale = row[4]
            is_nullable = row[5]
            column_default = row[6]

            # Formata tipo de dado
            if data_type in ('character varying', 'varchar'):
                type_str = f"varchar({char_max_length})" if char_max_length else "varchar"
            elif data_type in ('character', 'char'):
                type_str = f"char({char_max_length})" if char_max_length else "char"
            elif data_type in ('numeric', 'decimal'):
                if numeric_precision and numeric_scale:
                    type_str = f"numeric({numeric_precision},{numeric_scale})"
                else:
                    type_str = data_type
            else:
                type_str = data_type

            # Monta definição da coluna
            col_def = f'    "{col_name}" {type_str}'

            col_def += " NULL" if is_nullable == 'YES' else " NOT NULL"

            if column_default:
                col_def += f" DEFAULT {column_default}"

            columns.append(col_def)

        ddl_lines.append(",\n".join(columns))
        ddl_lines.append(");")

        return "\n".join(ddl_lines)

    def get_table_indexes(self, database_name: str, schema_name: str, table_name: str) -> List[Dict]:
        """
        Retorna índices existentes na tabela via pg_indexes.

        Args:
            database_name: Nome do database (ignorado).
            schema_name: Nome do schema.
            table_name: Nome da tabela.

        Returns:
            Lista de dicts padronizada:
            {
                'name': str,
                'type': str,
                'is_unique': bool,
                'is_primary_key': bool,
                'key_columns': str (comma-separated),
                'included_columns': str (sempre vazio, PostgreSQL não usa INCLUDE antes v11)
            }
        """
        query = """
        SELECT
            i.indexname,
            CASE
                WHEN i.indexdef LIKE '%%USING btree%%' THEN 'BTREE'
                WHEN i.indexdef LIKE '%%USING hash%%' THEN 'HASH'
                WHEN i.indexdef LIKE '%%USING gist%%' THEN 'GIST'
                WHEN i.indexdef LIKE '%%USING gin%%' THEN 'GIN'
                WHEN i.indexdef LIKE '%%USING brin%%' THEN 'BRIN'
                ELSE 'UNKNOWN'
            END as index_type,
            CASE WHEN i.indexdef LIKE '%%UNIQUE%%' THEN true ELSE false END as is_unique,
            CASE
                WHEN c.contype = 'p' THEN true
                ELSE false
            END as is_primary_key,
            i.indexdef
        FROM pg_indexes i
        LEFT JOIN pg_constraint c ON c.conname = i.indexname
            AND c.connamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)
        WHERE i.schemaname = %s
            AND i.tablename = %s
        ORDER BY is_primary_key DESC, i.indexname
        """

        results = self.connection.execute_query(query, (schema_name, schema_name, table_name))

        if not results:
            return []

        indexes = []
        for row in results:
            index_name = row[0]
            index_type = row[1]
            is_unique = row[2]
            is_primary_key = row[3]
            indexdef = row[4]

            # Extrai colunas do indexdef
            # Ex: CREATE INDEX idx_name ON schema.table USING btree (col1, col2)
            columns_match = re.search(r'\(([^)]+)\)', indexdef)
            key_columns = columns_match.group(1).strip() if columns_match else ''

            index_info = {
                'name': index_name,
                'type': index_type,
                'is_unique': is_unique,
                'is_primary_key': is_primary_key,
                'key_columns': key_columns,
                'included_columns': ''  # PostgreSQL não usa INCLUDE antes v11, manter vazio
            }
            indexes.append(index_info)

        return indexes

    def get_missing_indexes(self, database_name: str) -> List[Dict]:
        """
        Retorna sugestões de índices baseadas em table scans.

        PostgreSQL não tem DMVs de missing indexes como SQL Server,
        então usamos pg_stat_user_tables para identificar tabelas
        com alto ratio de sequential scans vs index scans.

        Args:
            database_name: Nome do database (ignorado, usa database atual).

        Returns:
            Lista de dicts padronizada (simplificada):
            {
                'schema_name': str,
                'table_name': str,
                'equality_columns': str (vazio),
                'inequality_columns': str (vazio),
                'included_columns': str (vazio),
                'avg_user_impact': float (estimado baseado em seq_scan ratio),
                'total_seeks_scans': int (seq_scan count)
            }
        """
        query = """
        SELECT
            schemaname,
            relname,
            seq_scan,
            idx_scan,
            CASE
                WHEN seq_scan + idx_scan > 0
                THEN (seq_scan::float / (seq_scan + idx_scan)) * 100
                ELSE 0
            END as seq_scan_ratio,
            seq_tup_read
        FROM pg_stat_user_tables
        WHERE seq_scan > 1000
            AND schemaname NOT IN ('pg_catalog', 'information_schema')
            AND (seq_scan::float / GREATEST(seq_scan + idx_scan, 1)) > 0.5
        ORDER BY seq_tup_read DESC
        LIMIT 10
        """

        results = self.connection.execute_query(query)

        if not results:
            return []

        missing_indexes = []
        for row in results:
            index_info = {
                'schema_name': row[0],
                'table_name': row[1],
                'equality_columns': '',  # PostgreSQL não fornece colunas específicas
                'inequality_columns': '',
                'included_columns': '',
                'avg_user_impact': float(row[4]) if row[4] else 0.0,  # Usando seq_scan_ratio como impacto
                'total_seeks_scans': int(row[2]) if row[2] else 0  # seq_scan count
            }
            missing_indexes.append(index_info)

        return missing_indexes
