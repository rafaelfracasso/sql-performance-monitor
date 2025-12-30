"""
Extração de metadados de tabelas: DDL, índices, constraints, etc.
"""
import re
from typing import Dict, List, Optional
from .connection import SQLServerConnection


class MetadataExtractor:
    """Extrai metadados de tabelas e schemas do SQL Server."""

    def __init__(self, connection: SQLServerConnection):
        """
        Inicializa extrator.

        Args:
            connection: Conexão ativa com SQL Server.
        """
        self.connection = connection

    def extract_table_info_from_query(self, query_text: str) -> List[Dict[str, str]]:
        """
        Extrai nomes de tabelas mencionadas na query.

        Args:
            query_text: Texto da query SQL.

        Returns:
            Lista de dicts com schema e nome da tabela.
        """
        # Regex para capturar [schema].[table] ou table
        pattern = r'\b(?:FROM|JOIN|INTO|UPDATE|DELETE\s+FROM)\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?'
        matches = re.findall(pattern, query_text, re.IGNORECASE)

        tables = []
        for schema, table in matches:
            if table.upper() not in ('SELECT', 'WHERE', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS'):
                tables.append({
                    'schema': schema if schema else 'dbo',
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
        Gera DDL da tabela (CREATE TABLE).

        Args:
            database_name: Nome do database.
            schema_name: Nome do schema.
            table_name: Nome da tabela.

        Returns:
            String com DDL ou None se não encontrado.
        """
        query = f"""
        SELECT
            c.name AS column_name,
            t.name AS data_type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity,
            ISNULL(dc.definition, '') AS default_value
        FROM [{database_name}].sys.columns c
        INNER JOIN [{database_name}].sys.types t ON c.user_type_id = t.user_type_id
        LEFT JOIN [{database_name}].sys.default_constraints dc ON c.default_object_id = dc.object_id
        WHERE c.object_id = OBJECT_ID('[{database_name}].[{schema_name}].[{table_name}]')
        ORDER BY c.column_id
        """

        results = self.connection.execute_query(query)

        if not results:
            return None

        # Constrói DDL
        ddl_lines = [f"CREATE TABLE [{schema_name}].[{table_name}] ("]

        columns = []
        for row in results:
            col_name = row[0]
            data_type = row[1]
            max_length = row[2]
            precision = row[3]
            scale = row[4]
            is_nullable = row[5]
            is_identity = row[6]
            default_value = row[7]

            # Formata tipo de dado
            if data_type in ('varchar', 'char', 'nvarchar', 'nchar'):
                length = 'MAX' if max_length == -1 else str(max_length if data_type.startswith('n') else max_length)
                type_str = f"{data_type}({length})"
            elif data_type in ('decimal', 'numeric'):
                type_str = f"{data_type}({precision},{scale})"
            else:
                type_str = data_type

            # Monta definição da coluna
            col_def = f"    [{col_name}] {type_str}"

            if is_identity:
                col_def += " IDENTITY(1,1)"

            col_def += " NOT NULL" if not is_nullable else " NULL"

            if default_value:
                col_def += f" DEFAULT {default_value}"

            columns.append(col_def)

        ddl_lines.append(",\n".join(columns))
        ddl_lines.append(");")

        return "\n".join(ddl_lines)

    def get_table_indexes(self, database_name: str, schema_name: str, table_name: str) -> List[Dict]:
        """
        Retorna índices existentes na tabela.

        Args:
            database_name: Nome do database.
            schema_name: Nome do schema.
            table_name: Nome da tabela.

        Returns:
            Lista de dicts com informações dos índices.
        """
        query = f"""
        SELECT
            i.name AS index_name,
            i.type_desc AS index_type,
            i.is_unique,
            i.is_primary_key,
            STUFF((
                SELECT ', ' + c2.name
                FROM [{database_name}].sys.index_columns ic2
                INNER JOIN [{database_name}].sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
                WHERE ic2.object_id = i.object_id
                    AND ic2.index_id = i.index_id
                    AND ic2.is_included_column = 0
                ORDER BY ic2.key_ordinal
                FOR XML PATH(''), TYPE
            ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS key_columns,
            STUFF((
                SELECT ', ' + c2.name
                FROM [{database_name}].sys.index_columns ic2
                INNER JOIN [{database_name}].sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
                WHERE ic2.object_id = i.object_id
                    AND ic2.index_id = i.index_id
                    AND ic2.is_included_column = 1
                ORDER BY ic2.key_ordinal
                FOR XML PATH(''), TYPE
            ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS included_columns
        FROM [{database_name}].sys.indexes i
        WHERE i.object_id = OBJECT_ID('[{database_name}].[{schema_name}].[{table_name}]')
            AND i.type > 0  -- Exclui heaps
        ORDER BY i.is_primary_key DESC, i.name
        """

        results = self.connection.execute_query(query)

        if not results:
            return []

        indexes = []
        for row in results:
            index_info = {
                'name': row[0],
                'type': row[1],
                'is_unique': bool(row[2]),
                'is_primary_key': bool(row[3]),
                'key_columns': row[4] if row[4] else '',
                'included_columns': row[5] if row[5] else ''
            }
            indexes.append(index_info)

        return indexes

    def get_missing_indexes(self, database_name: str) -> List[Dict]:
        """
        Retorna sugestões de índices faltantes do SQL Server.

        Args:
            database_name: Nome do database.

        Returns:
            Lista de sugestões de índices.
        """
        query = f"""
        SELECT TOP 10
            OBJECT_SCHEMA_NAME(mid.object_id, DB_ID('{database_name}')) AS schema_name,
            OBJECT_NAME(mid.object_id, DB_ID('{database_name}')) AS table_name,
            mid.equality_columns,
            mid.inequality_columns,
            mid.included_columns,
            migs.avg_user_impact,
            migs.user_seeks + migs.user_scans AS total_seeks_scans
        FROM sys.dm_db_missing_index_details mid
        INNER JOIN sys.dm_db_missing_index_groups mig ON mid.index_handle = mig.index_handle
        INNER JOIN sys.dm_db_missing_index_group_stats migs ON mig.index_group_handle = migs.group_handle
        WHERE mid.database_id = DB_ID('{database_name}')
        ORDER BY migs.avg_user_impact * (migs.user_seeks + migs.user_scans) DESC
        """

        results = self.connection.execute_query(query)

        if not results:
            return []

        missing_indexes = []
        for row in results:
            index_info = {
                'schema_name': row[0],
                'table_name': row[1],
                'equality_columns': row[2] if row[2] else '',
                'inequality_columns': row[3] if row[3] else '',
                'included_columns': row[4] if row[4] else '',
                'avg_user_impact': float(row[5]) if row[5] else 0.0,
                'total_seeks_scans': int(row[6]) if row[6] else 0
            }
            missing_indexes.append(index_info)

        return missing_indexes

    def format_indexes_for_display(self, indexes: List[Dict]) -> str:
        """
        Formata lista de índices para exibição.

        Args:
            indexes: Lista de índices.

        Returns:
            String formatada.
        """
        if not indexes:
            return "Nenhum índice encontrado."

        lines = ["Índices Existentes:"]
        for idx in indexes:
            pk_marker = " [PRIMARY KEY]" if idx['is_primary_key'] else ""
            unique_marker = " [UNIQUE]" if idx['is_unique'] else ""

            lines.append(f"\n  - {idx['name']}{pk_marker}{unique_marker}")
            lines.append(f"    Tipo: {idx['type']}")
            lines.append(f"    Colunas chave: {idx['key_columns']}")

            if idx['included_columns']:
                lines.append(f"    Colunas incluídas: {idx['included_columns']}")

        return "\n".join(lines)
