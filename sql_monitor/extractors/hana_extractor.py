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
        tables = []

        # Palavras reservadas e tabelas do sistema a ignorar
        ignored = {
            'SELECT', 'WHERE', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS',
            'DUMMY', 'DUAL', 'AND', 'OR', 'ON', 'AS', 'SET', 'VALUES',
            'NULL', 'NOT', 'IN', 'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
            'GROUP', 'ORDER', 'BY', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'ALL',
            'SYS', 'SYSTEM', 'PUBLIC'
        }

        # Normalizar query - remover quebras de linha extras
        normalized = ' '.join(query_text.split())

        # Padrao 1: schema.table com ou sem aspas
        # Exemplos: "SCHEMA"."TABLE", SCHEMA.TABLE, "SCHEMA".TABLE
        pattern1 = r'(?:FROM|JOIN|INTO|UPDATE)\s+"?([A-Za-z_][A-Za-z0-9_]*)"?\s*\.\s*"?([A-Za-z_][A-Za-z0-9_]*)"?'
        for match in re.finditer(pattern1, normalized, re.IGNORECASE):
            schema, table = match.groups()
            if table.upper() not in ignored and schema.upper() not in ignored:
                tables.append({'schema': schema, 'table': table})

        # Padrao 2: apenas tabela (sem schema) apos FROM/JOIN/INTO/UPDATE
        # Exemplos: FROM TABLE, JOIN "TABLE"
        pattern2 = r'(?:FROM|JOIN|INTO|UPDATE)\s+"?([A-Za-z_][A-Za-z0-9_]*)"?(?:\s|$|,|\()'
        for match in re.finditer(pattern2, normalized, re.IGNORECASE):
            table = match.group(1)
            # Verificar se nao e parte de schema.table (ja capturado acima)
            start = match.start()
            prefix = normalized[:start]
            if table.upper() not in ignored and not prefix.rstrip().endswith('.'):
                # Verificar se o proximo char nao e ponto (seria schema)
                end = match.end()
                if end < len(normalized) and normalized[end-1:end+1].strip().startswith('.'):
                    continue
                tables.append({'schema': 'PUBLIC', 'table': table})

        # Padrao 3: DELETE FROM schema.table ou DELETE FROM table
        pattern3 = r'DELETE\s+FROM\s+"?([A-Za-z_][A-Za-z0-9_]*)"?(?:\s*\.\s*"?([A-Za-z_][A-Za-z0-9_]*)"?)?'
        for match in re.finditer(pattern3, normalized, re.IGNORECASE):
            part1, part2 = match.groups()
            if part2:  # schema.table
                if part2.upper() not in ignored:
                    tables.append({'schema': part1, 'table': part2})
            else:  # apenas table
                if part1.upper() not in ignored:
                    tables.append({'schema': 'PUBLIC', 'table': part1})

        # Remove duplicatas mantendo ordem
        unique_tables = []
        seen = set()
        for table in tables:
            key = f"{table['schema'].upper()}.{table['table'].upper()}"
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

        FIX N+1: Usa query agregada com STRING_AGG em vez de 1 query por índice.

        Args:
            database_name: Nome do database (ignorado)
            schema_name: Nome do schema
            table_name: Nome da tabela

        Returns:
            Lista de dicts padronizada.
        """
        # Query otimizada: 1 consulta em vez de N+1
        query = """
        SELECT
            i.INDEX_NAME,
            i.INDEX_TYPE,
            i.CONSTRAINT,
            STRING_AGG(ic.COLUMN_NAME, ', ' ORDER BY ic.POSITION) AS key_columns
        FROM SYS.INDEXES i
        LEFT JOIN SYS.INDEX_COLUMNS ic
            ON i.SCHEMA_NAME = ic.SCHEMA_NAME
            AND i.TABLE_NAME = ic.TABLE_NAME
            AND i.INDEX_NAME = ic.INDEX_NAME
        WHERE i.SCHEMA_NAME = ?
            AND i.TABLE_NAME = ?
        GROUP BY i.INDEX_NAME, i.INDEX_TYPE, i.CONSTRAINT
        ORDER BY i.INDEX_NAME
        """

        results = self.connection.execute_query(query, (schema_name, table_name))

        if not results:
            return []

        indexes = []
        for row in results:
            index_info = {
                'name': row[0],
                'type': row[1],
                'is_unique': row[2] == 'UNIQUE' if row[2] else False,
                'is_primary_key': row[2] == 'PRIMARY KEY' if row[2] else False,
                'key_columns': row[3] or '',
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

    def get_query_plan(self, query_text: str, statement_hash: str = None) -> Optional[str]:
        """
        Obtém plano de execução REAL do cache (não estimado).

        Este método busca o plano real executado em M_SQL_PLAN_CACHE, que contém
        estatísticas reais de runtime, não apenas estimativas como EXPLAIN PLAN.

        Args:
            query_text: Texto da query
            statement_hash: Hash da query (se disponível, mais eficiente)

        Returns:
            Plano de execução real ou None
        """
        try:
            # Buscar plano por hash (mais rápido) ou por texto
            if statement_hash:
                query = """
                SELECT PLAN_ID, STATEMENT_STRING
                FROM SYS.M_SQL_PLAN_CACHE
                WHERE STATEMENT_HASH = ?
                ORDER BY LAST_EXECUTION_TIMESTAMP DESC
                LIMIT 1
                """
                result = self.connection.execute_query(query, (statement_hash,))
            else:
                query = """
                SELECT PLAN_ID, STATEMENT_STRING
                FROM SYS.M_SQL_PLAN_CACHE
                WHERE STATEMENT_STRING = ?
                ORDER BY LAST_EXECUTION_TIMESTAMP DESC
                LIMIT 1
                """
                result = self.connection.execute_query(query, (query_text,))

            if result and result[0]:
                plan_id = result[0][0]

                # Obter plano detalhado usando EXPLAIN PLAN SET STATEMENT_NAME
                # Com o PLAN_ID podemos pegar estatísticas reais dos operadores
                plan_details = self._get_plan_details(plan_id)

                # Obter plano visual usando PLAN_ID
                plan_query = """
                SELECT OPERATOR_NAME, OPERATOR_ID, TABLE_NAME, EXECUTION_COUNT,
                       INCLUSIVE_DURATION, EXCLUSIVE_DURATION
                FROM SYS.M_SQL_PLAN_CACHE_OVERVIEW
                WHERE PLAN_ID = ?
                ORDER BY OPERATOR_ID
                """
                plan_result = self.connection.execute_query(plan_query, (plan_id,))

                if plan_result:
                    plan_lines = [f"PLAN_ID: {plan_id}", "", "PLANO DE EXECUÇÃO REAL (com estatísticas de runtime):", "=" * 80]

                    for row in plan_result:
                        operator_name = row[0]
                        operator_id = row[1]
                        table_name = row[2] if row[2] else 'N/A'
                        exec_count = row[3] if row[3] else 0
                        inclusive_dur = row[4] if row[4] else 0
                        exclusive_dur = row[5] if row[5] else 0

                        plan_lines.append(
                            f"  [{operator_id}] {operator_name}: {table_name} "
                            f"(Exec: {exec_count}, Duration: {inclusive_dur}us)"
                        )

                    if plan_details:
                        plan_lines.append("")
                        plan_lines.append(plan_details)

                    return '\n'.join(plan_lines)

            # Fallback para EXPLAIN PLAN se não encontrou no cache
            print(f"   [INFO] Plano real não encontrado em cache, usando EXPLAIN PLAN (estimado)")
            return self._get_explain_plan_fallback(query_text)

        except Exception as e:
            print(f"[WARN]  Erro ao obter plano real: {e}")
            # Fallback para EXPLAIN PLAN
            return self._get_explain_plan_fallback(query_text)

    def _get_plan_details(self, plan_id: int) -> str:
        """
        Obtém detalhes dos operadores do plano.

        Args:
            plan_id: ID do plano em M_SQL_PLAN_CACHE

        Returns:
            String formatada com detalhes dos operadores
        """
        try:
            query = """
            SELECT
                OPERATOR_NAME,
                OPERATOR_ID,
                TABLE_NAME,
                EXECUTION_COUNT,
                INCLUSIVE_DURATION,
                EXCLUSIVE_DURATION,
                OUTPUT_SIZE
            FROM SYS.M_SQL_PLAN_CACHE_OVERVIEW
            WHERE PLAN_ID = ?
            ORDER BY OPERATOR_ID
            """

            results = self.connection.execute_query(query, (plan_id,))

            if not results:
                return ""

            details = ["", "OPERADORES DO PLANO (Detalhado):", "=" * 80]
            for row in results:
                operator_name = row[0]
                operator_id = row[1]
                table_name = row[2] if row[2] else 'N/A'
                exec_count = row[3] if row[3] else 0
                inclusive_dur = row[4] if row[4] else 0
                exclusive_dur = row[5] if row[5] else 0
                output_size = row[6] if row[6] else 0

                details.append(
                    f"  [{operator_id}] {operator_name}:\n"
                    f"      Table: {table_name}\n"
                    f"      Executions: {exec_count}\n"
                    f"      Inclusive Duration: {inclusive_dur}us\n"
                    f"      Exclusive Duration: {exclusive_dur}us\n"
                    f"      Output Size: {output_size} rows"
                )

            return '\n'.join(details)

        except Exception as e:
            print(f"[WARN]  Erro ao obter detalhes do plano: {e}")
            return ""

    def _get_explain_plan_fallback(self, query_text: str) -> Optional[str]:
        """
        Fallback para obter plano ESTIMADO via EXPLAIN PLAN quando plano real
        não está disponível em cache.

        Args:
            query_text: Texto da query

        Returns:
            Plano de execução estimado ou None
        """
        try:
            # HANA usa EXPLAIN PLAN para obter plano estimado
            explain_query = f"EXPLAIN PLAN FOR {query_text}"
            result = self.connection.execute_query(explain_query)

            if result:
                # Formatar resultado do EXPLAIN PLAN
                plan_lines = ["PLANO ESTIMADO (EXPLAIN PLAN - não contém estatísticas reais):", "=" * 80]
                for row in result:
                    plan_lines.append(' | '.join(str(col) for col in row))
                return '\n'.join(plan_lines)

            return None

        except Exception as e:
            print(f"[WARN]  Erro ao obter plano estimado: {e}")
            return None

