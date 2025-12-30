"""
Classe base abstrata para extratores de metadados.
Define interface comum para DDL, índices e sugestões.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Optional


class BaseMetadataExtractor(ABC):
    """
    Classe base abstrata para extratores de metadados.
    Define interface comum para DDL, índices e sugestões.
    """

    def __init__(self, connection):
        """
        Inicializa extrator.

        Args:
            connection: Instância de BaseDatabaseConnection.
        """
        self.connection = connection

    @abstractmethod
    def extract_table_info_from_query(self, query_text: str) -> List[Dict[str, str]]:
        """
        Extrai nomes de tabelas da query.

        Args:
            query_text: Texto da query.

        Returns:
            Lista de dicts: [{'schema': str, 'table': str}]
        """
        pass

    @abstractmethod
    def get_table_ddl(self, database_name: str, schema_name: str, table_name: str) -> Optional[str]:
        """
        Gera DDL da tabela.

        Args:
            database_name: Nome do database.
            schema_name: Nome do schema.
            table_name: Nome da tabela.

        Returns:
            String com DDL (CREATE TABLE) ou None.
        """
        pass

    @abstractmethod
    def get_table_indexes(self, database_name: str, schema_name: str, table_name: str) -> List[Dict]:
        """
        Retorna índices existentes.

        Args:
            database_name: Nome do database.
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
                'included_columns': str (comma-separated, opcional)
            }
        """
        pass

    @abstractmethod
    def get_missing_indexes(self, database_name: str) -> List[Dict]:
        """
        Retorna sugestões de índices faltantes.

        Args:
            database_name: Nome do database.

        Returns:
            Lista de dicts padronizada:
            {
                'schema_name': str,
                'table_name': str,
                'equality_columns': str,
                'inequality_columns': str,
                'included_columns': str,
                'avg_user_impact': float,
                'total_seeks_scans': int
            }
        """
        pass

    def format_indexes_for_display(self, indexes: List[Dict]) -> str:
        """
        Formata índices para exibição (implementação genérica).

        Args:
            indexes: Lista de índices.

        Returns:
            String formatada.
        """
        if not indexes:
            return "Nenhum índice encontrado."

        lines = ["Índices Existentes:"]
        for idx in indexes:
            pk = " [PRIMARY KEY]" if idx.get('is_primary_key') else ""
            unique = " [UNIQUE]" if idx.get('is_unique') else ""

            lines.append(f"\n  - {idx['name']}{pk}{unique}")
            lines.append(f"    Tipo: {idx['type']}")
            lines.append(f"    Colunas chave: {idx['key_columns']}")

            if idx.get('included_columns'):
                lines.append(f"    Colunas incluídas: {idx['included_columns']}")

        return "\n".join(lines)
