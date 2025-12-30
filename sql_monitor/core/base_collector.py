"""
Classe base abstrata para coletores de queries.
Define interface comum para coleta de queries ativas, expensive e table scans.
"""
from abc import ABC, abstractmethod
from typing import List, Dict


class BaseQueryCollector(ABC):
    """
    Classe base abstrata para coletores de queries.
    Define interface comum para coleta de queries ativas, expensive e table scans.
    """

    def __init__(self, connection):
        """
        Inicializa coletor.

        Args:
            connection: Instância de BaseDatabaseConnection.
        """
        self.connection = connection

    @abstractmethod
    def collect_active_queries(self, min_duration_seconds: int = 5) -> List[Dict]:
        """
        Coleta queries atualmente em execução.

        Args:
            min_duration_seconds: Duração mínima em segundos.

        Returns:
            Lista de dicts padronizada:
            {
                'session_id': int,
                'request_id': int,
                'start_time': datetime,
                'status': str,
                'command': str,
                'duration_seconds': int,
                'cpu_time_ms': int,
                'logical_reads': int,
                'physical_reads': int,
                'writes': int,
                'elapsed_time_ms': int,
                'database_name': str,
                'query_text': str,
                'full_query_text': str,
                'query_plan': str (XML ou JSON) | None,
                'host_name': str,
                'program_name': str,
                'login_name': str,
                'client_interface_name': str
            }
        """
        pass

    @abstractmethod
    def collect_recent_expensive_queries(self, top_n: int = 10) -> List[Dict]:
        """
        Coleta queries recentes mais caras.

        Args:
            top_n: Número de queries a retornar.

        Returns:
            Lista de dicts padronizada:
            {
                'execution_count': int,
                'total_cpu_time_ms': int,
                'avg_cpu_time_ms': int,
                'total_logical_reads': int,
                'avg_logical_reads': int,
                'total_physical_reads': int,
                'total_elapsed_time_ms': int,
                'avg_elapsed_time_ms': int,
                'last_execution_time': datetime,
                'database_name': str,
                'object_name': str | None,
                'query_text': str,
                'query_plan': str | None
            }
        """
        pass

    @abstractmethod
    def get_table_scan_queries(self) -> List[Dict]:
        """
        Identifica queries fazendo table scans.

        Returns:
            Lista de dicts padronizada:
            {
                'database_name': str,
                'object_name': str | None,
                'execution_count': int,
                'total_logical_reads': int,
                'avg_logical_reads': int,
                'total_cpu_time_ms': int,
                'query_text': str,
                'query_plan': str | None,
                'has_table_scan': True
            }
        """
        pass

    @abstractmethod
    def get_database_list(self) -> List[str]:
        """
        Retorna lista de databases disponíveis.

        Returns:
            Lista de nomes de databases.
        """
        pass
