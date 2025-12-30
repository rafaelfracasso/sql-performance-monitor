"""
Factory para criar componentes de banco de dados (Connection, Collector, Extractor).
"""
from typing import Tuple, Dict, Any
from ..core.database_types import DatabaseType
from ..core.base_connection import BaseDatabaseConnection
from ..core.base_collector import BaseQueryCollector
from ..core.base_extractor import BaseMetadataExtractor

# SQL Server
from ..connections.sqlserver_connection import SQLServerConnection
from ..collectors.sqlserver_collector import SQLServerCollector
from ..extractors.sqlserver_extractor import SQLServerExtractor

# PostgreSQL
from ..connections.postgresql_connection import PostgreSQLConnection
from ..collectors.postgresql_collector import PostgreSQLCollector
from ..extractors.postgresql_extractor import PostgreSQLExtractor

# SAP HANA
from ..connections.hana_connection import HANAConnection
from ..collectors.hana_collector import HANACollector
from ..extractors.hana_extractor import HANAExtractor


class DatabaseFactory:
    """
    Factory para criação de componentes de banco de dados.

    Cria Connection, Collector e Extractor apropriados baseado no tipo de banco.
    """

    @staticmethod
    def create_connection(db_type: DatabaseType, credentials: Dict[str, Any], timeout: int = 10) -> BaseDatabaseConnection:
        """
        Cria conexão apropriada para o tipo de banco.

        Args:
            db_type: Tipo do banco (DatabaseType enum)
            credentials: Dicionário com credenciais
                - SQL Server: server, port, database, username, password, driver (opcional)
                - PostgreSQL: server, port, database, username, password
                - SAP HANA: server, port, database, username, password
            timeout: Timeout de conexão em segundos (padrão: 10)

        Returns:
            Instância de conexão apropriada.

        Raises:
            ValueError: Se tipo de banco não for suportado.
        """
        if db_type == DatabaseType.SQLSERVER:
            return SQLServerConnection(
                server=credentials.get('server'),
                port=credentials.get('port', '1433'),
                database=credentials.get('database'),
                username=credentials.get('username'),
                password=credentials.get('password'),
                driver=credentials.get('driver', 'ODBC Driver 18 for SQL Server'),
                timeout=timeout
            )

        elif db_type == DatabaseType.POSTGRESQL:
            return PostgreSQLConnection(
                server=credentials.get('server'),
                port=credentials.get('port', '5432'),
                database=credentials.get('database'),
                username=credentials.get('username'),
                password=credentials.get('password'),
                timeout=timeout
            )

        elif db_type == DatabaseType.HANA:
            return HANAConnection(
                server=credentials.get('server'),
                port=credentials.get('port', '30015'),
                database=credentials.get('database'),
                username=credentials.get('username'),
                password=credentials.get('password'),
                timeout=timeout
            )

        else:
            raise ValueError(f"Tipo de banco não suportado: {db_type}")

    @staticmethod
    def create_collector(db_type: DatabaseType, connection: BaseDatabaseConnection) -> BaseQueryCollector:
        """
        Cria collector apropriado para o tipo de banco.

        Args:
            db_type: Tipo do banco (DatabaseType enum)
            connection: Conexão ativa com o banco

        Returns:
            Instância de collector apropriada.

        Raises:
            ValueError: Se tipo de banco não for suportado.
        """
        if db_type == DatabaseType.SQLSERVER:
            return SQLServerCollector(connection)

        elif db_type == DatabaseType.POSTGRESQL:
            return PostgreSQLCollector(connection)

        elif db_type == DatabaseType.HANA:
            return HANACollector(connection)

        else:
            raise ValueError(f"Tipo de banco não suportado: {db_type}")

    @staticmethod
    def create_extractor(db_type: DatabaseType, connection: BaseDatabaseConnection) -> BaseMetadataExtractor:
        """
        Cria extractor apropriado para o tipo de banco.

        Args:
            db_type: Tipo do banco (DatabaseType enum)
            connection: Conexão ativa com o banco

        Returns:
            Instância de extractor apropriada.

        Raises:
            ValueError: Se tipo de banco não for suportado.
        """
        if db_type == DatabaseType.SQLSERVER:
            return SQLServerExtractor(connection)

        elif db_type == DatabaseType.POSTGRESQL:
            return PostgreSQLExtractor(connection)

        elif db_type == DatabaseType.HANA:
            return HANAExtractor(connection)

        else:
            raise ValueError(f"Tipo de banco não suportado: {db_type}")

    @staticmethod
    def create_components(
        db_type: DatabaseType,
        credentials: Dict[str, Any],
        timeout: int = 10
    ) -> Tuple[BaseDatabaseConnection, BaseQueryCollector, BaseMetadataExtractor]:
        """
        Cria todos os componentes (connection, collector, extractor) de uma vez.

        Args:
            db_type: Tipo do banco (DatabaseType enum)
            credentials: Dicionário com credenciais
            timeout: Timeout de conexão em segundos (padrão: 10)

        Returns:
            Tupla com (connection, collector, extractor)

        Raises:
            ValueError: Se tipo de banco não for suportado.

        Example:
            >>> from sql_monitor.core.database_types import DatabaseType
            >>> credentials = {
            ...     'server': 'localhost',
            ...     'port': '5432',
            ...     'database': 'mydb',
            ...     'username': 'user',
            ...     'password': 'pass'
            ... }
            >>> conn, collector, extractor = DatabaseFactory.create_components(
            ...     DatabaseType.POSTGRESQL,
            ...     credentials,
            ...     timeout=10
            ... )
        """
        # Criar conexão
        connection = DatabaseFactory.create_connection(db_type, credentials, timeout)

        # Criar collector e extractor
        collector = DatabaseFactory.create_collector(db_type, connection)
        extractor = DatabaseFactory.create_extractor(db_type, connection)

        return connection, collector, extractor

    @staticmethod
    def get_supported_databases() -> list[DatabaseType]:
        """
        Retorna lista de tipos de banco suportados.

        Returns:
            Lista de DatabaseType suportados.
        """
        return [
            DatabaseType.SQLSERVER,
            DatabaseType.POSTGRESQL,
            DatabaseType.HANA
        ]

    @staticmethod
    def is_supported(db_type: DatabaseType) -> bool:
        """
        Verifica se tipo de banco é suportado.

        Args:
            db_type: Tipo do banco a verificar

        Returns:
            True se suportado, False caso contrário.
        """
        return db_type in DatabaseFactory.get_supported_databases()
