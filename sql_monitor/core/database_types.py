"""
Tipos de banco de dados suportados pelo monitor.
"""
from enum import Enum


class DatabaseType(Enum):
    """Tipos de banco de dados suportados."""

    SQLSERVER = "sqlserver"
    POSTGRESQL = "postgresql"
    HANA = "hana"

    def __str__(self):
        """Retorna valor string do enum."""
        return self.value
