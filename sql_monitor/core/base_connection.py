"""
Classe base abstrata para conexões de banco de dados.
Define interface comum para SQL Server, PostgreSQL e SAP HANA.
"""
from abc import ABC, abstractmethod
from typing import Optional, List, Tuple, Any
import time


class BaseDatabaseConnection(ABC):
    """
    Classe base abstrata para conexões de banco de dados.
    Define interface comum para SQL Server, PostgreSQL e SAP HANA.
    """

    def __init__(self):
        """Inicializa conexão."""
        self.server: str = ""
        self.port: str = ""
        self.database: str = ""
        self.username: str = ""
        self.password: str = ""
        self.connection: Optional[Any] = None
        self.cursor: Optional[Any] = None

    @abstractmethod
    def connect(self) -> bool:
        """
        Estabelece conexão com banco.

        Returns:
            bool: True se conectado com sucesso.
        """
        pass

    @abstractmethod
    def disconnect(self):
        """Fecha conexão."""
        pass

    @abstractmethod
    def execute_query(self, query: str, params: tuple = None) -> Optional[List[Tuple]]:
        """
        Executa query e retorna resultados.

        Args:
            query: Query SQL.
            params: Parâmetros opcionais.

        Returns:
            Lista de tuplas com resultados ou None em caso de erro.
        """
        pass

    @abstractmethod
    def execute_scalar(self, query: str, params: tuple = None) -> Optional[Any]:
        """
        Executa query e retorna valor único.

        Args:
            query: Query SQL.
            params: Parâmetros opcionais.

        Returns:
            Valor escalar ou None.
        """
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Verifica se conexão está ativa.

        Returns:
            bool: True se conectado.
        """
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """
        Testa conexão com query simples.

        Returns:
            bool: True se conexão funciona.
        """
        pass

    @abstractmethod
    def get_version(self) -> Optional[str]:
        """
        Retorna versão do banco.

        Returns:
            String com versão ou None.
        """
        pass

    def ensure_connection(self, max_retries: int = 3) -> bool:
        """
        Garante conexão ativa (implementação genérica).

        Args:
            max_retries: Número máximo de tentativas.

        Returns:
            bool: True se conectado.
        """
        if self.is_connected():
            return True

        print(f"⚠️  Conexão perdida com {self.server}, tentando reconectar...")

        for attempt in range(1, max_retries + 1):
            print(f"   Tentativa {attempt}/{max_retries}...")

            try:
                self.disconnect()
                if self.connect():
                    print("✓ Reconexão bem-sucedida!")
                    return True
            except Exception as e:
                print(f"   ✗ Falha: {e}")

            if attempt < max_retries:
                time.sleep(2 ** attempt)  # Backoff exponencial

        print("✗ Não foi possível reconectar")
        return False

    def __enter__(self):
        """Context manager enter."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
