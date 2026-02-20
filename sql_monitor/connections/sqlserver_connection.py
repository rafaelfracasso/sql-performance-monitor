"""
Gerenciamento de conexão com SQL Server usando pyodbc.
"""
import pyodbc
from typing import Optional, List, Tuple, Any
from ..core.base_connection import BaseDatabaseConnection


class SQLServerConnection(BaseDatabaseConnection):
    """Gerencia conexão com SQL Server."""

    def __init__(self, server: str, port: str, database: str, username: str, password: str, driver: str = None, timeout: int = 10, **kwargs):
        """
        Inicializa conexão com SQL Server.

        Args:
            server: Endereço do servidor
            port: Porta do servidor
            database: Nome do database
            username: Usuário
            password: Senha
            driver: Driver ODBC (opcional, padrão: ODBC Driver 18 for SQL Server)
            timeout: Timeout de conexão em segundos (padrão: 10)
            **kwargs: Parâmetros adicionais ignorados
        """
        super().__init__()
        self.server = server
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver or 'ODBC Driver 18 for SQL Server'
        self.timeout = timeout

        self.connection: Optional[pyodbc.Connection] = None
        self.cursor: Optional[pyodbc.Cursor] = None

    def connect(self) -> bool:
        """
        Estabelece conexão com SQL Server.

        Returns:
            bool: True se conectado com sucesso, False caso contrário.
        """
        try:
            connection_string = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server},{self.port};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"TrustServerCertificate=yes;"
            )

            self.connection = pyodbc.connect(connection_string, timeout=self.timeout)
            self.cursor = self.connection.cursor()

            print(f"✓ Conectado ao SQL Server: {self.server} - Database: {self.database}")
            return True

        except pyodbc.Error as e:
            print(f"✗ Erro ao conectar ao SQL Server: {e}")
            return False

    def disconnect(self):
        """Fecha conexão com SQL Server."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("✓ Conexão SQL Server fechada com sucesso")

    def execute_query(self, query: str, params: tuple = None, auto_reconnect: bool = True) -> Optional[List[Tuple]]:
        """
        Executa query e retorna resultados.

        Args:
            query: Query SQL a ser executada.
            params: Parâmetros opcionais para query parametrizada.
            auto_reconnect: Se True, tenta reconectar automaticamente em caso de falha de conexão.

        Returns:
            Lista de resultados ou None em caso de erro.
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            return self.cursor.fetchall()

        except pyodbc.Error as e:
            error_code = e.args[0] if e.args else None

            # Erros de conexão que requerem reconexão
            connection_errors = ['08S01', '08001', '08003', '08007', 'HYT00', 'HYT01']

            if auto_reconnect and error_code in connection_errors:
                print(f"✗ Erro ao executar query: {e}")

                # Tenta reconectar
                if self.ensure_connection():
                    print("   🔄 Tentando executar query novamente...")
                    try:
                        if params:
                            self.cursor.execute(query, params)
                        else:
                            self.cursor.execute(query)
                        return self.cursor.fetchall()
                    except pyodbc.Error as retry_error:
                        print(f"✗ Erro ao re-executar query: {retry_error}")
                        return None
            else:
                print(f"✗ Erro ao executar query: {e}")

            return None

    def execute_scalar(self, query: str, params: tuple = None) -> Optional[Any]:
        """
        Executa query e retorna um único valor.

        Args:
            query: Query SQL a ser executada.
            params: Parâmetros opcionais.

        Returns:
            Valor escalar ou None.
        """
        result = self.execute_query(query, params)
        if result and len(result) > 0:
            return result[0][0]
        return None

    def is_connected(self) -> bool:
        """
        Verifica se a conexão está ativa.

        Returns:
            bool: True se conexão está ativa, False caso contrário.
        """
        if not self.connection:
            return False

        try:
            self.cursor.execute("SELECT 1")
            self.cursor.fetchone()
            return True
        except (pyodbc.Error, AttributeError):
            return False

    def test_connection(self) -> bool:
        """
        Testa conexão executando query simples.

        Returns:
            bool: True se conexão está funcionando.
        """
        try:
            result = self.execute_scalar("SELECT @@VERSION")
            if result:
                print(f"✓ SQL Server Version: {result[:50]}...")
                return True
            return False
        except Exception as e:
            print(f"✗ Teste de conexão SQL Server falhou: {e}")
            return False

    def get_version(self) -> Optional[str]:
        """
        Retorna versão do SQL Server.

        Returns:
            String com versão ou None.
        """
        return self.execute_scalar("SELECT @@VERSION")
