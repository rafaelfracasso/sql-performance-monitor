"""
Gerenciamento de conexão com SQL Server usando pyodbc.
"""
import os
import pyodbc
from typing import Optional
from dotenv import load_dotenv


class SQLServerConnection:
    """Gerencia conexão com SQL Server."""

    def __init__(self):
        """Inicializa conexão carregando variáveis de ambiente."""
        load_dotenv()

        self.server = os.getenv('SQL_SERVER', 'localhost')
        self.port = os.getenv('SQL_PORT', '1433')
        self.database = os.getenv('SQL_DATABASE', 'master')
        self.username = os.getenv('SQL_USERNAME')
        self.password = os.getenv('SQL_PASSWORD')
        self.driver = os.getenv('SQL_DRIVER', 'ODBC Driver 17 for SQL Server')

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

            self.connection = pyodbc.connect(connection_string, timeout=10)
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
            print("✓ Conexão fechada com sucesso")

    def execute_query(self, query: str, params: tuple = None, auto_reconnect: bool = True):
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

    def execute_scalar(self, query: str, params: tuple = None):
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

    def ensure_connection(self, max_retries: int = 3) -> bool:
        """
        Garante que existe uma conexão ativa, reconectando se necessário.

        Args:
            max_retries: Número máximo de tentativas de reconexão.

        Returns:
            bool: True se conectado com sucesso, False caso contrário.
        """
        if self.is_connected():
            return True

        print("⚠️  Conexão perdida, tentando reconectar...")

        for attempt in range(1, max_retries + 1):
            print(f"   Tentativa {attempt}/{max_retries}...")

            try:
                # Fecha conexões antigas se existirem
                if self.cursor:
                    try:
                        self.cursor.close()
                    except Exception:
                        # Ignora erros ao fechar cursor (pode já estar fechado)
                        pass
                if self.connection:
                    try:
                        self.connection.close()
                    except Exception:
                        # Ignora erros ao fechar conexão (pode já estar fechada)
                        pass

                # Tenta reconectar
                if self.connect():
                    print("✓ Reconexão bem-sucedida!")
                    return True

            except Exception as e:
                print(f"   ✗ Falha na tentativa {attempt}: {e}")

            if attempt < max_retries:
                import time
                time.sleep(2 ** attempt)  # Backoff exponencial: 2s, 4s, 8s

        print("✗ Não foi possível reconectar após todas as tentativas")
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
            print(f"✗ Teste de conexão falhou: {e}")
            return False

    def __enter__(self):
        """Context manager enter."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
