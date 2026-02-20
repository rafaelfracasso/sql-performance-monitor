"""
Gerenciamento de conexão com PostgreSQL usando psycopg2.
"""
import psycopg2
import psycopg2.extras
from typing import Optional, List, Tuple, Any
from ..core.base_connection import BaseDatabaseConnection


class PostgreSQLConnection(BaseDatabaseConnection):
    """Gerencia conexão com PostgreSQL."""

    def __init__(self, server: str, port: str, database: str, username: str, password: str, timeout: int = 10, **kwargs):
        """
        Inicializa conexão com PostgreSQL.

        Args:
            server: Endereço do servidor
            port: Porta do servidor (default: 5432)
            database: Nome do database
            username: Usuário
            password: Senha
            timeout: Timeout de conexão em segundos (padrão: 10)
            **kwargs: Parâmetros adicionais ignorados
        """
        super().__init__()
        self.server = server
        self.port = port or '5432'
        self.database = database
        self.username = username
        self.password = password
        self.timeout = timeout

        self.connection: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extensions.cursor] = None

    def connect(self) -> bool:
        """
        Estabelece conexão com PostgreSQL.

        Returns:
            bool: True se conectado com sucesso, False caso contrário.
        """
        try:
            self.connection = psycopg2.connect(
                host=self.server,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                connect_timeout=self.timeout
            )
            self.cursor = self.connection.cursor()

            print(f"✓ Conectado ao PostgreSQL: {self.server} - Database: {self.database}")
            return True

        except (psycopg2.OperationalError, psycopg2.Error) as e:
            print(f"✗ Erro ao conectar ao PostgreSQL: {e}")
            return False

    def disconnect(self):
        """Fecha conexão com PostgreSQL."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("✓ Conexão PostgreSQL fechada com sucesso")

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

            # PostgreSQL não auto-commit por padrão, mas para queries SELECT não afeta
            return self.cursor.fetchall()

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            # Erros de conexão que requerem reconexão
            if self.connection:
                try:
                    self.connection.rollback()
                except:
                    pass

            if auto_reconnect:
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
                    except (psycopg2.OperationalError, psycopg2.Error) as retry_error:
                        if self.connection:
                            try:
                                self.connection.rollback()
                            except:
                                pass
                        print(f"✗ Erro ao re-executar query: {retry_error}")
                        return None
            else:
                print(f"✗ Erro ao executar query: {e}")

            return None

        except psycopg2.Error as e:
            if self.connection:
                try:
                    self.connection.rollback()
                except:
                    pass
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
        if not self.connection or self.connection.closed:
            return False

        try:
            self.cursor.execute("SELECT 1")
            self.cursor.fetchone()
            return True
        except (psycopg2.OperationalError, psycopg2.InterfaceError, AttributeError):
            return False

    def test_connection(self) -> bool:
        """
        Testa conexão executando query simples.

        Returns:
            bool: True se conexão está funcionando.
        """
        try:
            result = self.execute_scalar("SELECT version()")
            if result:
                print(f"✓ PostgreSQL Version: {result[:80]}...")
                return True
            return False
        except Exception as e:
            print(f"✗ Teste de conexão PostgreSQL falhou: {e}")
            return False

    def get_version(self) -> Optional[str]:
        """
        Retorna versão do PostgreSQL.

        Returns:
            String com versão ou None.
        """
        return self.execute_scalar("SELECT version()")
