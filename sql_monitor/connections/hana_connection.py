"""
Gerenciamento de conexão com SAP HANA usando hdbcli.
"""
from hdbcli import dbapi
from typing import Optional, List, Tuple, Any
from ..core.base_connection import BaseDatabaseConnection


class HANAConnection(BaseDatabaseConnection):
    """Gerencia conexão com SAP HANA."""

    def __init__(self, server: str, port: str, database: str, username: str, password: str, timeout: int = 10, **kwargs):
        """
        Inicializa conexão com SAP HANA.

        Args:
            server: Endereço do servidor HANA
            port: Porta do servidor (default: 30015 para SYSTEMDB, 30013 para tenant)
            database: Nome do database/tenant (opcional)
            username: Usuário
            password: Senha
            timeout: Timeout de conexão em segundos (padrão: 10)
            **kwargs: Parâmetros adicionais (ex: encrypt=True, sslValidateCertificate=False)
        """
        super().__init__()
        self.server = server
        self.port = port or '30015'
        self.database = database
        self.username = username
        self.password = password
        self.timeout = timeout
        self.kwargs = kwargs

        self.connection: Optional[dbapi.Connection] = None
        self.cursor: Optional[dbapi.Cursor] = None

    def connect(self) -> bool:
        """
        Estabelece conexão com SAP HANA.

        Returns:
            bool: True se conectado com sucesso, False caso contrário.
        """
        try:
            # Preparar parâmetros de conexão
            conn_params = {
                'address': self.server,
                'port': int(self.port),
                'user': self.username,
                'password': self.password,
                'timeout': self.timeout * 1000  # HANA usa milissegundos
            }

            # Adicionar database se especificado (para tenant)
            if self.database:
                conn_params['databaseName'] = self.database

            # Adicionar parâmetros extras (encrypt, ssl, etc)
            conn_params.update(self.kwargs)

            # Conectar
            self.connection = dbapi.connect(**conn_params)
            self.cursor = self.connection.cursor()

            db_info = f"{self.server}:{self.port}"
            if self.database:
                db_info += f" - Database: {self.database}"

            print(f"✓ Conectado ao SAP HANA: {db_info}")
            return True

        except dbapi.Error as e:
            print(f"✗ Erro ao conectar ao SAP HANA: {e}")
            return False
        except Exception as e:
            print(f"✗ Erro inesperado ao conectar: {e}")
            return False

    def disconnect(self):
        """Fecha conexão com SAP HANA."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("✓ Conexão SAP HANA fechada com sucesso")

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

            # HANA auto-commit por padrão, mas queries SELECT não afetam
            return self.cursor.fetchall()

        except dbapi.Error as e:
            # Erros de conexão que requerem reconexão
            if auto_reconnect and self._is_connection_error(e):
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
                    except dbapi.Error as retry_error:
                        print(f"✗ Erro ao re-executar query: {retry_error}")
                        return None
            else:
                print(f"✗ Erro ao executar query: {e}")

            return None

        except Exception as e:
            print(f"✗ Erro inesperado ao executar query: {e}")
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
            self.cursor.execute("SELECT 1 FROM DUMMY")
            self.cursor.fetchone()
            return True
        except (dbapi.Error, AttributeError):
            return False

    def test_connection(self) -> bool:
        """
        Testa conexão executando query simples.

        Returns:
            bool: True se conexão está funcionando.
        """
        try:
            result = self.execute_scalar("SELECT VERSION FROM SYS.M_DATABASE")
            if result:
                print(f"✓ SAP HANA Version: {result}")
                return True
            return False
        except Exception as e:
            print(f"✗ Teste de conexão SAP HANA falhou: {e}")
            return False

    def get_version(self) -> Optional[str]:
        """
        Retorna versão do SAP HANA.

        Returns:
            String com versão ou None.
        """
        return self.execute_scalar("SELECT VERSION FROM SYS.M_DATABASE")

    def _is_connection_error(self, error: Exception) -> bool:
        """
        Verifica se erro é de conexão perdida.

        Args:
            error: Exceção capturada

        Returns:
            True se for erro de conexão.
        """
        # Erros comuns de conexão no HANA
        connection_errors = [
            'connection lost',
            'connection closed',
            'broken pipe',
            'not connected',
            'connection failed'
        ]

        error_msg = str(error).lower()
        return any(err in error_msg for err in connection_errors)
