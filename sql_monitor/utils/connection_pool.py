"""
Sistema de Connection Pooling para reutilização de conexões de banco de dados.

Previne resource leaks e reduz overhead de criação/destruição de conexões.
"""
import threading
import queue
import time
from typing import Dict, Any, Optional, Callable
from contextlib import contextmanager
import logging


logger = logging.getLogger(__name__)


class ConnectionPool:
    """
    Pool de conexões genérico que funciona com qualquer tipo de conexão.

    Gerencia um pool de conexões reutilizáveis, evitando criar novas conexões
    para cada operação.
    """

    def __init__(
        self,
        connection_factory: Callable[[], Any],
        min_size: int = 1,
        max_size: int = 5,
        max_idle_time: int = 300,  # 5 minutos
        connection_test_fn: Optional[Callable[[Any], bool]] = None
    ):
        """
        Inicializa connection pool.

        Args:
            connection_factory: Função que cria uma nova conexão
            min_size: Número mínimo de conexões no pool
            max_size: Número máximo de conexões no pool
            max_idle_time: Tempo máximo de inatividade (segundos) antes de fechar conexão
            connection_test_fn: Função para testar se conexão está ativa (retorna bool)
        """
        self.connection_factory = connection_factory
        self.min_size = min_size
        self.max_size = max_size
        self.max_idle_time = max_idle_time
        self.connection_test_fn = connection_test_fn

        # Pool de conexões disponíveis
        self.pool: queue.Queue = queue.Queue(maxsize=max_size)

        # Rastreamento de conexões criadas
        self.created_connections = 0
        self.active_connections = 0
        self.lock = threading.Lock()

        # Metadata de conexões (timestamp de último uso)
        self.connection_metadata: Dict[int, float] = {}

        # Flag de shutdown
        self.closed = False

        # Criar conexões mínimas iniciais
        self._initialize_pool()

    def _initialize_pool(self):
        """Cria conexões mínimas iniciais no pool."""
        for _ in range(self.min_size):
            try:
                conn = self._create_connection()
                if conn:
                    self.pool.put(conn, block=False)
            except queue.Full:
                break
            except Exception as e:
                logger.error(f"Erro ao criar conexão inicial: {e}")

    def _create_connection(self) -> Optional[Any]:
        """
        Cria uma nova conexão.

        Returns:
            Nova conexão ou None se falhar.
        """
        try:
            with self.lock:
                if self.created_connections >= self.max_size:
                    logger.warning(f"Pool atingiu limite máximo de {self.max_size} conexões")
                    return None

                conn = self.connection_factory()
                self.created_connections += 1
                self.connection_metadata[id(conn)] = time.time()

                logger.debug(f"Nova conexão criada (total: {self.created_connections})")
                return conn

        except Exception as e:
            logger.error(f"Erro ao criar conexão: {e}")
            return None

    def _test_connection(self, conn: Any) -> bool:
        """
        Testa se conexão está ativa.

        Args:
            conn: Conexão a testar.

        Returns:
            True se conexão está ativa, False caso contrário.
        """
        if self.connection_test_fn:
            try:
                return self.connection_test_fn(conn)
            except Exception as e:
                logger.debug(f"Teste de conexão falhou: {e}")
                return False
        return True

    def _is_connection_stale(self, conn: Any) -> bool:
        """
        Verifica se conexão está inativa há muito tempo.

        Args:
            conn: Conexão a verificar.

        Returns:
            True se conexão está stale, False caso contrário.
        """
        conn_id = id(conn)
        if conn_id in self.connection_metadata:
            last_used = self.connection_metadata[conn_id]
            idle_time = time.time() - last_used
            return idle_time > self.max_idle_time
        return False

    @contextmanager
    def get_connection(self, timeout: float = 30):
        """
        Context manager para obter conexão do pool.

        Exemplo:
            with pool.get_connection() as conn:
                result = conn.execute_query("SELECT 1")

        Args:
            timeout: Tempo máximo de espera por uma conexão (segundos).

        Yields:
            Conexão do pool.

        Raises:
            TimeoutError: Se não conseguir obter conexão no timeout.
        """
        if self.closed:
            raise RuntimeError("Connection pool já foi fechado")

        conn = None
        acquired = False

        try:
            # Tentar obter conexão existente do pool
            try:
                conn = self.pool.get(timeout=timeout)
                acquired = True

                # Testar se conexão ainda está ativa
                if not self._test_connection(conn):
                    logger.debug("Conexão do pool está inativa, criando nova")
                    self._discard_connection(conn)
                    conn = self._create_connection()

                # Verificar se conexão está stale
                elif self._is_connection_stale(conn):
                    logger.debug("Conexão do pool está stale, criando nova")
                    self._discard_connection(conn)
                    conn = self._create_connection()

            except queue.Empty:
                # Pool vazio, criar nova conexão
                logger.debug("Pool vazio, criando nova conexão")
                conn = self._create_connection()

            if not conn:
                raise TimeoutError(f"Não foi possível obter conexão em {timeout}s")

            with self.lock:
                self.active_connections += 1

            # Atualizar timestamp de último uso
            self.connection_metadata[id(conn)] = time.time()

            # Yield conexão para uso
            yield conn

        finally:
            # Devolver conexão ao pool ou descartar se houver erro
            if conn:
                try:
                    # Testar conexão antes de devolver ao pool
                    if self._test_connection(conn):
                        self.pool.put(conn, block=False)
                    else:
                        logger.debug("Conexão falhou no teste, descartando")
                        self._discard_connection(conn)
                except queue.Full:
                    # Pool cheio, descartar conexão extra
                    logger.debug("Pool cheio, descartando conexão extra")
                    self._discard_connection(conn)

                with self.lock:
                    self.active_connections -= 1

            # Marcar como devolvida se foi adquirida do pool
            if acquired:
                try:
                    self.pool.task_done()
                except ValueError:
                    pass

    def _discard_connection(self, conn: Any):
        """
        Descarta uma conexão (fecha e remove do tracking).

        Args:
            conn: Conexão a descartar.
        """
        try:
            # Tentar fechar conexão gracefully
            if hasattr(conn, 'disconnect'):
                conn.disconnect()
            elif hasattr(conn, 'close'):
                conn.close()

            with self.lock:
                self.created_connections -= 1
                conn_id = id(conn)
                if conn_id in self.connection_metadata:
                    del self.connection_metadata[conn_id]

            logger.debug(f"Conexão descartada (total: {self.created_connections})")

        except Exception as e:
            logger.error(f"Erro ao descartar conexão: {e}")

    def close_all(self):
        """Fecha todas as conexões no pool."""
        if self.closed:
            return

        logger.info("Fechando connection pool...")

        self.closed = True

        # Fechar todas as conexões no pool
        while not self.pool.empty():
            try:
                conn = self.pool.get(block=False)
                self._discard_connection(conn)
            except queue.Empty:
                break

        logger.info(f"Connection pool fechado ({self.created_connections} conexões restantes)")

    def get_stats(self) -> Dict[str, int]:
        """
        Retorna estatísticas do pool.

        Returns:
            Dicionário com estatísticas.
        """
        return {
            'created_connections': self.created_connections,
            'active_connections': self.active_connections,
            'available_connections': self.pool.qsize(),
            'min_size': self.min_size,
            'max_size': self.max_size
        }

    def __del__(self):
        """Destrutor - fecha pool se ainda estiver aberto."""
        if not self.closed:
            self.close_all()
