"""
Sistema de cache para evitar análises duplicadas de queries.
Mantém histórico de queries analisadas com TTL de 24 horas deslizantes.
Usa DuckDB para persistência em vez de JSON.
"""
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from sql_monitor.utils.metrics_store import MetricsStore


class QueryCache:
    """
    Gerencia cache de queries analisadas para evitar reenvio duplicado ao LLM.

    Funcionalidades:
    - Hash SHA256 único baseado na estrutura da query (sem valores literais)
    - TTL de 24 horas deslizantes por query
    - Persistência em DuckDB (substituindo JSON)
    - Limpeza automática de entradas expiradas
    - Rastreamento de quantas vezes cada query foi vista
    """

    def __init__(self, metrics_store: 'MetricsStore'):
        """
        Inicializa o sistema de cache usando DuckDB.

        Args:
            metrics_store: Instância do MetricsStore para persistência
        """
        self.metrics_store = metrics_store
        self._load_config()

    def _load_config(self):
        """Carrega configuração do DuckDB."""
        result = self.metrics_store.execute_query(
            "SELECT enabled, ttl_hours FROM query_cache_config WHERE id = 1"
        )
        if result and len(result) > 0:
            self.enabled = result[0][0]
            self.ttl_hours = result[0][1]
        else:
            self.enabled = True
            self.ttl_hours = 24

    def generate_hash(
        self,
        sanitized_query: str,
        database: str,
        schema: str,
        table: str
    ) -> str:
        """
        Gera hash SHA256 único da query baseado em sua estrutura.

        Args:
            sanitized_query: Query já sanitizada (sem valores literais).
            database: Nome do database.
            schema: Nome do schema.
            table: Nome da tabela.

        Returns:
            String hexadecimal do hash SHA256.

        Note:
            Queries com valores literais diferentes mas estrutura idêntica
            terão o mesmo hash (comportamento desejado).
        """
        # Chave única: database.schema.table::query_sanitizada
        key = f"{database}.{schema}.{table}::{sanitized_query}"
        return hashlib.sha256(key.encode('utf-8')).hexdigest()

    def is_cached_and_valid(self, query_hash: str) -> bool:
        """
        Verifica se query está em cache e ainda é válida no DuckDB.

        Args:
            query_hash: Hash SHA256 da query.

        Returns:
            True se query está em cache e ainda não expirou (< TTL).
        """
        if not self.enabled:
            return False

        result = self.metrics_store.execute_query("""
            SELECT 1 FROM query_cache_entries
            WHERE query_hash = ? AND expires_at > CURRENT_TIMESTAMP
        """, (query_hash,))

        return len(result) > 0

    def get_cached_query(self, query_hash: str) -> Optional[Dict]:
        """
        Retorna informações da query do DuckDB.

        Args:
            query_hash: Hash SHA256 da query.

        Returns:
            Dicionário com informações da query ou None se não existir.
        """
        result = self.metrics_store.execute_query("""
            SELECT analyzed_at, expires_at, last_seen, seen_count,
                   database_name, schema_name, table_name, log_file, query_preview
            FROM query_cache_entries
            WHERE query_hash = ?
        """, (query_hash,))

        if not result or len(result) == 0:
            return None

        row = result[0]
        return {
            'analyzed_at': row[0],
            'expires_at': row[1],
            'last_seen': row[2],
            'seen_count': row[3],
            'database': row[4],
            'schema': row[5],
            'table': row[6],
            'log_file': row[7],
            'query_preview': row[8]
        }

    def get_hours_since_analysis(self, query_hash: str) -> float:
        """
        Calcula horas desde análise usando DuckDB.

        Args:
            query_hash: Hash SHA256 da query.

        Returns:
            Número de horas (float) desde a análise.
        """
        result = self.metrics_store.execute_query("""
            SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - analyzed_at)) / 3600
            FROM query_cache_entries
            WHERE query_hash = ?
        """, (query_hash,))

        return result[0][0] if result else 0.0

    def add_analyzed_query(
        self,
        query_hash: str,
        database: str,
        schema: str,
        table: str,
        log_file: str,
        query_preview: str
    ) -> None:
        """
        Adiciona query ao cache no DuckDB.

        Args:
            query_hash: Hash SHA256 da query.
            database: Nome do database.
            schema: Nome do schema.
            table: Nome da tabela.
            log_file: Caminho do arquivo de log gerado.
            query_preview: Primeiros caracteres da query para referência.
        """
        if not self.enabled:
            return

        self.metrics_store.execute("""
            INSERT INTO query_cache_entries
            (query_hash, analyzed_at, expires_at, last_seen, seen_count,
             database_name, schema_name, table_name, log_file, query_preview)
            VALUES (?, CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP + INTERVAL ? HOUR,
                    CURRENT_TIMESTAMP, 1, ?, ?, ?, ?, ?)
            ON CONFLICT (query_hash) DO UPDATE SET
                last_seen = CURRENT_TIMESTAMP,
                seen_count = seen_count + 1
        """, (query_hash, self.ttl_hours, database, schema, table,
              log_file, query_preview))

    def update_last_seen(self, query_hash: str) -> None:
        """
        Atualiza timestamp e contador no DuckDB.

        Args:
            query_hash: Hash SHA256 da query.
        """
        if not self.enabled:
            return

        self.metrics_store.execute("""
            UPDATE query_cache_entries
            SET last_seen = CURRENT_TIMESTAMP,
                seen_count = seen_count + 1
            WHERE query_hash = ?
        """, (query_hash,))

    def get_cache_size(self) -> int:
        """
        Retorna número de queries em cache no DuckDB.

        Returns:
            Número de queries armazenadas.
        """
        result = self.metrics_store.execute_query(
            "SELECT COUNT(*) FROM query_cache_entries"
        )
        return result[0][0] if result else 0

    def cleanup_expired(self) -> int:
        """
        Remove entradas expiradas do DuckDB.

        Returns:
            Número de entradas removidas.
        """
        if not self.enabled:
            return 0

        result = self.metrics_store.execute_query("""
            DELETE FROM query_cache_entries
            WHERE expires_at < CURRENT_TIMESTAMP
            RETURNING query_hash
        """)

        return len(result) if result else 0

    def get_statistics(self) -> Dict:
        """
        Retorna estatísticas do cache no DuckDB.

        Returns:
            Dicionário com estatísticas:
            - total_queries: Total de queries no cache
            - total_views: Soma de todas as visualizações
            - avg_views_per_query: Média de visualizações por query
            - oldest_analysis: Data da análise mais antiga
        """
        result = self.metrics_store.execute_query("""
            SELECT
                COUNT(*) as total_queries,
                SUM(seen_count) as total_views,
                AVG(seen_count) as avg_views_per_query,
                MIN(analyzed_at) as oldest_analysis
            FROM query_cache_entries
        """)

        if not result or len(result) == 0:
            return {
                'total_queries': 0,
                'total_views': 0,
                'avg_views_per_query': 0,
                'oldest_analysis': None
            }

        row = result[0]
        return {
            'total_queries': row[0] or 0,
            'total_views': row[1] or 0,
            'avg_views_per_query': row[2] or 0,
            'oldest_analysis': str(row[3]) if row[3] else None
        }
