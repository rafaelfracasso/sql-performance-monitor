"""
Cache em memória para metadados de DDL e índices.

Reduz overhead de queries repetitivas ao banco de dados, especialmente
importante para HANA onde o problema N+1 de get_table_indexes era crítico.
"""
from typing import Callable, Any, Optional, Dict, Tuple
from datetime import datetime, timedelta
from .metrics_store import MetricsStore


class MetadataCache:
    """
    Cache em memória para DDL e índices de tabelas.

    Features:
    - TTL configurável por entry
    - Configuração dinâmica via DuckDB
    - Clear manual via API
    - Thread-safe por design (dict do Python)
    """

    def __init__(self, metrics_store: MetricsStore):
        """
        Inicializa cache de metadados.

        Args:
            metrics_store: Instância do MetricsStore para carregar configuração
        """
        self.metrics_store = metrics_store
        self.cache: Dict[str, Tuple[Any, datetime]] = {}
        self._load_config()

    def _load_config(self):
        """Carrega configuração de cache do DuckDB."""
        query = """
            SELECT enabled, ttl_hours, max_entries, cache_ddl, cache_indexes
            FROM metadata_cache_config
            WHERE id = 1
        """
        result = self.metrics_store.execute_query(query)

        if result and len(result) > 0:
            row = result[0]
            self.enabled = row[0]
            self.ttl_hours = row[1]
            self.max_entries = row[2]
            self.cache_ddl = row[3]
            self.cache_indexes = row[4]
        else:
            # Defaults se não configurado
            self.enabled = True
            self.ttl_hours = 24
            self.max_entries = 1000
            self.cache_ddl = True
            self.cache_indexes = True

    def get_or_fetch(self, key: str, fetch_func: Callable[[], Any]) -> Any:
        """
        Retorna valor do cache ou executa fetch_func se cache miss.

        Args:
            key: Chave do cache (ex: "HANA.SCHEMA.TABLE:ddl")
            fetch_func: Função a ser executada em cache miss

        Returns:
            Valor do cache ou resultado de fetch_func
        """
        if not self.enabled:
            return fetch_func()

        # Verificar se está no cache e é válido
        if key in self.cache:
            value, timestamp = self.cache[key]
            age_hours = (datetime.now() - timestamp).total_seconds() / 3600

            if age_hours < self.ttl_hours:
                # Cache hit válido
                return value

        # Cache miss ou expirado - executar fetch
        value = fetch_func()

        # Verificar limite de entries antes de inserir
        if len(self.cache) >= self.max_entries:
            # Remover entry mais antiga
            oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k][1])
            del self.cache[oldest_key]

        # Armazenar no cache
        self.cache[key] = (value, datetime.now())
        return value

    def clear(self):
        """Limpa todo o cache."""
        self.cache.clear()

    def clear_prefix(self, prefix: str):
        """
        Limpa entries com determinado prefixo.

        Args:
            prefix: Prefixo das keys a serem removidas (ex: "HANA.SCHEMA.TABLE")
        """
        keys_to_remove = [key for key in self.cache.keys() if key.startswith(prefix)]
        for key in keys_to_remove:
            del self.cache[key]

    def get_stats(self) -> Dict[str, Any]:
        """
        Retorna estatísticas do cache.

        Returns:
            Dict com estatísticas de uso
        """
        return {
            'enabled': self.enabled,
            'ttl_hours': self.ttl_hours,
            'max_entries': self.max_entries,
            'current_entries': len(self.cache),
            'cache_ddl': self.cache_ddl,
            'cache_indexes': self.cache_indexes
        }

    def reload_config(self):
        """Recarrega configuração do DuckDB (após mudanças na UI)."""
        self._load_config()
