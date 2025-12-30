"""
Sistema de cache para evitar análises duplicadas de queries.
Mantém histórico de queries analisadas com TTL de 24 horas deslizantes.
"""
import os
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple


class QueryCache:
    """
    Gerencia cache de queries analisadas para evitar reenvio duplicado ao LLM.

    Funcionalidades:
    - Hash SHA256 único baseado na estrutura da query (sem valores literais)
    - TTL de 24 horas deslizantes por query
    - Persistência em arquivo JSON
    - Limpeza automática de entradas expiradas
    - Rastreamento de quantas vezes cada query foi vista
    """

    def __init__(self, config: Dict):
        """
        Inicializa o sistema de cache.

        Args:
            config: Dicionário com configurações do cache.
                   Espera-se: {
                       'enabled': bool,
                       'ttl_hours': int,
                       'cache_file': str,
                       'auto_save_interval': int
                   }
        """
        self.enabled = config.get('enabled', True)
        self.ttl_hours = config.get('ttl_hours', 24)
        self.cache_file = config.get('cache_file', 'logs/query_cache.json')
        self.auto_save_interval = config.get('auto_save_interval', 300)

        # Cache em memória: {hash: query_info}
        self.cache: Dict[str, Dict] = {}

        # Garante que o diretório existe
        cache_dir = os.path.dirname(self.cache_file)
        if cache_dir and not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

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
        Verifica se query está em cache e ainda é válida (TTL não expirado).

        Args:
            query_hash: Hash SHA256 da query.

        Returns:
            True se query está em cache e ainda não expirou (< TTL).
        """
        if not self.enabled:
            return False

        if query_hash not in self.cache:
            return False

        entry = self.cache[query_hash]
        expires_at = datetime.fromisoformat(entry['expires_at'])

        return datetime.now() < expires_at

    def get_cached_query(self, query_hash: str) -> Optional[Dict]:
        """
        Retorna informações da query do cache.

        Args:
            query_hash: Hash SHA256 da query.

        Returns:
            Dicionário com informações da query ou None se não existir.
        """
        return self.cache.get(query_hash)

    def get_hours_since_analysis(self, query_hash: str) -> float:
        """
        Retorna quantas horas se passaram desde a análise da query.

        Args:
            query_hash: Hash SHA256 da query.

        Returns:
            Número de horas (float) desde a análise.
        """
        if query_hash not in self.cache:
            return 0.0

        entry = self.cache[query_hash]
        analyzed = datetime.fromisoformat(entry['analyzed_at'])
        delta = datetime.now() - analyzed

        return delta.total_seconds() / 3600

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
        Adiciona query ao cache após análise pelo LLM.

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

        now = datetime.now()
        expires = now + timedelta(hours=self.ttl_hours)

        self.cache[query_hash] = {
            'analyzed_at': now.isoformat(),
            'expires_at': expires.isoformat(),
            'last_seen': now.isoformat(),
            'seen_count': 1,
            'database': database,
            'schema': schema,
            'table': table,
            'log_file': log_file,
            'query_preview': query_preview
        }

    def update_last_seen(self, query_hash: str) -> None:
        """
        Atualiza timestamp de última visualização e incrementa contador.

        Args:
            query_hash: Hash SHA256 da query.
        """
        if not self.enabled:
            return

        if query_hash in self.cache:
            self.cache[query_hash]['last_seen'] = datetime.now().isoformat()
            self.cache[query_hash]['seen_count'] += 1

    def load_cache(self) -> Tuple[int, int]:
        """
        Carrega cache do arquivo JSON e remove entradas expiradas.

        Returns:
            Tupla (loaded, expired) com contadores de:
            - loaded: Número de queries válidas carregadas
            - expired: Número de queries expiradas removidas
        """
        if not self.enabled:
            return 0, 0

        if not os.path.exists(self.cache_file):
            return 0, 0

        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            loaded = 0
            expired = 0
            now = datetime.now()

            for hash_key, entry in data.get('queries', {}).items():
                expires_at = datetime.fromisoformat(entry['expires_at'])

                if now < expires_at:
                    # Query ainda válida
                    self.cache[hash_key] = entry
                    loaded += 1
                else:
                    # Query expirada (>TTL)
                    expired += 1

            return loaded, expired

        except Exception as e:
            print(f"⚠️  Erro ao carregar cache: {e}")
            return 0, 0

    def save_cache(self) -> bool:
        """
        Salva cache em arquivo JSON.

        Returns:
            True se salvou com sucesso, False caso contrário.
        """
        if not self.enabled:
            return False

        try:
            data = {
                'last_updated': datetime.now().isoformat(),
                'queries': self.cache
            }

            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            return True

        except Exception as e:
            print(f"⚠️  Erro ao salvar cache: {e}")
            return False

    def get_cache_size(self) -> int:
        """
        Retorna número de queries no cache.

        Returns:
            Número de queries armazenadas.
        """
        return len(self.cache)

    def cleanup_expired(self) -> int:
        """
        Remove entradas expiradas do cache em memória.

        Returns:
            Número de entradas removidas.
        """
        if not self.enabled:
            return 0

        now = datetime.now()
        expired_keys = []

        for hash_key, entry in self.cache.items():
            expires_at = datetime.fromisoformat(entry['expires_at'])
            if now >= expires_at:
                expired_keys.append(hash_key)

        for key in expired_keys:
            del self.cache[key]

        return len(expired_keys)

    def get_statistics(self) -> Dict:
        """
        Retorna estatísticas sobre o cache.

        Returns:
            Dicionário com estatísticas:
            - total_queries: Total de queries no cache
            - total_views: Soma de todas as visualizações
            - avg_views_per_query: Média de visualizações por query
            - oldest_analysis: Data da análise mais antiga
        """
        if not self.cache:
            return {
                'total_queries': 0,
                'total_views': 0,
                'avg_views_per_query': 0,
                'oldest_analysis': None
            }

        total_views = sum(entry['seen_count'] for entry in self.cache.values())
        oldest = min(
            datetime.fromisoformat(entry['analyzed_at'])
            for entry in self.cache.values()
        )

        return {
            'total_queries': len(self.cache),
            'total_views': total_views,
            'avg_views_per_query': total_views / len(self.cache),
            'oldest_analysis': oldest.isoformat()
        }
