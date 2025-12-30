"""
Monitor individual para um banco de dados específico.
"""
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from ..core.database_types import DatabaseType
from ..factories.database_factory import DatabaseFactory
from ..utils.query_cache import QueryCache
from ..utils.query_sanitizer import QuerySanitizer
from ..utils.llm_analyzer import LLMAnalyzer
from ..utils.logger import PerformanceLogger
from ..utils.performance_checker import PerformanceChecker
from ..utils.metrics_store import MetricsStore


class DatabaseMonitor:
    """
    Monitor individual para um banco de dados.

    Responsável por:
    - Criar componentes via Factory (connection, collector, extractor)
    - Coletar queries problemáticas
    - Sanitizar dados sensíveis
    - Analisar via LLM
    - Salvar logs estruturados
    - Gerenciar cache de queries
    """

    def __init__(
        self,
        db_type: DatabaseType,
        instance_name: str,
        credentials: Dict[str, Any],
        config: Dict[str, Any],
        cache: QueryCache,
        sanitizer: QuerySanitizer,
        llm_analyzer: LLMAnalyzer,
        logger: PerformanceLogger,
        performance_checker: PerformanceChecker,
        metrics_store: Optional[MetricsStore] = None
    ):
        """
        Inicializa monitor para uma instância de banco.

        Args:
            db_type: Tipo do banco (DatabaseType enum)
            instance_name: Nome da instância (usado para logs)
            credentials: Credenciais de conexão
            config: Configuração completa (config.json)
            cache: Cache compartilhado para este tipo de banco
            sanitizer: Sanitizador de queries
            llm_analyzer: Analisador LLM
            logger: Logger de performance
            performance_checker: Verificador de thresholds
            metrics_store: Store DuckDB para métricas e observabilidade
        """
        self.db_type = db_type
        self.instance_name = instance_name
        self.credentials = credentials
        self.config = config
        self.cache = cache
        self.sanitizer = sanitizer
        self.llm_analyzer = llm_analyzer
        self.logger = logger
        self.performance_checker = performance_checker
        self.metrics_store = metrics_store

        # Componentes criados pela Factory
        self.connection = None
        self.collector = None
        self.extractor = None

        # Estatísticas
        self.stats = {
            'queries_collected': 0,
            'queries_analyzed': 0,
            'cache_hits': 0,
            'errors': 0
        }

        # Ciclo atual (para tracking)
        self.current_cycle_id = None

    def initialize(self) -> bool:
        """
        Inicializa componentes via Factory e conecta ao banco.

        Returns:
            True se inicializado com sucesso, False caso contrário.
        """
        try:
            print(f"\n[{self.instance_name}] Inicializando componentes via Factory...")

            # Obter timeout de conexão do config (padrão: 10)
            timeout = self.config.get('timeouts', {}).get('database_connect', 10)

            # Criar componentes via Factory
            self.connection, self.collector, self.extractor = DatabaseFactory.create_components(
                self.db_type,
                self.credentials,
                timeout=timeout
            )

            # Conectar ao banco
            if not self.connection.connect():
                print(f"[{self.instance_name}] Falha ao conectar")
                return False

            # Testar conexão
            if not self.connection.test_connection():
                print(f"[{self.instance_name}] Falha no teste de conexão")
                self.connection.disconnect()
                return False

            print(f"[{self.instance_name}] Inicializado com sucesso")
            return True

        except Exception as e:
            print(f"[{self.instance_name}] Erro ao inicializar: {e}")
            self.stats['errors'] += 1
            return False

    def shutdown(self):
        """Desconecta do banco e limpa recursos."""
        if self.connection:
            self.connection.disconnect()
            print(f"[{self.instance_name}] Desconectado")

    def run_cycle(self) -> Dict[str, Any]:
        """
        Executa um ciclo de monitoramento.

        Returns:
            Dicionário com estatísticas do ciclo.
        """
        cycle_start = datetime.now()

        cycle_stats = {
            'instance': self.instance_name,
            'db_type': self.db_type.value,
            'timestamp': cycle_start.isoformat(),
            'queries_found': 0,
            'queries_analyzed': 0,
            'cache_hits': 0,
            'errors': 0
        }

        # Registrar início do ciclo no metrics_store
        if self.metrics_store:
            self.current_cycle_id = self.metrics_store.start_monitoring_cycle(
                instance_name=self.instance_name,
                db_type=self.db_type.value
            )

        error_message = None

        try:
            print(f"\n[{self.instance_name}] Iniciando ciclo de monitoramento...")

            # Coletar queries ativas
            active_queries = self._collect_active_queries()
            cycle_stats['queries_found'] += len(active_queries)

            # Coletar expensive queries
            expensive_queries = self._collect_expensive_queries()
            cycle_stats['queries_found'] += len(expensive_queries)

            # Coletar table scans
            table_scans = self._collect_table_scans()
            cycle_stats['queries_found'] += len(table_scans)

            # Processar todas as queries coletadas
            all_queries = active_queries + expensive_queries + table_scans

            for query_info in all_queries:
                try:
                    # Verificar se query atende thresholds
                    if not self.performance_checker.should_analyze_query(query_info):
                        continue

                    # Verificar cache
                    query_hash = self.cache.get_query_hash(query_info.get('query_text', ''))
                    if self.cache.is_cached(query_hash):
                        cycle_stats['cache_hits'] += 1
                        self.stats['cache_hits'] += 1
                        continue

                    # Processar query
                    self._process_query(query_info)
                    cycle_stats['queries_analyzed'] += 1
                    self.stats['queries_analyzed'] += 1

                    # Adicionar ao cache
                    self.cache.add_query(query_hash, query_info)

                except Exception as e:
                    print(f"[{self.instance_name}] Erro ao processar query: {e}")
                    cycle_stats['errors'] += 1
                    self.stats['errors'] += 1

            print(f"[{self.instance_name}] Ciclo concluído: {cycle_stats['queries_found']} queries encontradas, "
                  f"{cycle_stats['queries_analyzed']} analisadas, {cycle_stats['cache_hits']} cache hits")

        except Exception as e:
            print(f"[{self.instance_name}] Erro no ciclo: {e}")
            cycle_stats['errors'] += 1
            self.stats['errors'] += 1
            error_message = str(e)

        finally:
            # Calcular duração do ciclo
            cycle_duration_ms = (datetime.now() - cycle_start).total_seconds() * 1000
            cycle_stats['cycle_duration_ms'] = cycle_duration_ms

            # Finalizar ciclo no metrics_store
            if self.metrics_store and self.current_cycle_id:
                self.metrics_store.end_monitoring_cycle(
                    cycle_id=self.current_cycle_id,
                    stats=cycle_stats,
                    error_message=error_message
                )

        return cycle_stats

    def _collect_active_queries(self) -> List[Dict]:
        """Coleta queries ativas."""
        try:
            queries = self.collector.collect_active_queries()
            self.stats['queries_collected'] += len(queries)
            return queries
        except Exception as e:
            print(f"[{self.instance_name}] Erro ao coletar queries ativas: {e}")
            return []

    def _collect_expensive_queries(self) -> List[Dict]:
        """Coleta expensive queries."""
        try:
            queries = self.collector.collect_recent_expensive_queries()
            self.stats['queries_collected'] += len(queries)
            return queries
        except Exception as e:
            print(f"[{self.instance_name}] Erro ao coletar expensive queries: {e}")
            return []

    def _collect_table_scans(self) -> List[Dict]:
        """Coleta table scans."""
        try:
            queries = self.collector.get_table_scan_queries()
            self.stats['queries_collected'] += len(queries)
            return queries
        except Exception as e:
            print(f"[{self.instance_name}] Erro ao coletar table scans: {e}")
            return []

    def _process_query(self, query_info: Dict):
        """
        Processa uma query: sanitiza, extrai metadados, analisa via LLM e salva log.

        Args:
            query_info: Informações da query coletada
        """
        # Sanitizar query
        query_text = query_info.get('query_text', '') or query_info.get('full_query_text', '')
        sanitized_result = self.sanitizer.sanitize_query(query_text)

        # Extrair informações de tabela
        tables = self.extractor.extract_table_info_from_query(query_text)

        if not tables:
            print(f"[{self.instance_name}] Nenhuma tabela identificada na query")
            return

        # Processar primeira tabela (simplificação)
        table_info = tables[0]
        schema = table_info.get('schema', 'dbo')
        table_name = table_info.get('table', 'unknown')

        # Extrair metadados da tabela
        database_name = query_info.get('database_name', 'current')

        # Gerar hash da query
        query_hash = None
        if self.metrics_store:
            query_hash = self.metrics_store.generate_query_hash(
                sanitized_query=sanitized_result['sanitized_query'],
                database=database_name,
                schema=schema,
                table=table_name
            )

            # Verificar se query já foi analisada (cache via metrics_store)
            if self.metrics_store.is_query_analyzed_and_valid(query_hash):
                print(f"[{self.instance_name}] Query já analisada (cache): {query_hash[:8]}...")
                return

        try:
            ddl = self.extractor.get_table_ddl(database_name, schema, table_name)
            indexes = self.extractor.get_table_indexes(database_name, schema, table_name)
        except Exception as e:
            print(f"[{self.instance_name}] Erro ao extrair metadados: {e}")
            ddl = "N/A"
            indexes = []

        # Adicionar query coletada ao metrics_store
        if self.metrics_store and query_hash:
            # Extrair métricas para persistência
            metrics_data = {
                'cpu_time_ms': query_info.get('cpu_time_ms'),
                'duration_ms': query_info.get('duration_ms'),
                'logical_reads': query_info.get('logical_reads'),
                'physical_reads': query_info.get('physical_reads'),
                'writes': query_info.get('writes'),
                'row_count': query_info.get('row_count'),
                'memory_mb': query_info.get('memory_mb'),
                'wait_time_ms': query_info.get('wait_time_ms'),
                'status': query_info.get('status'),
                'wait_type': query_info.get('wait_type'),
                'execution_count': query_info.get('execution_count')
            }

            self.metrics_store.add_collected_query(
                query_hash=query_hash,
                instance_name=self.instance_name,
                db_type=self.db_type.value,
                query_text=query_text,
                sanitized_query=sanitized_result['sanitized_query'],
                database_name=database_name,
                schema_name=schema,
                table_name=table_name,
                query_type=query_info.get('query_type', 'unknown'),
                metrics=metrics_data
            )

        # Preparar contexto para LLM
        context = {
            'sanitized_query': sanitized_result['sanitized_query'],
            'placeholders': sanitized_result['placeholders'],
            'ddl': ddl,
            'existing_indexes': indexes,
            'metrics': query_info
        }

        # Analisar via LLM
        analysis_start = datetime.now()
        try:
            analysis = self.llm_analyzer.analyze_query(context)
        except Exception as e:
            print(f"[{self.instance_name}] Erro na análise LLM: {e}")
            analysis = "Análise LLM não disponível"

        analysis_duration_ms = (datetime.now() - analysis_start).total_seconds() * 1000

        # Salvar análise LLM no metrics_store
        if self.metrics_store and query_hash:
            self.metrics_store.add_llm_analysis(
                query_hash=query_hash,
                instance_name=self.instance_name,
                database_name=database_name,
                schema_name=schema,
                table_name=table_name,
                analysis_text=analysis,
                recommendations="",  # TODO: Extrair recomendações do analysis
                severity="medium",  # TODO: Determinar severity baseado nas métricas
                ttl_hours=self.config.get('query_cache', {}).get('ttl_hours', 24),
                analysis_duration_ms=analysis_duration_ms
            )

        # Registrar alertas de performance no metrics_store
        if self.metrics_store and query_hash:
            thresholds = self.config.get('performance_thresholds', {})

            # Verificar CPU threshold
            cpu_time = query_info.get('cpu_time_ms', 0)
            cpu_threshold = thresholds.get('cpu_time_ms', 1000)
            if cpu_time > cpu_threshold:
                self.metrics_store.add_performance_alert(
                    instance_name=self.instance_name,
                    query_hash=query_hash,
                    alert_type='cpu_threshold',
                    severity='high' if cpu_time > cpu_threshold * 2 else 'medium',
                    threshold_value=cpu_threshold,
                    actual_value=cpu_time,
                    database_name=database_name,
                    table_name=table_name,
                    query_preview=query_text[:200]
                )

            # Verificar duration threshold
            duration = query_info.get('duration_ms', 0)
            duration_threshold = thresholds.get('duration_ms', 1000)
            if duration > duration_threshold:
                self.metrics_store.add_performance_alert(
                    instance_name=self.instance_name,
                    query_hash=query_hash,
                    alert_type='duration_threshold',
                    severity='high' if duration > duration_threshold * 2 else 'medium',
                    threshold_value=duration_threshold,
                    actual_value=duration,
                    database_name=database_name,
                    table_name=table_name,
                    query_preview=query_text[:200]
                )

        # Salvar log tradicional (mantido para compatibilidade)
        log_data = {
            'instance': self.instance_name,
            'db_type': self.db_type.value,
            'database': database_name,
            'schema': schema,
            'table': table_name,
            'sanitized_query': sanitized_result['sanitized_query'],
            'placeholders': sanitized_result['placeholders'],
            'metrics': query_info,
            'ddl': ddl,
            'indexes': indexes,
            'llm_analysis': analysis,
            'timestamp': datetime.now().isoformat()
        }

        self.logger.save_analysis_log(
            server=self.instance_name,
            database=database_name,
            schema=schema,
            table=table_name,
            log_content=json.dumps(log_data, indent=2, ensure_ascii=False)
        )

    def get_stats(self) -> Dict[str, Any]:
        """
        Retorna estatísticas do monitor.

        Returns:
            Dicionário com estatísticas acumuladas.
        """
        return {
            'instance': self.instance_name,
            'db_type': self.db_type.value,
            **self.stats
        }
