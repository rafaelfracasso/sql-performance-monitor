"""
Monitor individual para um banco de dados específico.
"""
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from ..core.database_types import DatabaseType
from ..factories.database_factory import DatabaseFactory
from ..utils.query_sanitizer import QuerySanitizer
from ..utils.llm_analyzer import LLMAnalyzer
from ..utils.llm_providers import get_model_pricing
from ..utils.currency import CurrencyConverter
from ..utils.logger import PerformanceLogger
from ..utils.performance_checker import PerformanceChecker
from ..utils.metrics_store import MetricsStore
from ..utils.metadata_cache import MetadataCache
from ..utils.baseline_calculator import BaselineCalculator


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
        self.sanitizer = sanitizer
        self.llm_analyzer = llm_analyzer
        self.logger = logger
        self.performance_checker = performance_checker
        self.metrics_store = metrics_store

        # Cache de metadados (DDL, indexes)
        self.metadata_cache = MetadataCache(metrics_store) if metrics_store else None

        # Baseline calculator para deteccao de desvios
        self.baseline_calculator = BaselineCalculator(metrics_store) if metrics_store else None

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
            'queries_stored': 0,
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

        # Resetar contador de análises LLM para este ciclo
        if self.llm_analyzer:
            self.llm_analyzer.reset_cycle_count()

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
                    # Armazenar TODAS as queries (sem gate de threshold)
                    query_hash = self._store_query(query_info)
                    if query_hash:
                        cycle_stats['queries_stored'] += 1

                        # Analise LLM apenas para queries que excedem thresholds
                        if self.performance_checker.should_analyze_query(query_info):
                            self._analyze_query(query_info, query_hash)
                            cycle_stats['queries_analyzed'] += 1
                            self.stats['queries_analyzed'] += 1

                except Exception as e:
                    print(f"[{self.instance_name}] Erro ao processar query: {e}")
                    cycle_stats['errors'] += 1
                    self.stats['errors'] += 1

            # Coletar e armazenar wait stats
            self._collect_and_store_wait_stats()

            # Metricas adicionais PostgreSQL
            self._collect_health_metrics()

            print(f"[{self.instance_name}] Ciclo concluido: {cycle_stats['queries_found']} encontradas, "
                  f"{cycle_stats['queries_stored']} armazenadas, {cycle_stats['queries_analyzed']} analisadas")

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

    def _store_query(self, query_info: Dict) -> Optional[str]:
        """
        Armazena query no metrics_store. Executa para TODAS as queries,
        independente de thresholds.

        Args:
            query_info: Informacoes da query coletada

        Returns:
            query_hash se armazenada com sucesso, None caso contrario
        """
        query_text = query_info.get('query_text', '') or query_info.get('full_query_text', '')

        # Ignorar DDL e statements de controle que nao sao workload de aplicacao.
        # Remove comentarios SQL (-- linha e /* bloco */) antes de extrair a
        # primeira palavra, para nao falhar em textos como "-- comment\nCREATE ...".
        _DDL_KEYWORDS = {
            'CREATE', 'ALTER', 'DROP', 'TRUNCATE',
            'GRANT', 'REVOKE', 'DENY',
            'BACKUP', 'RESTORE',
        }
        if query_text and query_text.strip():
            import re as _re
            _no_comments = _re.sub(r'/\*.*?\*/', '', query_text, flags=_re.DOTALL)
            _no_comments = _re.sub(r'--[^\n]*', '', _no_comments)
            _first_word = _no_comments.strip().upper().split()[0] if _no_comments.strip() else ''
        else:
            _first_word = ''
        if _first_word in _DDL_KEYWORDS:
            return None

        if self.config.get('security', {}).get('sanitize_queries', True):
            sanitized_result = self.sanitizer.sanitize_query(query_text)
        else:
            sanitized_result = {
                'sanitized_query': query_text,
                'placeholders': '',
                'placeholder_map': {}
            }

        tables = self.extractor.extract_table_info_from_query(query_text)
        if tables:
            table_info = tables[0]
            schema = table_info.get('schema', 'dbo')
            table_name = table_info.get('table', 'unknown')
        else:
            schema = 'unknown'
            table_name = 'unknown'

        database_name = query_info.get('database_name', 'current')

        query_hash = None
        if self.metrics_store:
            query_hash = self.metrics_store.generate_query_hash(
                sanitized_query=sanitized_result['sanitized_query'],
                database=database_name,
                schema=schema,
                table=table_name
            )

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
                metrics=metrics_data,
                login_name=query_info.get('login_name'),
                host_name=query_info.get('host_name'),
                program_name=query_info.get('program_name'),
                client_interface_name=query_info.get('client_interface_name'),
                session_id=query_info.get('session_id')
            )

        # Verificar desvio de baseline
        if self.baseline_calculator and query_hash:
            try:
                deviations = self.baseline_calculator.check_deviation(
                    query_hash=query_hash,
                    instance_name=self.instance_name,
                    current_metrics=metrics_data
                )
                for dev in deviations:
                    self.metrics_store.add_performance_alert(
                        instance_name=self.instance_name,
                        query_hash=query_hash,
                        alert_type='baseline_deviation',
                        severity='high' if dev['multiplier'] >= 3.0 else 'medium',
                        threshold_value=dev['baseline_avg'],
                        actual_value=dev['current_value'],
                        database_name=database_name,
                        table_name=table_name,
                        query_preview=f"[{dev['metric']}] {dev['multiplier']}x baseline ({dev['baseline_samples']} amostras)"
                    )
            except Exception as e:
                print(f"[{self.instance_name}] Erro ao verificar baseline: {e}")

        # Guardar dados extraidos no query_info para _analyze_query reutilizar
        query_info['_extracted'] = {
            'sanitized_result': sanitized_result,
            'schema': schema,
            'table_name': table_name,
            'database_name': database_name
        }

        return query_hash

    def _analyze_query(self, query_info: Dict, query_hash: str):
        """
        Analisa query via LLM, gera alertas de performance.
        So executa para queries que excedem thresholds.

        Args:
            query_info: Informacoes da query coletada
            query_hash: Hash da query ja calculado
        """
        extracted = query_info.get('_extracted', {})
        sanitized_result = extracted.get('sanitized_result', {})
        schema = extracted.get('schema', 'unknown')
        table_name = extracted.get('table_name', 'unknown')
        database_name = extracted.get('database_name', 'current')
        query_text = query_info.get('query_text', '') or query_info.get('full_query_text', '')

        # Verificar cache de analise
        if self.metrics_store and self.metrics_store.is_query_analyzed_and_valid(query_hash):
            print(f"[{self.instance_name}] Query ja analisada (cache): {query_hash[:8]}...")
            return

        try:
            if self.metadata_cache:
                cache_key_ddl = f"{database_name}.{schema}.{table_name}:ddl"
                cache_key_idx = f"{database_name}.{schema}.{table_name}:indexes"
                ddl = self.metadata_cache.get_or_fetch(
                    cache_key_ddl,
                    lambda: self.extractor.get_table_ddl(database_name, schema, table_name)
                )
                indexes = self.metadata_cache.get_or_fetch(
                    cache_key_idx,
                    lambda: self.extractor.get_table_indexes(database_name, schema, table_name)
                )
            else:
                ddl = self.extractor.get_table_ddl(database_name, schema, table_name)
                indexes = self.extractor.get_table_indexes(database_name, schema, table_name)
        except Exception as e:
            print(f"[{self.instance_name}] Erro ao extrair metadados: {e}")
            ddl = "N/A"
            indexes = []

        # Formatar placeholders para o LLM
        placeholders = sanitized_result.get('placeholders', '')
        if isinstance(placeholders, dict):
            placeholder_map = "VALORES ORIGINAIS:\n"
            for placeholder, original in placeholders.items():
                placeholder_map += f"  {placeholder} = {original}\n"
        else:
            placeholder_map = str(placeholders)

        # Formatar indices existentes
        indexes_formatted = "INDICES EXISTENTES:\n"
        if indexes:
            if isinstance(indexes, str):
                for idx in indexes.split('\n'):
                    if idx.strip():
                        indexes_formatted += f"  {idx}\n"
            elif isinstance(indexes, list):
                for idx in indexes:
                    if str(idx).strip():
                        indexes_formatted += f"  {idx}\n"
            else:
                indexes_formatted += f"  {indexes}\n"
        else:
            indexes_formatted += "  Nenhum indice encontrado\n"

        # Analisar via LLM
        analysis_start = datetime.now()
        try:
            analysis = self.llm_analyzer.analyze_query_performance(
                sanitized_query=sanitized_result.get('sanitized_query', ''),
                placeholder_map=placeholder_map,
                table_ddl=ddl or "DDL nao disponivel",
                existing_indexes=indexes_formatted,
                metrics=query_info,
                db_type=self.db_type.value
            )
        except Exception as e:
            print(f"[{self.instance_name}] Erro na analise LLM: {e}")
            analysis = "Analise LLM nao disponivel"

        analysis_duration_ms = (datetime.now() - analysis_start).total_seconds() * 1000

        if isinstance(analysis, dict):
            analysis_text = analysis.get('explanation', 'Analise nao disponivel')
            recommendations = analysis.get('suggestions', '')
            priority = analysis.get('priority', 'MEDIUM')
            severity_map = {'LOW': 'low', 'MEDIUM': 'medium', 'HIGH': 'high', 'CRITICAL': 'critical'}
            severity = severity_map.get(priority, 'medium')
            tokens_used = analysis.get('tokens_used', 0)
            prompt_tokens = analysis.get('prompt_tokens', 0)
            completion_tokens = analysis.get('completion_tokens', 0)
        else:
            analysis_text = str(analysis)
            recommendations = ""
            severity = "medium"
            tokens_used = 0
            prompt_tokens = 0
            completion_tokens = 0

        model_used = analysis.get('model_used', self.llm_analyzer.model_name) if isinstance(analysis, dict) else self.llm_analyzer.model_name
        input_price, output_price = get_model_pricing(model_used)
        if input_price == 0.0 and output_price == 0.0:
            # Modelo não está na tabela: usa preços manuais do config como fallback
            llm_config = self.config.get('llm', {})
            input_price = llm_config.get('input_price_per_million', 0.0)
            output_price = llm_config.get('output_price_per_million', 0.0)

        estimated_cost_usd = (
            (prompt_tokens / 1_000_000 * input_price) +
            (completion_tokens / 1_000_000 * output_price)
        )

        ptax_rate = CurrencyConverter.get_usd_brl_rate()
        estimated_cost_brl = estimated_cost_usd * ptax_rate

        print(f"[{self.instance_name}] Analise concluida: {tokens_used} tokens, Cost: ${estimated_cost_usd:.6f} (R$ {estimated_cost_brl:.6f})")

        if self.metrics_store:
            self.metrics_store.add_llm_analysis(
                query_hash=query_hash,
                instance_name=self.instance_name,
                database_name=database_name,
                schema_name=schema,
                table_name=table_name,
                analysis_text=analysis_text,
                recommendations=recommendations,
                severity=severity,
                ttl_hours=self.config.get('query_cache', {}).get('ttl_hours', 24),
                model_used=self.llm_analyzer.model_name,
                analysis_duration_ms=analysis_duration_ms,
                tokens_used=tokens_used,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                estimated_cost_usd=estimated_cost_usd,
                estimated_cost_brl=estimated_cost_brl
            )

        # Registrar alertas de performance
        self._check_and_create_alerts(query_info, query_hash, query_text, database_name, table_name)

        self.logger.write_simple_log(
            f"Query analisada: {self.instance_name}.{database_name}.{schema}.{table_name} - "
            f"Hash: {query_hash[:8]}... - CPU: {query_info.get('cpu_time_ms', 0)}ms"
        )

    def _check_and_create_alerts(self, query_info: Dict, query_hash: str,
                                  query_text: str, database_name: str, table_name: str):
        """Verifica thresholds e cria alertas de performance."""
        if not self.metrics_store:
            return

        thresholds = self.metrics_store.get_thresholds(self.db_type.value) if self.metrics_store else {}

        alert_checks = [
            ('cpu_threshold', 'cpu_time_ms', thresholds.get('cpu_time_ms', 1000)),
            ('logical_reads_threshold', 'logical_reads', thresholds.get('logical_reads', 50000)),
            ('physical_reads_threshold', 'physical_reads', thresholds.get('physical_reads', 10000)),
            ('writes_threshold', 'writes', thresholds.get('writes', 5000)),
            ('memory_grant_threshold', 'memory_mb', thresholds.get('memory_mb', 500)),
            ('row_count_threshold', 'row_count', thresholds.get('row_count', 100000)),
        ]

        for alert_type, metric_key, threshold in alert_checks:
            value = query_info.get(metric_key, 0) or 0
            if value > threshold:
                self.metrics_store.add_performance_alert(
                    instance_name=self.instance_name,
                    query_hash=query_hash,
                    alert_type=alert_type,
                    severity='high' if value > threshold * 2 else 'medium',
                    threshold_value=threshold,
                    actual_value=value,
                    database_name=database_name,
                    table_name=table_name,
                    query_preview=query_text[:200]
                )

        # Duration threshold (execution_time_seconds -> ms)
        duration = query_info.get('duration_ms', 0) or 0
        duration_threshold_ms = thresholds.get('execution_time_seconds', 30) * 1000
        if duration > duration_threshold_ms:
            self.metrics_store.add_performance_alert(
                instance_name=self.instance_name,
                query_hash=query_hash,
                alert_type='duration_threshold',
                severity='high' if duration > duration_threshold_ms * 2 else 'medium',
                threshold_value=duration_threshold_ms,
                actual_value=duration,
                database_name=database_name,
                table_name=table_name,
                query_preview=query_text[:200]
            )

        # Blocking
        blocking_session_id = query_info.get('blocking_session_id')
        if blocking_session_id and blocking_session_id > 0:
            wait_time_ms = query_info.get('wait_time_ms', 0)
            blocking_info = None
            try:
                if hasattr(self.collector, 'get_blocking_sessions'):
                    blocking_sessions = self.collector.get_blocking_sessions()
                    session_id = query_info.get('session_id')
                    for block in blocking_sessions:
                        if block.get('blocked_session_id') == session_id:
                            blocking_info = json.dumps({
                                'blocking_session_id': int(block.get('blocking_session_id') or 0),
                                'blocking_query': str(block.get('blocking_query') or '')[:500],
                                'blocking_host': str(block.get('blocking_host') or ''),
                                'blocking_program': str(block.get('blocking_program') or ''),
                                'blocking_login': str(block.get('blocking_login') or ''),
                                'wait_time_seconds': float(block.get('wait_time_seconds') or 0),
                                'blocker_query_hash': None,
                            })
                            break
            except Exception as e:
                self.logger.log_error(
                    f"[{self.instance_name}] Erro ao buscar blocking info: {e}"
                )

            self.metrics_store.add_performance_alert(
                instance_name=self.instance_name,
                query_hash=query_hash,
                alert_type='blocking',
                severity='critical' if wait_time_ms > 30000 else 'high',
                threshold_value=0,
                actual_value=blocking_session_id,
                database_name=database_name,
                table_name=table_name,
                query_preview=query_text[:200],
                extra_info=blocking_info
            )

        # Wait time
        wait_time_ms = query_info.get('wait_time_ms', 0) or 0
        wait_threshold = thresholds.get('wait_time_ms', 5000)
        if wait_time_ms > wait_threshold:
            wait_type = query_info.get('wait_type', 'UNKNOWN')
            self.metrics_store.add_performance_alert(
                instance_name=self.instance_name,
                query_hash=query_hash,
                alert_type='wait_time_threshold',
                severity='high' if wait_time_ms > wait_threshold * 2 else 'medium',
                threshold_value=wait_threshold,
                actual_value=wait_time_ms,
                database_name=database_name,
                table_name=table_name,
                query_preview=f"[{wait_type}] {query_text[:180]}"
            )

    def _collect_and_store_wait_stats(self):
        """Coleta wait stats de todos os collectors e armazena no metrics_store."""
        if not self.collector or not self.metrics_store:
            return

        if not hasattr(self.collector, 'get_wait_statistics'):
            return

        try:
            waits = self.collector.get_wait_statistics()
            if not waits:
                return

            self.metrics_store.add_wait_stats_snapshot(
                instance_name=self.instance_name,
                db_type=self.db_type.value,
                wait_stats=waits
            )
            print(f"[{self.instance_name}] Wait Stats: {len(waits)} tipos armazenados")
        except Exception as e:
            print(f"[{self.instance_name}] Erro ao coletar/armazenar wait stats: {e}")

    def _collect_health_metrics(self):
        """Coleta metricas de saude dos collectors e gera alertas."""
        if not self.collector or not self.metrics_store:
            return

        # Vacuum stats
        if hasattr(self.collector, 'get_vacuum_stats'):
            try:
                vacuum_stats = self.collector.get_vacuum_stats()
                for stat in vacuum_stats:
                    if stat.get('dead_tuple_ratio', 0) > 20:
                        self.metrics_store.add_performance_alert(
                            instance_name=self.instance_name,
                            query_hash='vacuum_health',
                            alert_type='vacuum_needed',
                            severity='high' if stat['dead_tuple_ratio'] > 50 else 'medium',
                            threshold_value=20.0,
                            actual_value=stat['dead_tuple_ratio'],
                            database_name=self.instance_name,
                            table_name=f"{stat.get('schema', 'public')}.{stat.get('table', 'unknown')}",
                            query_preview=f"dead_tuples: {stat.get('n_dead_tup', 0)}, ratio: {stat['dead_tuple_ratio']}%"
                        )
                    for alert_msg in stat.get('alerts', []):
                        if 'nunca executou' in alert_msg or 'nao roda ha' in alert_msg:
                            self.metrics_store.add_performance_alert(
                                instance_name=self.instance_name,
                                query_hash='vacuum_health',
                                alert_type='autovacuum_stale',
                                severity='medium',
                                threshold_value=7.0,
                                actual_value=0.0,
                                database_name=self.instance_name,
                                table_name=f"{stat.get('schema', 'public')}.{stat.get('table', 'unknown')}",
                                query_preview=alert_msg
                            )
                if vacuum_stats:
                    print(f"[{self.instance_name}] Vacuum: {len(vacuum_stats)} tabelas com dead tuples elevados")
            except Exception as e:
                print(f"[{self.instance_name}] Erro ao coletar vacuum stats: {e}")

        # Connection stats
        if hasattr(self.collector, 'get_connection_stats'):
            try:
                conn_stats = self.collector.get_connection_stats()
                if conn_stats:
                    usage = conn_stats.get('usage_percent', 0)
                    if usage > 80:
                        self.metrics_store.add_performance_alert(
                            instance_name=self.instance_name,
                            query_hash='connection_health',
                            alert_type='connection_saturation',
                            severity='critical' if usage > 95 else 'high',
                            threshold_value=80.0,
                            actual_value=usage,
                            database_name=self.instance_name,
                            query_preview=f"Conexoes: {conn_stats.get('total', 0)}/{conn_stats.get('max_connections', 0)} ({usage}%)"
                        )
                    idle_in_tx = conn_stats.get('idle_in_transaction', 0)
                    if idle_in_tx > 10:
                        self.metrics_store.add_performance_alert(
                            instance_name=self.instance_name,
                            query_hash='connection_health',
                            alert_type='idle_in_transaction',
                            severity='high' if idle_in_tx > 20 else 'medium',
                            threshold_value=10.0,
                            actual_value=float(idle_in_tx),
                            database_name=self.instance_name,
                            query_preview=f"{idle_in_tx} conexoes idle in transaction"
                        )
                    print(f"[{self.instance_name}] Connections: {conn_stats.get('total', 0)}/{conn_stats.get('max_connections', 0)} ({usage}%)")
            except Exception as e:
                print(f"[{self.instance_name}] Erro ao coletar connection stats: {e}")

        # Replication status
        if hasattr(self.collector, 'get_replication_status'):
            try:
                rep_status = self.collector.get_replication_status()
                for rep in rep_status:
                    lag = rep.get('replay_lag_seconds', 0)
                    if lag > 60:
                        self.metrics_store.add_performance_alert(
                            instance_name=self.instance_name,
                            query_hash='replication_health',
                            alert_type='replication_lag',
                            severity='critical' if lag > 300 else 'high',
                            threshold_value=60.0,
                            actual_value=lag,
                            database_name=self.instance_name,
                            query_preview=f"Replica {rep.get('client_addr', 'N/A')}: lag {lag:.1f}s"
                        )
                if rep_status:
                    print(f"[{self.instance_name}] Replication: {len(rep_status)} replicas monitoradas")
            except Exception as e:
                print(f"[{self.instance_name}] Erro ao coletar replication status: {e}")

        # Blocking sessions
        if hasattr(self.collector, 'get_blocking_sessions'):
            try:
                blocks = self.collector.get_blocking_sessions()
                for block in blocks:
                    wait_seconds = float(block.get('wait_time_seconds', 0))
                    severity = 'critical' if wait_seconds > 30 else 'high'
                    blocked_sid = block.get('blocked_session_id')
                    blocking_sid = block.get('blocking_session_id')
                    db_name = block.get('database_name', self.instance_name)
                    query_preview = block.get('blocked_query', '')[:200]
                    actual_value = float(blocking_sid or 0)

                    # Buscar query_hashes das duas sessoes na mesma query
                    blocked_hash = None
                    blocker_hash = None
                    try:
                        conn = self.metrics_store._get_connection()
                        if blocked_sid:
                            row = conn.execute("""
                                SELECT query_hash FROM queries_collected
                                WHERE session_id = ? AND instance_name = ?
                                ORDER BY collected_at DESC LIMIT 1
                            """, [blocked_sid, self.instance_name]).fetchone()
                            blocked_hash = row[0] if row else None
                        if blocking_sid:
                            row = conn.execute("""
                                SELECT query_hash FROM queries_collected
                                WHERE session_id = ? AND instance_name = ?
                                ORDER BY collected_at DESC LIMIT 1
                            """, [blocking_sid, self.instance_name]).fetchone()
                            blocker_hash = row[0] if row else None
                    except Exception:
                        pass

                    blocking_info_dict = {
                        'blocking_session_id': int(blocking_sid or 0),
                        'blocking_query': block.get('blocking_query', '')[:500],
                        'blocking_host': str(block.get('blocking_host', '')),
                        'blocking_program': str(block.get('blocking_program', '')),
                        'blocking_login': str(block.get('blocking_login', '')),
                        'wait_time_seconds': wait_seconds,
                        'blocker_query_hash': blocker_hash,
                    }
                    blocking_info_str = json.dumps(blocking_info_dict)

                    # Alerta generico (visao global de blocking)
                    self.metrics_store.add_performance_alert(
                        instance_name=self.instance_name,
                        query_hash='blocking_sessions',
                        alert_type='blocking',
                        severity=severity,
                        threshold_value=0.0,
                        actual_value=actual_value,
                        database_name=db_name,
                        query_preview=query_preview,
                        extra_info=blocking_info_str
                    )

                    # Alerta linkado ao query_hash real da sessao bloqueada
                    if blocked_hash:
                        self.metrics_store.add_performance_alert(
                            instance_name=self.instance_name,
                            query_hash=blocked_hash,
                            alert_type='blocking',
                            severity=severity,
                            threshold_value=0.0,
                            actual_value=actual_value,
                            database_name=db_name,
                            query_preview=query_preview,
                            extra_info=blocking_info_str
                        )

                if blocks:
                    print(f"[{self.instance_name}] Blocking: {len(blocks)} sessoes bloqueadas detectadas")
            except Exception as e:
                print(f"[{self.instance_name}] Erro ao coletar blocking sessions: {e}")

        # Database sizes (informativo, sem alerta)
        if hasattr(self.collector, 'get_database_sizes'):
            try:
                sizes = self.collector.get_database_sizes()
                if sizes:
                    total_size = sum(s.get('size_bytes', 0) for s in sizes)
                    print(f"[{self.instance_name}] Database sizes: {len(sizes)} databases, total {total_size / (1024*1024*1024):.2f} GB")
            except Exception as e:
                print(f"[{self.instance_name}] Erro ao coletar database sizes: {e}")

        # Wait statistics agora coletadas via _collect_and_store_wait_stats()

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
