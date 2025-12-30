"""
Multi-Database Monitor - Orquestra monitoramento de múltiplos bancos simultaneamente.
"""
import json
import time
import threading
from typing import Dict, List, Any
from pathlib import Path
from datetime import datetime
from ..core.database_types import DatabaseType
from .database_monitor import DatabaseMonitor
from ..utils.query_cache import QueryCache
from ..utils.query_sanitizer import QuerySanitizer
from ..utils.llm_analyzer import LLMAnalyzer
from ..utils.logger import PerformanceLogger
from ..utils.performance_checker import PerformanceChecker
from ..utils.credentials_resolver import CredentialsResolver, check_plaintext_passwords
from ..utils.structured_logger import create_logger
from ..utils.metrics_store import MetricsStore


class MultiDatabaseMonitor:
    """
    Monitor multi-database que gerencia múltiplas instâncias de bancos de dados.

    Estratégia de execução:
    - Uma thread POR TIPO de banco (não por instância)
    - Processamento SEQUENCIAL dentro de cada tipo
    - Cache individual por tipo (thread-safe por design)

    Exemplo:
    - Thread 1: SQL Server -> processa SQL1 → SQL2 → SQL3 sequencialmente
    - Thread 2: PostgreSQL -> processa PG1 → PG2 sequencialmente
    - Thread 3: HANA -> processa HANA1 sequencialmente
    """

    def __init__(self, config_path: str = "config.json", db_config_path: str = "config/databases.json"):
        """
        Inicializa multi-database monitor.

        Args:
            config_path: Caminho para config.json (configurações gerais)
            db_config_path: Caminho para databases.json (configuração de databases)
        """
        self.config_path = config_path
        self.db_config_path = db_config_path

        # Carregar configurações
        self.config = self._load_config(config_path)
        self.db_config = self._load_db_config(db_config_path)

        # Componentes compartilhados
        self.sanitizer = QuerySanitizer(self.config)
        self.llm_analyzer = LLMAnalyzer(self.config)
        self.logger = PerformanceLogger(self.config)
        self.performance_checker = PerformanceChecker(self.config)

        # MetricsStore DuckDB para observabilidade
        self.metrics_store = MetricsStore(
            db_path=self.config.get('metrics_store', {}).get('db_path', 'logs/metrics.duckdb'),
            enable_compression=self.config.get('metrics_store', {}).get('enable_compression', True)
        )

        # Caches individuais por tipo de banco
        self.caches = {
            DatabaseType.SQLSERVER: QueryCache(
                cache_file=self.config.get('query_cache', {}).get('cache_file', 'logs/query_cache.json').replace('.json', '_sqlserver.json'),
                config=self.config
            ),
            DatabaseType.POSTGRESQL: QueryCache(
                cache_file=self.config.get('query_cache', {}).get('cache_file', 'logs/query_cache.json').replace('.json', '_postgresql.json'),
                config=self.config
            ),
            DatabaseType.HANA: QueryCache(
                cache_file=self.config.get('query_cache', {}).get('cache_file', 'logs/query_cache.json').replace('.json', '_hana.json'),
                config=self.config
            )
        }

        # Monitors agrupados por tipo
        self.monitors_by_type: Dict[DatabaseType, List[DatabaseMonitor]] = {
            DatabaseType.SQLSERVER: [],
            DatabaseType.POSTGRESQL: [],
            DatabaseType.HANA: []
        }

        # Controle de threads
        self.threads: List[threading.Thread] = []
        self.running = False
        self.shutdown_event = threading.Event()  # Event para graceful shutdown
        self.stats = {
            'cycles_completed': 0,
            'total_queries_found': 0,
            'total_queries_analyzed': 0,
            'total_cache_hits': 0,
            'total_errors': 0,
            'start_time': None,
            'monitors_by_type': {}
        }

        # Logger estruturado
        self.structured_logger = create_logger(__name__)

    def _load_config(self, config_path: str) -> Dict:
        """Carrega config.json."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"✗ Erro ao carregar {config_path}: {e}")
            return {}

    def _load_db_config(self, db_config_path: str) -> Dict:
        """Carrega databases.json."""
        try:
            with open(db_config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"✗ Erro ao carregar {db_config_path}: {e}")
            return {"databases": []}

    def initialize(self) -> bool:
        """
        Inicializa todos os monitors baseado na configuração.

        Returns:
            True se inicializado com sucesso.
        """
        self.structured_logger.info("Inicializando Multi-Database Monitor")

        databases = self.db_config.get('databases', [])

        if not databases:
            self.structured_logger.error("Nenhum database configurado em databases.json")
            return False

        # Agrupar databases por tipo
        for db_entry in databases:
            if not db_entry.get('enabled', True):
                self.structured_logger.info("Pulando database desabilitado",
                                           database=db_entry.get('name', 'unknown'))
                continue

            try:
                # Obter tipo de banco
                db_type_str = db_entry.get('type', '').upper()
                db_type = DatabaseType[db_type_str]

                # Resolver credenciais (substitui ${VAR} por valores de env)
                raw_credentials = db_entry.get('credentials', {})

                # Verificar se há senhas em plaintext (gerar warning)
                check_plaintext_passwords(raw_credentials)

                # Resolver variáveis de ambiente
                try:
                    resolved_credentials = CredentialsResolver.resolve_credentials(raw_credentials)
                except ValueError as e:
                    self.structured_logger.error("Erro ao resolver credenciais",
                                                database=db_entry.get('name'),
                                                error=str(e))
                    continue

                # Criar monitor para esta instância
                monitor = DatabaseMonitor(
                    db_type=db_type,
                    instance_name=db_entry.get('name'),
                    credentials=resolved_credentials,
                    config=self.config,
                    cache=self.caches[db_type],
                    sanitizer=self.sanitizer,
                    llm_analyzer=self.llm_analyzer,
                    logger=self.logger,
                    performance_checker=self.performance_checker,
                    metrics_store=self.metrics_store
                )

                # Inicializar monitor
                if monitor.initialize():
                    self.monitors_by_type[db_type].append(monitor)
                    self.structured_logger.info("Monitor criado com sucesso",
                                               database=db_entry.get('name'),
                                               db_type=db_type.value)
                else:
                    self.structured_logger.error("Falha ao inicializar monitor",
                                                database=db_entry.get('name'))

            except KeyError:
                self.structured_logger.error("Tipo de banco inválido",
                                            database=db_entry.get('name'),
                                            type=db_entry.get('type'))
            except Exception as e:
                self.structured_logger.error("Erro ao criar monitor",
                                            database=db_entry.get('name'),
                                            error=str(e))

        # Resumo
        print("\n" + "=" * 80)
        print("RESUMO DE INICIALIZAÇÃO")
        print("=" * 80)

        total_monitors = 0
        for db_type, monitors in self.monitors_by_type.items():
            count = len(monitors)
            total_monitors += count
            self.stats['monitors_by_type'][db_type.value] = count
            if count > 0:
                print(f"  {db_type.value}: {count} instância(s)")

        print(f"\nTotal: {total_monitors} monitor(s) ativo(s)")
        print("=" * 80 + "\n")

        return total_monitors > 0

    def _monitor_worker(self, db_type: DatabaseType):
        """
        Worker que processa sequencialmente todos os monitors de um tipo.

        Args:
            db_type: Tipo de banco que esta thread processa
        """
        monitors = self.monitors_by_type[db_type]

        if not monitors:
            return

        print(f"[Thread {db_type.value}] Iniciada com {len(monitors)} monitor(s)")

        while not self.shutdown_event.is_set():
            try:
                # Processar cada monitor sequencialmente
                for monitor in monitors:
                    # Verificar shutdown antes de processar cada monitor
                    if self.shutdown_event.is_set():
                        break

                    try:
                        cycle_stats = monitor.run_cycle()

                        # Atualizar estatísticas globais
                        self.stats['total_queries_found'] += cycle_stats.get('queries_found', 0)
                        self.stats['total_queries_analyzed'] += cycle_stats.get('queries_analyzed', 0)
                        self.stats['total_cache_hits'] += cycle_stats.get('cache_hits', 0)
                        self.stats['total_errors'] += cycle_stats.get('errors', 0)

                    except Exception as e:
                        print(f"[Thread {db_type.value}] Erro no ciclo de {monitor.instance_name}: {e}")
                        self.stats['total_errors'] += 1

                # Aguardar intervalo configurado (ou até shutdown)
                if not self.shutdown_event.is_set():
                    interval = self.config.get('monitor', {}).get('interval_seconds', 60)
                    # Usar wait() em vez de sleep() para permitir interrupção rápida
                    self.shutdown_event.wait(timeout=interval)

                self.stats['cycles_completed'] += 1

            except Exception as e:
                print(f"[Thread {db_type.value}] Erro fatal: {e}")
                self.stats['total_errors'] += 1
                break

        print(f"[Thread {db_type.value}] Finalizada gracefully")

    def start(self):
        """Inicia monitoramento em background (threads)."""
        if self.running:
            print("⚠️  Monitor já está em execução")
            return

        self.running = True
        self.shutdown_event.clear()  # Resetar evento de shutdown
        self.stats['start_time'] = datetime.now().isoformat()

        print("\n" + "=" * 80)
        print("INICIANDO MONITORAMENTO MULTI-DATABASE")
        print("=" * 80)

        # Criar uma thread por tipo de banco que tem monitors
        for db_type, monitors in self.monitors_by_type.items():
            if monitors:
                thread = threading.Thread(
                    target=self._monitor_worker,
                    args=(db_type,),
                    name=f"Monitor-{db_type.value}",
                    daemon=False  # ✅ Thread normal (não daemon) para graceful shutdown
                )
                thread.start()
                self.threads.append(thread)
                print(f"✓ Thread iniciada: {db_type.value}")

        print(f"\nTotal: {len(self.threads)} thread(s) ativa(s)")
        print("=" * 80 + "\n")

    def stop(self):
        """Para monitoramento e aguarda threads finalizarem gracefully."""
        if not self.running:
            return

        print("\n" + "=" * 80)
        print("PARANDO MULTI-DATABASE MONITOR")
        print("=" * 80)

        self.logger.info("Iniciando graceful shutdown",
                        active_threads=len(self.threads),
                        shutdown_timeout=self.config.get('timeouts', {}).get('thread_shutdown', 90))

        self.running = False
        self.shutdown_event.set()  # Sinalizar shutdown para todas as threads

        print("Aguardando threads finalizarem...")

        # Aguardar threads finalizarem com timeout maior (permite completar análise LLM em andamento)
        shutdown_timeout = self.config.get('timeouts', {}).get('thread_shutdown', 90)
        for thread in self.threads:
            print(f"  Aguardando thread {thread.name}...")
            thread.join(timeout=shutdown_timeout)
            if thread.is_alive():
                self.logger.warning("Thread não finalizou no timeout",
                                   thread_name=thread.name,
                                   timeout_seconds=shutdown_timeout)
                print(f"⚠️  Thread {thread.name} não finalizou em {shutdown_timeout}s")
                print(f"     (Possível análise LLM em andamento será perdida)")
            else:
                self.logger.debug("Thread finalizada com sucesso",
                                 thread_name=thread.name)
                print(f"  ✓ Thread {thread.name} finalizada")

        # Desconectar todos os monitors
        print("\nDesconectando monitors...")
        for db_type, monitors in self.monitors_by_type.items():
            for monitor in monitors:
                try:
                    monitor.shutdown()
                except Exception as e:
                    print(f"⚠️  Erro ao desconectar {monitor.instance_name}: {e}")

        # Salvar caches
        print("\nSalvando caches...")
        for db_type, cache in self.caches.items():
            try:
                cache.save_cache()
                print(f"  ✓ Cache {db_type.value} salvo")
            except Exception as e:
                print(f"  ⚠️  Erro ao salvar cache {db_type.value}: {e}")

        print("\n✓ Multi-Database Monitor parado gracefully")
        print("=" * 80 + "\n")

    def get_stats(self) -> Dict[str, Any]:
        """
        Retorna estatísticas agregadas.

        Returns:
            Dicionário com estatísticas de todos os monitors.
        """
        stats = {
            **self.stats,
            'monitors': {}
        }

        # Coletar stats de cada monitor
        for db_type, monitors in self.monitors_by_type.items():
            stats['monitors'][db_type.value] = []
            for monitor in monitors:
                stats['monitors'][db_type.value].append(monitor.get_stats())

        return stats

    def print_stats(self):
        """Imprime estatísticas formatadas."""
        stats = self.get_stats()

        print("\n" + "=" * 80)
        print("ESTATÍSTICAS DO MULTI-DATABASE MONITOR")
        print("=" * 80)

        print(f"\nInício: {stats.get('start_time', 'N/A')}")
        print(f"Ciclos completados: {stats.get('cycles_completed', 0)}")
        print(f"Queries encontradas: {stats.get('total_queries_found', 0)}")
        print(f"Queries analisadas: {stats.get('total_queries_analyzed', 0)}")
        print(f"Cache hits: {stats.get('total_cache_hits', 0)}")
        print(f"Erros: {stats.get('total_errors', 0)}")

        print("\n" + "-" * 80)
        print("MONITORS POR TIPO")
        print("-" * 80)

        for db_type_str, count in stats.get('monitors_by_type', {}).items():
            print(f"  {db_type_str}: {count} monitor(s)")

        print("\n" + "-" * 80)
        print("DETALHES POR MONITOR")
        print("-" * 80)

        for db_type_str, monitors_stats in stats.get('monitors', {}).items():
            if monitors_stats:
                print(f"\n{db_type_str}:")
                for monitor_stat in monitors_stats:
                    print(f"  - {monitor_stat.get('instance', 'unknown')}")
                    print(f"      Queries coletadas: {monitor_stat.get('queries_collected', 0)}")
                    print(f"      Queries analisadas: {monitor_stat.get('queries_analyzed', 0)}")
                    print(f"      Cache hits: {monitor_stat.get('cache_hits', 0)}")
                    print(f"      Erros: {monitor_stat.get('errors', 0)}")

        print("\n" + "=" * 80 + "\n")

    def run_once(self) -> Dict[str, Any]:
        """
        Executa um único ciclo de monitoramento (síncrono, útil para testes).

        Returns:
            Estatísticas do ciclo.
        """
        print("\n" + "=" * 80)
        print("EXECUTANDO CICLO ÚNICO")
        print("=" * 80)

        cycle_stats = {
            'timestamp': datetime.now().isoformat(),
            'results_by_type': {}
        }

        for db_type, monitors in self.monitors_by_type.items():
            if not monitors:
                continue

            cycle_stats['results_by_type'][db_type.value] = []

            for monitor in monitors:
                try:
                    result = monitor.run_cycle()
                    cycle_stats['results_by_type'][db_type.value].append(result)
                except Exception as e:
                    print(f"✗ Erro no ciclo de {monitor.instance_name}: {e}")

        print("=" * 80 + "\n")
        return cycle_stats
