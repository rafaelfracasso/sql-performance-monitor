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

        # Carregar configurações iniciais do JSON (para bootstrap)
        config_json = self._load_config_json(config_path)

        # Inicializar MetricsStore (precisa do db_path do JSON)
        self.metrics_store = MetricsStore(
            db_path=config_json.get('metrics_store', {}).get('db_path', 'logs/metrics.duckdb'),
            enable_compression=config_json.get('metrics_store', {}).get('enable_compression', True)
        )

        # Inicializar configurações padrão no DuckDB
        self.metrics_store.init_config_defaults()

        # Agora carregar configurações definitivas do DuckDB (sobrescreve JSON)
        self.config = self._load_config(config_path)
        self.db_config = self._load_db_config(db_config_path)

        # Componentes compartilhados
        self.sanitizer = QuerySanitizer(self.config)
        self.llm_analyzer = LLMAnalyzer(self.config, metrics_store=self.metrics_store)

        # Obter diretório de logs da configuração
        log_file = self.config.get('logging', {}).get('log_file', 'logs/monitor.log')
        log_dir = str(Path(log_file).parent)
        self.logger = PerformanceLogger(log_dir)

        # Logger estruturado (antes dos checkers que podem precisar logar erros)
        self.structured_logger = create_logger(__name__)

        # Performance checkers por tipo de banco (carregam do DuckDB)
        self.performance_checkers = {}
        for db_type in ['hana', 'sqlserver', 'postgresql']:
            try:
                checker = PerformanceChecker(
                    metrics_store=self.metrics_store,
                    db_type=db_type
                )
                self.performance_checkers[db_type] = checker
            except Exception as e:
                self.structured_logger.warning(
                    f"Erro ao criar PerformanceChecker para {db_type}: {e}. "
                    f"Execute: python scripts/migrate_config_to_duckdb.py"
                )

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
        self._stats_lock = threading.Lock()
        self.stats = {
            'cycles_completed': 0,
            'total_queries_found': 0,
            'total_queries_analyzed': 0,
            'total_cache_hits': 0,
            'total_errors': 0,
            'start_time': None,
            'monitors_by_type': {}
        }

    def _load_config_json(self, config_path: str) -> Dict:
        """Carrega config.json simples (para bootstrap)."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"✗ Erro ao carregar {config_path}: {e}")
            return {}

    def _load_config(self, config_path: str) -> Dict:
        """
        Carrega configurações do DuckDB (preferencial) ou config.json (fallback).

        Prioridade:
        1. Configurações do DuckDB (se metrics_store estiver disponível)
        2. config.json (fallback para compatibilidade)
        """
        config = {}

        # Tentar carregar do DuckDB primeiro
        if hasattr(self, 'metrics_store') and self.metrics_store:
            try:
                config = self.metrics_store.load_config_from_db()
                if config:
                    print("✓ Configurações carregadas do DuckDB")
                    return config
            except Exception as e:
                print(f"⚠️  Erro ao carregar do DuckDB: {e}")
                print("   Usando config.json como fallback")

        # Fallback: carregar do JSON
        return self._load_config_json(config_path)

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

                # Obter performance checker específico do dbtype
                db_type_key = db_type.value.lower()  # 'sqlserver', 'postgresql', 'hana'
                perf_checker = self.performance_checkers.get(db_type_key)

                if perf_checker is None:
                    self.structured_logger.error(
                        f"Nenhum PerformanceChecker disponível para {db_type_key}. "
                        f"Execute: python scripts/migrate_config_to_duckdb.py",
                        database=db_entry.get('name')
                    )
                    continue

                # Criar monitor para esta instância
                monitor = DatabaseMonitor(
                    db_type=db_type,
                    instance_name=db_entry.get('name'),
                    credentials=resolved_credentials,
                    config=self.config,
                    sanitizer=self.sanitizer,
                    llm_analyzer=self.llm_analyzer,
                    logger=self.logger,
                    performance_checker=perf_checker,
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

                        # Atualizar estatísticas globais (protegido por lock)
                        with self._stats_lock:
                            self.stats['total_queries_found'] += cycle_stats.get('queries_found', 0)
                            self.stats['total_queries_analyzed'] += cycle_stats.get('queries_analyzed', 0)
                            self.stats['total_cache_hits'] += cycle_stats.get('cache_hits', 0)
                            self.stats['total_errors'] += cycle_stats.get('errors', 0)

                    except Exception as e:
                        print(f"[Thread {db_type.value}] Erro no ciclo de {monitor.instance_name}: {e}")
                        with self._stats_lock:
                            self.stats['total_errors'] += 1

                # Aguardar intervalo configurado (ou até shutdown)
                if not self.shutdown_event.is_set():
                    interval = self.config.get('monitor', {}).get('interval_seconds', 60)
                    # Usar wait() em vez de sleep() para permitir interrupção rápida
                    self.shutdown_event.wait(timeout=interval)

                with self._stats_lock:
                    self.stats['cycles_completed'] += 1

            except Exception as e:
                print(f"[Thread {db_type.value}] Erro fatal: {e}")
                with self._stats_lock:
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

        try:
            self.structured_logger.info("Iniciando graceful shutdown",
                                       active_threads=len(self.threads),
                                       shutdown_timeout=self.config.get('timeouts', {}).get('thread_shutdown', 90))
        except:
            print("Iniciando graceful shutdown...")

        self.running = False
        self.shutdown_event.set()  # Sinalizar shutdown para todas as threads

        print("Aguardando threads finalizarem...")

        # Aguardar threads finalizarem com timeout maior (permite completar análise LLM em andamento)
        shutdown_timeout = self.config.get('timeouts', {}).get('thread_shutdown', 90)
        for thread in self.threads:
            print(f"  Aguardando thread {thread.name}...")
            thread.join(timeout=shutdown_timeout)
            if thread.is_alive():
                self.structured_logger.warning("Thread não finalizou no timeout",
                                             thread_name=thread.name,
                                             timeout_seconds=shutdown_timeout)
                print(f"⚠️  Thread {thread.name} não finalizou em {shutdown_timeout}s")
                print(f"     (Possível análise LLM em andamento será perdida)")
            else:
                self.structured_logger.debug("Thread finalizada com sucesso",
                                           thread_name=thread.name)
                print(f"✓ Thread {thread.name.replace('Monitor-', '')} finalizada gracefully")

        # Desconectar todos os monitors
        print("\nDesconectando monitors...")
        for db_type, monitors in self.monitors_by_type.items():
            for monitor in monitors:
                try:
                    monitor.shutdown()
                except Exception as e:
                    print(f"⚠️  Erro ao desconectar {monitor.instance_name}: {e}")

        # Cache agora é gerenciado por MetadataCache dentro de cada DatabaseMonitor
        # Não precisa salvar explicitamente (é em memória com TTL)

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
