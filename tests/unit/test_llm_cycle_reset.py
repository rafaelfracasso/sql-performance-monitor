
import pytest
from unittest.mock import MagicMock, patch
from sql_monitor.utils.llm_analyzer import LLMAnalyzer
from sql_monitor.monitor.database_monitor import DatabaseMonitor
from sql_monitor.core.database_types import DatabaseType

class TestLLMCycleReset:
    """Testes para garantir o reset do contador de ciclo do LLM."""

    def test_reset_cycle_count_method(self):
        """Verifica se o método reset_cycle_count zera o contador."""
        config = {'llm': {'rate_limit': {'max_requests_per_cycle': 10}}}
        
        # Mock do genai.Client para evitar chamadas reais
        with patch('google.genai.Client'):
            analyzer = LLMAnalyzer(config)
            
            # Simular uso
            analyzer.cycle_request_count = 5
            assert analyzer.cycle_request_count == 5
            
            # Resetar
            analyzer.reset_cycle_count()
            assert analyzer.cycle_request_count == 0

    def test_monitor_resets_cycle_count(self):
        """Verifica se DatabaseMonitor.run_cycle chama reset_cycle_count."""
        # Mocks
        mock_cache = MagicMock()
        mock_sanitizer = MagicMock()
        mock_logger = MagicMock()
        mock_checker = MagicMock()
        mock_store = MagicMock()
        
        # Mock do LLM Analyzer
        mock_llm = MagicMock()
        
        # Mock do Collector para retornar lista vazia e evitar processamento real
        mock_collector = MagicMock()
        mock_collector.collect_active_queries.return_value = []
        mock_collector.collect_recent_expensive_queries.return_value = []
        mock_collector.get_table_scan_queries.return_value = []

        monitor = DatabaseMonitor(
            db_type=DatabaseType.SQLSERVER,
            instance_name="TEST_DB",
            credentials={},
            config={'timeouts': {}},
            cache=mock_cache,
            sanitizer=mock_sanitizer,
            llm_analyzer=mock_llm,
            logger=mock_logger,
            performance_checker=mock_checker,
            metrics_store=mock_store
        )
        
        # Injetar collector mockado (já que initialize não é chamado)
        monitor.collector = mock_collector
        
        # Executar ciclo
        monitor.run_cycle()
        
        # Verificar se reset_cycle_count foi chamado
        mock_llm.reset_cycle_count.assert_called_once()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
