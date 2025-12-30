"""
Sistema de logging para salvar análises de queries em arquivos organizados.
Formato: server-database-schema-table-timestamp.log
"""
import os
from datetime import datetime
from typing import Dict, Optional
from .sql_formatter import format_sql_for_log


class PerformanceLogger:
    """Gerencia logs de análise de performance."""

    def __init__(self, log_directory: str = "logs"):
        """
        Inicializa logger.

        Args:
            log_directory: Diretório onde logs serão salvos.
        """
        self.log_directory = log_directory

        # Cria diretório se não existir
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
            print(f"✓ Diretório de logs criado: {log_directory}")

    def generate_log_filename(
        self,
        server: str,
        database: str,
        schema: str,
        table: str
    ) -> str:
        """
        Gera nome do arquivo de log.

        Args:
            server: Nome do servidor.
            database: Nome do database.
            schema: Nome do schema.
            table: Nome da tabela.

        Returns:
            Nome do arquivo formatado.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Sanitiza nomes removendo caracteres inválidos
        server = self._sanitize_filename(server)
        database = self._sanitize_filename(database)
        schema = self._sanitize_filename(schema)
        table = self._sanitize_filename(table)

        filename = f"{server}-{database}-{schema}-{table}-{timestamp}.log"
        return os.path.join(self.log_directory, filename)

    def _sanitize_filename(self, name: str) -> str:
        """
        Remove caracteres inválidos de nome de arquivo.

        Args:
            name: Nome original.

        Returns:
            Nome sanitizado.
        """
        # Remove caracteres especiais, mantém apenas alfanuméricos, underscore e hífen
        import re
        return re.sub(r'[^\w\-.]', '_', name)

    def write_analysis_log(
        self,
        server: str,
        database: str,
        schema: str,
        table: str,
        query_info: Dict,
        sanitized_query: str,
        placeholder_map: str,
        table_ddl: Optional[str],
        existing_indexes: str,
        performance_summary: str,
        llm_analysis: Dict
    ) -> str:
        """
        Escreve log completo de análise.

        Args:
            server: Nome do servidor.
            database: Nome do database.
            schema: Nome do schema.
            table: Nome da tabela.
            query_info: Informações da query original.
            sanitized_query: Query parametrizada.
            placeholder_map: Mapa de placeholders formatado.
            table_ddl: DDL da tabela.
            existing_indexes: Índices existentes formatados.
            performance_summary: Resumo de problemas de performance.
            llm_analysis: Análise da LLM.

        Returns:
            Caminho do arquivo de log criado.
        """
        log_filename = self.generate_log_filename(server, database, schema, table)

        with open(log_filename, 'w', encoding='utf-8') as f:
            # Cabeçalho
            f.write("=" * 80 + "\n")
            f.write("ANÁLISE DE PERFORMANCE - SQL SERVER QUERY\n")
            f.write("=" * 80 + "\n")
            f.write(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Servidor: {server}\n")
            f.write(f"Database: {database}\n")
            f.write(f"Schema: {schema}\n")
            f.write(f"Tabela: {table}\n")
            f.write("=" * 80 + "\n\n")

            # Informações de Origem/Execução
            if query_info.get('host_name') or query_info.get('program_name'):
                f.write("=" * 80 + "\n")
                f.write("ORIGEM DA EXECUÇÃO (Quem executou esta query?)\n")
                f.write("=" * 80 + "\n")
                f.write(f"Hostname/IP Cliente: {query_info.get('host_name', 'N/A')}\n")
                f.write(f"Aplicação/Driver: {query_info.get('program_name', 'N/A')}\n")
                f.write(f"Usuário/Login: {query_info.get('login_name', 'N/A')}\n")
                f.write(f"Interface: {query_info.get('client_interface_name', 'N/A')}\n")
                f.write(f"Session ID: {query_info.get('session_id', 'N/A')}\n")
                f.write("=" * 80 + "\n\n")

            # Query Parametrizada (SEGURA) - FORMATADA
            f.write("=" * 80 + "\n")
            f.write("QUERY PARAMETRIZADA (Dados sensíveis substituídos por placeholders)\n")
            f.write("=" * 80 + "\n")
            f.write(format_sql_for_log(sanitized_query) + "\n\n")

            # Placeholders
            f.write(placeholder_map + "\n\n")

            # Métricas de Performance
            f.write("=" * 80 + "\n")
            f.write(performance_summary + "\n")
            f.write("=" * 80 + "\n\n")

            # DDL da Tabela
            if table_ddl:
                f.write("=" * 80 + "\n")
                f.write("DDL DA TABELA\n")
                f.write("=" * 80 + "\n")
                f.write(table_ddl + "\n\n")

            # Índices Existentes
            f.write("=" * 80 + "\n")
            f.write(existing_indexes + "\n")
            f.write("=" * 80 + "\n\n")

            # Análise da LLM
            f.write("=" * 80 + "\n")
            f.write("ANÁLISE INTELIGENTE (Google Gemini)\n")
            f.write("=" * 80 + "\n\n")

            f.write("EXPLICAÇÃO DO PROBLEMA:\n")
            f.write("-" * 80 + "\n")
            f.write(llm_analysis.get('explanation', 'N/A') + "\n\n")

            f.write("PRIORIDADE: " + llm_analysis.get('priority', 'MÉDIO') + "\n\n")

            f.write("SUGESTÕES DE ÍNDICES:\n")
            f.write("-" * 80 + "\n")
            f.write(llm_analysis.get('suggestions', 'N/A') + "\n\n")

            f.write("JUSTIFICATIVA TÉCNICA:\n")
            f.write("-" * 80 + "\n")
            f.write(llm_analysis.get('justification', 'N/A') + "\n\n")

            # Rodapé
            f.write("=" * 80 + "\n")
            f.write("FIM DA ANÁLISE\n")
            f.write("=" * 80 + "\n")

        return log_filename

    def write_simple_log(self, message: str, filename: str = "monitor.log") -> None:
        """
        Escreve log simples de eventos do monitor.

        Args:
            message: Mensagem a ser logada.
            filename: Nome do arquivo de log.
        """
        log_path = os.path.join(self.log_directory, filename)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] {message}\n")

    def log_monitoring_cycle(self, queries_found: int, queries_analyzed: int) -> None:
        """
        Loga informações de um ciclo de monitoramento.

        Args:
            queries_found: Número de queries encontradas.
            queries_analyzed: Número de queries analisadas.
        """
        message = f"Ciclo de monitoramento: {queries_found} queries encontradas, {queries_analyzed} analisadas"
        self.write_simple_log(message)
        print(f"ℹ️  {message}")

    def log_error(self, error_message: str) -> None:
        """
        Loga mensagem de erro.

        Args:
            error_message: Mensagem de erro.
        """
        message = f"ERRO: {error_message}"
        self.write_simple_log(message, "errors.log")
        print(f"✗ {error_message}")
