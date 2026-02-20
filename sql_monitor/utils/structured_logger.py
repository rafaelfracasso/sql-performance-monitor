"""
Sistema de logging estruturado para o monitor de performance.

Suporta múltiplos formatos (texto e JSON) e níveis configuráveis.
"""
import logging
import json
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path


class JSONFormatter(logging.Formatter):
    """
    Formatter que emite logs em formato JSON estruturado.

    Útil para integração com sistemas de análise de logs (ELK, Splunk, etc).
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Formata log record como JSON.

        Args:
            record: Log record do Python logging.

        Returns:
            String JSON com campos estruturados.
        """
        log_data = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }

        # Adiciona extra fields se existirem
        if hasattr(record, 'extra_fields'):
            log_data.update(record.extra_fields)

        # Adiciona exception info se existir
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)

        return json.dumps(log_data, ensure_ascii=False)


class ColoredFormatter(logging.Formatter):
    """
    Formatter que adiciona cores ANSI aos logs (para console).
    """

    # Códigos ANSI de cores
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }

    def format(self, record: logging.LogRecord) -> str:
        """
        Formata log com cores.

        Args:
            record: Log record do Python logging.

        Returns:
            String formatada com cores ANSI.
        """
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']

        # Formato: [TIMESTAMP] [LEVEL] [module.function] message
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        level = f"{color}{record.levelname:<8}{reset}"
        location = f"{record.module}.{record.funcName}"
        message = record.getMessage()

        formatted = f"[{timestamp}] {level} [{location:30}] {message}"

        # Adiciona exception se existir
        if record.exc_info:
            formatted += '\n' + self.formatException(record.exc_info)

        return formatted


def setup_logging(
    log_level: str = 'INFO',
    log_format: str = 'colored',  # 'colored', 'json', 'simple'
    log_file: Optional[str] = None,
    enable_console: bool = True
) -> logging.Logger:
    """
    Configura sistema de logging global.

    Args:
        log_level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Formato de saída ('colored', 'json', 'simple')
        log_file: Caminho para arquivo de log (opcional)
        enable_console: Se True, loga também no console

    Returns:
        Logger raiz configurado.
    """
    # Obter logger raiz
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Limpar handlers existentes
    root_logger.handlers.clear()

    # Escolher formatter
    if log_format == 'json':
        formatter = JSONFormatter()
    elif log_format == 'colored':
        formatter = ColoredFormatter()
    else:  # simple
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    # Handler de console
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    # Handler de arquivo (se especificado)
    if log_file:
        # Garantir que diretório existe
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, encoding='utf-8')

        # Arquivo sempre usa JSON para facilitar parsing
        file_handler.setFormatter(JSONFormatter())
        root_logger.addHandler(file_handler)

    return root_logger


def get_logger(name: str) -> logging.Logger:
    """
    Obtém logger com nome específico.

    Args:
        name: Nome do logger (geralmente __name__ do módulo).

    Returns:
        Logger configurado.
    """
    return logging.getLogger(name)


class StructuredLogger:
    """
    Wrapper que adiciona contexto estruturado aos logs.

    Exemplo:
        logger = StructuredLogger("my_module")
        logger.info("Query executada", extra={
            'query_hash': 'abc123',
            'duration_ms': 150,
            'database': 'production'
        })
    """

    def __init__(self, name: str):
        """
        Inicializa logger estruturado.

        Args:
            name: Nome do logger.
        """
        self.logger = logging.getLogger(name)
        self.context: Dict[str, Any] = {}

    def set_context(self, **kwargs):
        """
        Define contexto que será incluído em todos os logs.

        Args:
            **kwargs: Campos de contexto.
        """
        self.context.update(kwargs)

    def clear_context(self):
        """Remove todo o contexto."""
        self.context.clear()

    def _log(self, level: int, message: str, extra: Optional[Dict] = None, **kwargs):
        """
        Log interno com contexto estruturado.

        Args:
            level: Nível de log (logging.INFO, etc).
            message: Mensagem de log.
            extra: Campos extras.
            **kwargs: Campos adicionais.
        """
        # Mesclar contexto global + extra + kwargs
        extra_fields = {**self.context}
        if extra:
            extra_fields.update(extra)
        extra_fields.update(kwargs)

        # Criar LogRecord com extra fields
        self.logger.log(level, message, extra={'extra_fields': extra_fields})

    def debug(self, message: str, extra: Optional[Dict] = None, **kwargs):
        """Log DEBUG."""
        self._log(logging.DEBUG, message, extra, **kwargs)

    def info(self, message: str, extra: Optional[Dict] = None, **kwargs):
        """Log INFO."""
        self._log(logging.INFO, message, extra, **kwargs)

    def warning(self, message: str, extra: Optional[Dict] = None, **kwargs):
        """Log WARNING."""
        self._log(logging.WARNING, message, extra, **kwargs)

    def error(self, message: str, extra: Optional[Dict] = None, exc_info: bool = False, **kwargs):
        """
        Log ERROR.

        Args:
            message: Mensagem de erro.
            extra: Campos extras.
            exc_info: Se True, inclui traceback da exception.
            **kwargs: Campos adicionais.
        """
        self._log(logging.ERROR, message, extra, **kwargs)
        if exc_info:
            self.logger.exception(message)

    def critical(self, message: str, extra: Optional[Dict] = None, exc_info: bool = False, **kwargs):
        """
        Log CRITICAL.

        Args:
            message: Mensagem crítica.
            extra: Campos extras.
            exc_info: Se True, inclui traceback da exception.
            **kwargs: Campos adicionais.
        """
        self._log(logging.CRITICAL, message, extra, **kwargs)
        if exc_info:
            self.logger.exception(message)


# Helper para criar loggers rapidamente
def create_logger(name: str, structured: bool = True):
    """
    Factory function para criar loggers.

    Args:
        name: Nome do logger.
        structured: Se True, retorna StructuredLogger, senão logging.Logger padrão.

    Returns:
        Logger configurado.
    """
    if structured:
        return StructuredLogger(name)
    else:
        return logging.getLogger(name)
