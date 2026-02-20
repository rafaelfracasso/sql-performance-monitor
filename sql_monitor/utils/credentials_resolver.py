"""
Sistema de resolução de credenciais com suporte a variáveis de ambiente.

Este módulo permite armazenar senhas de forma segura usando variáveis de ambiente
em vez de plaintext em arquivos de configuração.

Exemplo de uso em databases.json:
{
    "credentials": {
        "password": "${SQL_SERVER_PROD_PASSWORD}"  // Lê de variável de ambiente
    }
}
"""
import os
import re
from typing import Any, Dict
import logging


logger = logging.getLogger(__name__)


class CredentialsResolver:
    """
    Resolve credenciais de variáveis de ambiente.

    Suporta a sintaxe ${VAR_NAME} para referenciar variáveis de ambiente.
    """

    # Padrão para detectar referências a variáveis de ambiente
    ENV_VAR_PATTERN = re.compile(r'\$\{([A-Za-z0-9_]+)\}')

    @classmethod
    def resolve(cls, value: Any) -> Any:
        """
        Resolve um valor que pode conter referência a variável de ambiente.

        Args:
            value: Valor a resolver (string, dict, list, ou outro tipo)

        Returns:
            Valor resolvido com variáveis de ambiente substituídas.

        Raises:
            ValueError: Se variável de ambiente referenciada não existir.
        """
        if isinstance(value, str):
            return cls._resolve_string(value)
        elif isinstance(value, dict):
            return cls._resolve_dict(value)
        elif isinstance(value, list):
            return cls._resolve_list(value)
        else:
            return value

    @classmethod
    def _resolve_string(cls, value: str) -> str:
        """
        Resolve uma string que pode conter ${VAR_NAME}.

        Args:
            value: String a resolver.

        Returns:
            String com variáveis de ambiente substituídas.

        Raises:
            ValueError: Se variável de ambiente não existir.
        """
        match = cls.ENV_VAR_PATTERN.search(value)

        if not match:
            # Não tem referência a variável de ambiente
            return value

        # Se é EXATAMENTE ${VAR_NAME} (sem texto adicional), retornar valor direto
        if cls.ENV_VAR_PATTERN.fullmatch(value):
            var_name = match.group(1)
            env_value = os.environ.get(var_name)

            if env_value is None:
                raise ValueError(
                    f"Variável de ambiente '{var_name}' não encontrada. "
                    f"Configure-a antes de executar o monitor.\n"
                    f"Exemplo: export {var_name}='sua_senha_aqui'"
                )

            logger.debug(f"Resolvido ${{{var_name}}} de variável de ambiente")
            return env_value

        # Se tem texto adicional, substituir todas as ocorrências
        def replacer(match):
            var_name = match.group(1)
            env_value = os.environ.get(var_name)

            if env_value is None:
                raise ValueError(
                    f"Variável de ambiente '{var_name}' não encontrada. "
                    f"Configure-a antes de executar o monitor."
                )

            return env_value

        return cls.ENV_VAR_PATTERN.sub(replacer, value)

    @classmethod
    def _resolve_dict(cls, value: Dict) -> Dict:
        """
        Resolve recursivamente um dicionário.

        Args:
            value: Dicionário a resolver.

        Returns:
            Dicionário com valores resolvidos.
        """
        return {k: cls.resolve(v) for k, v in value.items()}

    @classmethod
    def _resolve_list(cls, value: list) -> list:
        """
        Resolve recursivamente uma lista.

        Args:
            value: Lista a resolver.

        Returns:
            Lista com valores resolvidos.
        """
        return [cls.resolve(item) for item in value]

    @classmethod
    def resolve_credentials(cls, credentials: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve credenciais de banco de dados.

        Args:
            credentials: Dicionário de credenciais (pode conter ${VAR_NAME}).

        Returns:
            Credenciais com variáveis de ambiente resolvidas.

        Raises:
            ValueError: Se alguma variável de ambiente necessária não existir.
        """
        return cls._resolve_dict(credentials)

    @classmethod
    def validate_env_vars(cls, credentials: Dict[str, Any]) -> list:
        """
        Valida se todas as variáveis de ambiente necessárias existem.

        Args:
            credentials: Dicionário de credenciais a validar.

        Returns:
            Lista de variáveis de ambiente faltando (vazia se todas existem).
        """
        missing_vars = []

        def check_value(value):
            if isinstance(value, str):
                match = cls.ENV_VAR_PATTERN.search(value)
                if match:
                    var_name = match.group(1)
                    if os.environ.get(var_name) is None:
                        missing_vars.append(var_name)
            elif isinstance(value, dict):
                for v in value.values():
                    check_value(v)
            elif isinstance(value, list):
                for item in value:
                    check_value(item)

        check_value(credentials)
        return missing_vars


def check_plaintext_passwords(credentials: Dict[str, Any]) -> bool:
    """
    Verifica se há senhas em plaintext (não usa variáveis de ambiente).

    Args:
        credentials: Dicionário de credenciais.

    Returns:
        True se encontrou senha em plaintext, False caso contrário.

    Warning:
        Senhas em plaintext são uma vulnerabilidade de segurança!
    """
    password = credentials.get('password', '')

    if isinstance(password, str) and password:
        # Se NÃO começa com ${, é plaintext
        if not CredentialsResolver.ENV_VAR_PATTERN.fullmatch(password):
            logger.warning(
                "⚠️  AVISO DE SEGURANÇA: Senha em plaintext detectada! "
                "Use variáveis de ambiente: password: '${SQL_PASSWORD}'"
            )
            return True

    return False
