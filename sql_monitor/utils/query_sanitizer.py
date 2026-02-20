"""
Sanitização de queries SQL para remover dados sensíveis.
Substitui valores literais por placeholders tipados para compliance de segurança.
"""
import re
import sqlparse
from sqlparse import tokens as T
from typing import Dict, List, Tuple


class QuerySanitizer:
    """Sanitiza queries SQL substituindo valores por placeholders tipados."""

    def __init__(self, config: Dict = None, placeholder_prefix: str = "@p", show_examples: bool = True):
        """
        Inicializa o sanitizador.

        Args:
            config: Dicionário de configuração (opcional).
            placeholder_prefix: Prefixo para placeholders (usado se config não fornecido).
            show_examples: Se True, gera exemplos genéricos (usado se config não fornecido).
        """
        if config:
            security_config = config.get('security', {})
            self.placeholder_prefix = security_config.get('placeholder_prefix', placeholder_prefix)
            self.show_examples = security_config.get('show_example_values', show_examples)
        else:
            self.placeholder_prefix = placeholder_prefix
            self.show_examples = show_examples
            
        self.placeholder_counter = 0
        self.placeholder_map: Dict[str, Dict[str, str]] = {}

    def sanitize_query(self, query: str) -> Dict[str, str]:
        """
        Sanitiza query e retorna formato esperado pelo monitor.
        
        Args:
            query: Query SQL original.
            
        Returns:
            Dict com 'sanitized_query' e 'placeholders'.
        """
        sanitized, p_map = self.sanitize(query)
        formatted_map = self.format_placeholder_map()
        
        return {
            'sanitized_query': sanitized,
            'placeholders': formatted_map,
            'placeholder_map': p_map
        }

    def sanitize(self, query: str) -> Tuple[str, Dict[str, Dict[str, str]]]:
        """
        Sanitiza query substituindo valores literais por placeholders.

        Args:
            query: Query SQL original.

        Returns:
            Tupla (query_sanitizada, mapa_placeholders).
        """
        self.placeholder_counter = 0
        self.placeholder_map = {}

        # Parse SQL
        parsed = sqlparse.parse(query)
        if not parsed:
            return query, {}

        sanitized_query = self._process_tokens(parsed[0])

        return sanitized_query, self.placeholder_map

    def _process_tokens(self, token) -> str:
        """
        Processa tokens recursivamente substituindo valores.

        Args:
            token: Token SQL a processar.

        Returns:
            String SQL processada.
        """
        if token.is_group:
            # Processa grupo de tokens recursivamente
            result = []
            for sub_token in token.tokens:
                result.append(self._process_tokens(sub_token))
            return ''.join(result)

        # Substitui strings (valores entre aspas)
        if token.ttype in (T.String.Single, T.String.Symbol):
            return self._replace_string_value(token)

        # Substitui números
        elif token.ttype in (T.Number.Integer, T.Number.Float, T.Number.Hexadecimal):
            return self._replace_numeric_value(token)

        # Mantém outros tokens como estão
        else:
            return str(token)

    def _replace_string_value(self, token) -> str:
        """
        Substitui valor string por placeholder tipado.

        Args:
            token: Token contendo string.

        Returns:
            Placeholder tipado.
        """
        original_value = str(token)
        self.placeholder_counter += 1

        # Detecta tipo de string
        if original_value.startswith("N'") or original_value.startswith('N"'):
            sql_type = "NVARCHAR"
        else:
            sql_type = "VARCHAR"

        placeholder_name = f"{self.placeholder_prefix}{self.placeholder_counter}_{sql_type}"

        # Gera exemplo genérico
        example = self._generate_string_example(original_value)

        self.placeholder_map[placeholder_name] = {
            "type": sql_type,
            "example": example if self.show_examples else "***"
        }

        return placeholder_name

    def _replace_numeric_value(self, token) -> str:
        """
        Substitui valor numérico por placeholder tipado.

        Args:
            token: Token contendo número.

        Returns:
            Placeholder tipado.
        """
        original_value = str(token)
        self.placeholder_counter += 1

        # Detecta tipo numérico
        if '.' in original_value or 'e' in original_value.lower():
            sql_type = "DECIMAL"
        else:
            sql_type = "INT"

        placeholder_name = f"{self.placeholder_prefix}{self.placeholder_counter}_{sql_type}"

        # Gera exemplo genérico
        example = self._generate_numeric_example(original_value, sql_type)

        self.placeholder_map[placeholder_name] = {
            "type": sql_type,
            "example": example if self.show_examples else "***"
        }

        return placeholder_name

    def _generate_string_example(self, original: str) -> str:
        """
        Gera exemplo genérico para string (sem expor valor real).

        Args:
            original: Valor original.

        Returns:
            Exemplo genérico descritivo.
        """
        # Remove aspas
        value = original.strip("'\"N")

        length = len(value)

        # Detecta padrões comuns
        if re.match(r'^\d{11}$', value):
            return "ex: CPF (11 dígitos)"
        elif re.match(r'^\d{14}$', value):
            return "ex: CNPJ (14 dígitos)"
        elif '@' in value:
            return "ex: email@example.com"
        elif re.match(r'^\d{4}-\d{2}-\d{2}', value):
            return "ex: data YYYY-MM-DD"
        elif length > 50:
            return f"ex: texto longo ({length} caracteres)"
        else:
            return f"ex: string ({length} caracteres)"

    def _generate_numeric_example(self, original: str, sql_type: str) -> str:
        """
        Gera exemplo genérico para número.

        Args:
            original: Valor original.
            sql_type: Tipo SQL detectado.

        Returns:
            Exemplo genérico.
        """
        try:
            if sql_type == "DECIMAL":
                value = float(original)
                if value > 1000000:
                    return "ex: valor grande (milhões)"
                elif value > 1000:
                    return "ex: valor médio (milhares)"
                else:
                    return "ex: valor decimal"
            else:
                value = int(original)
                if value > 1000000:
                    return "ex: ID grande (milhões)"
                elif value > 1000:
                    return "ex: valor inteiro (milhares)"
                else:
                    return "ex: valor inteiro pequeno"
        except (ValueError, TypeError, OverflowError):
            # Falha ao converter para número - retorna descrição genérica
            return f"ex: número {sql_type}"

    def format_placeholder_map(self) -> str:
        """
        Formata mapa de placeholders para exibição em log.

        Returns:
            String formatada com placeholders e exemplos.
        """
        if not self.placeholder_map:
            return "Nenhum valor substituído."

        lines = ["Placeholders:"]
        for placeholder, info in self.placeholder_map.items():
            lines.append(f"  - {placeholder}: {info['type']} ({info['example']})")

        return "\n".join(lines)


def sanitize_query(query: str, config: dict = None) -> Tuple[str, str]:
    """
    Função helper para sanitizar query.

    Args:
        query: Query SQL original.
        config: Configurações opcionais.

    Returns:
        Tupla (query_sanitizada, mapa_formatado).
    """
    if config is None:
        config = {}

    sanitizer = QuerySanitizer(
        placeholder_prefix=config.get('placeholder_prefix', '@p'),
        show_examples=config.get('show_example_values', True)
    )

    sanitized_query, placeholder_map = sanitizer.sanitize(query)
    formatted_map = sanitizer.format_placeholder_map()

    return sanitized_query, formatted_map
