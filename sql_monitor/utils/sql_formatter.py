"""
Utilitário para formatação de queries SQL com indentação e quebras de linha.
"""
import sqlparse


def format_sql(query: str, compact: bool = False) -> str:
    """
    Formata query SQL com indentação e quebras de linha.

    Args:
        query: Query SQL para formatar.
        compact: Se True, usa formatação mais compacta (para Teams/mensagens).

    Returns:
        Query formatada com indentação adequada.

    Examples:
        >>> format_sql("SELECT * FROM Users WHERE id=1")
        SELECT *
        FROM Users
        WHERE id = 1

        >>> format_sql("SELECT * FROM Users WHERE id=1", compact=True)
        SELECT * FROM Users WHERE id = 1
    """
    if not query or not query.strip():
        return query

    try:
        # Formata usando sqlparse
        formatted = sqlparse.format(
            query,
            reindent=True,
            keyword_case='upper',
            identifier_case='lower',
            indent_width=2,
            wrap_after=80 if compact else 120,
            comma_first=False,
            strip_comments=False,
            use_space_around_operators=True
        )

        return formatted.strip()

    except Exception as e:
        # Se falhar ao formatar, retorna query original
        print(f"⚠️  Erro ao formatar SQL: {e}")
        return query


def format_sql_for_log(query: str) -> str:
    """
    Formata query SQL para arquivo de log (formato completo e legível).

    Args:
        query: Query SQL para formatar.

    Returns:
        Query formatada com indentação completa.
    """
    return format_sql(query, compact=False)


def format_sql_for_teams(query: str, max_length: int = 500) -> str:
    """
    Formata query SQL para mensagens do Teams.

    Args:
        query: Query SQL para formatar.
        max_length: Comprimento máximo da query formatada.

    Returns:
        Query formatada e truncada se necessário.
    """
    formatted = format_sql(query, compact=True)

    # Trunca se muito longa
    if len(formatted) > max_length:
        formatted = formatted[:max_length-3] + "..."

    return formatted
