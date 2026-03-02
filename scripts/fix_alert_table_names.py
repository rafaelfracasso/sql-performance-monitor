"""
Script pontual: reprocessa alertas históricos com table_name ausente, desconhecido
ou capturado errado (ex: 'PIMSMCPRD.dbo' em vez do nome real da tabela).
Usa o extractor corrigido com suporte a nomes de 3 partes (db.schema.table).
"""
import re
import sys
import duckdb
from pathlib import Path

DUCKDB_PATH = Path(__file__).parent.parent / "logs" / "metrics.duckdb"


# ── Extractor com suporte a 1, 2 e 3 partes ────────────────────────────────────

def extract_tables_sqlserver(query_text: str):
    skip = {'SELECT', 'WHERE', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS', 'SET', 'TOP'}
    THREE_PART = r'\[?(\w+)\]?\.\[?(\w+)\]?\.\[?(\w+)\]?'
    TWO_PART   = r'\[?(\w+)\]?\.\[?(\w+)\]?'
    ONE_PART   = r'\[?(\w+)\]?'

    def _parse_ref(text, keyword_pat):
        results = []
        full_pat = rf'\b{keyword_pat}\s+(?:{THREE_PART}|{TWO_PART}|{ONE_PART})'
        for m in re.finditer(full_pat, text, re.IGNORECASE):
            g = m.groups()
            if g[0] and g[1] and g[2]:
                schema, table = g[1], g[2]
            elif g[3] and g[4]:
                schema, table = g[3], g[4]
            elif g[5]:
                schema, table = 'dbo', g[5]
            else:
                continue
            if table.upper() not in skip:
                results.append({'schema': schema, 'table': table})
        return results

    def _dedupe(lst):
        seen, out = set(), []
        for t in lst:
            key = f"{t['schema']}.{t['table']}"
            if key not in seen:
                seen.add(key)
                out.append(t)
        return out

    normalized = ' '.join(query_text.split())
    if re.match(r'(?i)\s*UPDATE\b', normalized):
        from_tables = _parse_ref(normalized, 'FROM')
        if from_tables:
            return _dedupe(from_tables)
        return _dedupe(_parse_ref(normalized, 'UPDATE'))

    results = []
    for kw in (r'FROM', r'JOIN', r'INTO', r'DELETE\s+FROM'):
        results.extend(_parse_ref(query_text, kw))
    return _dedupe(results)


def extract_tables_generic(query_text: str, default_schema: str = 'public'):
    skip = {'SELECT', 'WHERE', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS', 'SET', 'TOP', 'ONLY', 'PUBLIC'}
    W = r'[A-Za-z_][A-Za-z0-9_]*'
    THREE = rf'"?({W})"?\."?({W})"?\."?({W})"?'
    TWO   = rf'"?({W})"?\."?({W})"?'
    ONE   = rf'"?({W})"?'

    def _parse_ref(text, keyword_pat):
        results = []
        full_pat = rf'\b{keyword_pat}\s+(?:{THREE}|{TWO}|{ONE})'
        for m in re.finditer(full_pat, text, re.IGNORECASE):
            g = m.groups()
            if g[0] and g[1] and g[2]:
                schema, table = g[1], g[2]
            elif g[3] and g[4]:
                schema, table = g[3], g[4]
            elif g[5]:
                schema, table = default_schema, g[5]
            else:
                continue
            if table.upper() not in skip:
                results.append({'schema': schema, 'table': table})
        return results

    def _dedupe(lst):
        seen, out = set(), []
        for t in lst:
            key = f"{t['schema']}.{t['table']}"
            if key not in seen:
                seen.add(key)
                out.append(t)
        return out

    normalized = ' '.join(query_text.split())
    if re.match(r'(?i)\s*UPDATE\b', normalized):
        from_tables = _parse_ref(normalized, 'FROM')
        if from_tables:
            return _dedupe(from_tables)
        return _dedupe(_parse_ref(normalized, 'UPDATE'))

    results = []
    for kw in (r'FROM', r'JOIN', r'INTO', r'DELETE\s+FROM'):
        results.extend(_parse_ref(query_text, kw))
    return _dedupe(results)


def looks_wrong(table_name: str) -> bool:
    """Retorna True se o table_name parece estar incorreto e deve ser reprocessado."""
    if not table_name:
        return True
    t = table_name.strip()
    if not t or t.lower() in ('none', 'null', 'unknown', ''):
        return True
    # Casos como "PIMSMCPRD.dbo", "CORPORERM.dbo" — contém ponto (captura errada de db.schema)
    if '.' in t:
        return True
    # Schema sozinho sem tabela (dbo, PUBLIC, public, etc.)
    if t.lower() in ('dbo', 'public', 'schema'):
        return True
    return False


def main():
    if not DUCKDB_PATH.exists():
        print(f"DuckDB nao encontrado em: {DUCKDB_PATH}")
        sys.exit(1)

    print(f"Conectando em: {DUCKDB_PATH}")
    conn = duckdb.connect(str(DUCKDB_PATH))

    rows = conn.execute("""
        SELECT pa.id, pa.query_hash, pa.instance_name, pa.database_name, pa.table_name
        FROM performance_alerts pa
        WHERE pa.query_hash IS NOT NULL AND pa.query_hash != ''
        ORDER BY pa.id
    """).fetchall()

    # Filtra apenas os que parecem errados
    to_fix = [(id_, qh, inst, db, tbl) for id_, qh, inst, db, tbl in rows if looks_wrong(tbl)]

    print(f"Total de alertas: {len(rows)}")
    print(f"Alertas a reprocessar (ausente/errado): {len(to_fix)}")

    if not to_fix:
        print("Nada a fazer.")
        conn.close()
        return

    updated = 0
    skipped = 0

    for alert_id, query_hash, instance_name, database_name, old_table in to_fix:
        qr = conn.execute("""
            SELECT sanitized_query, query_text, db_type
            FROM queries_collected
            WHERE query_hash = ?
            ORDER BY collected_at DESC
            LIMIT 1
        """, [query_hash]).fetchone()

        if not qr:
            skipped += 1
            continue

        sanitized, raw_text, db_type = qr
        query_text = sanitized or raw_text or ''
        if not query_text.strip():
            skipped += 1
            continue

        db_type = (db_type or 'sqlserver').lower()
        if db_type == 'sqlserver':
            tables = extract_tables_sqlserver(query_text)
        elif db_type == 'postgresql':
            tables = extract_tables_generic(query_text, default_schema='public')
        else:
            tables = extract_tables_generic(query_text, default_schema='PUBLIC')

        if not tables:
            skipped += 1
            continue

        new_table = tables[0]['table']
        new_schema = tables[0]['schema']

        if looks_wrong(new_table):
            skipped += 1
            continue

        conn.execute("""
            UPDATE performance_alerts SET table_name = ? WHERE id = ?
        """, [new_table, alert_id])

        print(f"  #{alert_id} | {old_table!r} -> {new_schema}.{new_table}")
        updated += 1

    conn.close()
    print(f"\nConcluido: {updated} atualizados, {skipped} sem dados suficientes.")


if __name__ == '__main__':
    main()
