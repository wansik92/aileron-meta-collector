from __future__ import annotations

import re

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Parenthesis
from sqlparse.tokens import DML, DDL, Keyword

# lineage 추적 대상에서 제외할 스키마 접두사
_SKIP_SCHEMAS = {"information_schema", "pg_catalog", "sys", "performance_schema"}

_RE_UNLOAD_INNER = re.compile(r"UNLOAD\s*\((.+)\)\s*TO", re.IGNORECASE | re.DOTALL)
_RE_UNLOAD_TO    = re.compile(r"\bTO\s+['\"]?(s3://[^\s'\"]+)['\"]?", re.IGNORECASE)

# CREATE TABLE <name> [WITH (...)] AS SELECT
_RE_CTAS = re.compile(
    r"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)",
    re.IGNORECASE,
)
# AS SELECT ... 이후 본문 추출
_RE_CTAS_BODY = re.compile(r"\bAS\s+(SELECT\b.+)$", re.IGNORECASE | re.DOTALL)

# UPDATE <table>
_RE_UPDATE = re.compile(r"UPDATE\s+([^\s(]+)", re.IGNORECASE)

# INSERT INTO <table>
_RE_INSERT = re.compile(r"INSERT\s+(?:OVERWRITE\s+)?(?:INTO\s+)?([^\s(]+)", re.IGNORECASE)


def extract_tables(sql: str) -> tuple[list[str], list[str]]:
    """
    Returns (input_tables, output_tables).

    - SELECT                 → input만
    - INSERT INTO / OVERWRITE→ output=대상 테이블, input=SELECT 테이블
    - UPDATE                 → output=대상 테이블, input=FROM/JOIN 테이블
    - CREATE TABLE … AS SELECT (CTAS) → output=생성 테이블, input=SELECT 테이블
    - UNLOAD (SELECT …) TO 's3://…' → input=SELECT 테이블, output=__s3__ 마커
    """
    stripped = sql.strip()
    upper = stripped.upper().lstrip()

    # ── UNLOAD ───────────────────────────────────────────────────────────────
    if upper.startswith("UNLOAD"):
        return _parse_unload(stripped)

    parsed = sqlparse.parse(stripped)
    if not parsed:
        return [], []

    stmt = parsed[0]
    op = _get_stmt_type(stmt)

    # ── CTAS / CREATE EXTERNAL TABLE AS SELECT ───────────────────────────────
    if op == "CREATE":
        return _parse_ctas(stripped)

    # ── INSERT INTO ──────────────────────────────────────────────────────────
    if op == "INSERT":
        return _parse_insert(stripped, stmt)

    # ── UPDATE ───────────────────────────────────────────────────────────────
    if op == "UPDATE":
        return _parse_update(stripped, stmt)

    # ── SELECT ───────────────────────────────────────────────────────────────
    if op == "SELECT":
        inputs = _collect_from_tables(stmt)
        return _dedupe(inputs), []

    return [], []


# ── UNLOAD ────────────────────────────────────────────────────────────────────

def _parse_unload(sql: str) -> tuple[list[str], list[str]]:
    inputs: list[str] = []
    inner_match = _RE_UNLOAD_INNER.search(sql)
    if inner_match:
        inner_sql = inner_match.group(1).strip()
        inputs, _ = extract_tables(inner_sql)

    outputs: list[str] = []
    to_match = _RE_UNLOAD_TO.search(sql)
    if to_match:
        s3_path = to_match.group(1).rstrip("/").replace("s3://", "")
        outputs.append(f"__s3__{s3_path}")

    return inputs, outputs


# ── CTAS ──────────────────────────────────────────────────────────────────────

def _parse_ctas(sql: str) -> tuple[list[str], list[str]]:
    """CREATE [EXTERNAL] TABLE <name> [WITH (...)] AS SELECT ..."""
    output_table = ""
    m = _RE_CTAS.search(sql)
    if m:
        output_table = _normalize(m.group(1))

    # WITH (...) 절이 있을 수 있으므로 AS SELECT 이후 본문만 추출
    body_match = _RE_CTAS_BODY.search(sql)
    inputs: list[str] = []
    if body_match:
        select_body = body_match.group(1)
        sub_parsed = sqlparse.parse(select_body)
        if sub_parsed:
            inputs = _dedupe(_collect_from_tables(sub_parsed[0]))

    outputs = [output_table] if output_table else []
    return inputs, outputs


# ── INSERT INTO ───────────────────────────────────────────────────────────────

def _parse_insert(sql: str, stmt) -> tuple[list[str], list[str]]:
    output_table = ""
    m = _RE_INSERT.search(sql)
    if m:
        output_table = _normalize(m.group(1))

    # SELECT 부분에서 input 추출
    inputs = _dedupe(_collect_from_tables(stmt))
    # output 테이블 자신이 input에도 잡힐 수 있으면 제거
    inputs = [t for t in inputs if t != output_table]

    outputs = [output_table] if output_table else []
    return inputs, outputs


# ── UPDATE ────────────────────────────────────────────────────────────────────

def _parse_update(sql: str, stmt) -> tuple[list[str], list[str]]:
    output_table = ""
    m = _RE_UPDATE.search(sql)
    if m:
        output_table = _normalize(m.group(1))

    # UPDATE … FROM … 패턴의 추가 input 테이블
    inputs = _dedupe(_collect_from_tables(stmt))
    inputs = [t for t in inputs if t != output_table]

    outputs = [output_table] if output_table else []
    return inputs, outputs


# ── 공통 FROM/JOIN 테이블 수집 ─────────────────────────────────────────────────

def _collect_from_tables(stmt) -> list[str]:
    tables: list[str] = []
    _walk(stmt.tokens, tables)
    return tables


def _walk(tokens: list, tables: list[str], after_from: bool = False) -> None:
    for token in tokens:
        if token.is_whitespace:
            continue

        if token.ttype is Keyword and token.normalized.upper() in (
            "FROM", "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN",
            "FULL JOIN", "CROSS JOIN", "INTO",
        ):
            after_from = True
            continue

        if token.ttype is Keyword and token.normalized.upper() in (
            "WHERE", "SET", "ON", "GROUP", "ORDER", "HAVING",
            "LIMIT", "UNION", "EXCEPT", "INTERSECT",
        ):
            after_from = False
            continue

        if after_from and token.ttype is None:
            if isinstance(token, Identifier):
                name = _resolve_name(token)
                if name:
                    tables.append(name)
                after_from = False
                _walk(token.tokens, tables, after_from=False)
                continue
            elif isinstance(token, IdentifierList):
                for ident in token.get_identifiers():
                    if isinstance(ident, Identifier):
                        name = _resolve_name(ident)
                        if name:
                            tables.append(name)
                        _walk(ident.tokens, tables, after_from=False)
                after_from = False
                continue

        if hasattr(token, "tokens"):
            _walk(token.tokens, tables, after_from)


def _resolve_name(ident: Identifier) -> str | None:
    if any(isinstance(t, Parenthesis) for t in ident.tokens):
        return None
    name = ident.get_real_name()
    schema = ident.get_parent_name()
    if not name:
        return None
    full = f"{schema}.{name}" if schema else name
    return _normalize(full)


# ── 유틸 ──────────────────────────────────────────────────────────────────────

def _get_stmt_type(stmt: sqlparse.sql.Statement) -> str:
    for token in stmt.tokens:
        if token.ttype is DML:
            return token.normalized.upper()
        if token.ttype is DDL:
            return token.normalized.upper()
        if token.ttype is Keyword and token.normalized.upper() == "CREATE":
            return "CREATE"
    return ""


def _normalize(name: str) -> str:
    return name.strip('"').strip("'").strip("`").lower()


def _dedupe(tables: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for t in tables:
        schema = t.split(".")[0] if "." in t else ""
        if t and t not in seen and schema not in _SKIP_SCHEMAS:
            seen.add(t)
            result.append(t)
    return result
