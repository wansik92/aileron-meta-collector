from __future__ import annotations

import re

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Parenthesis
from sqlparse.tokens import DML, Keyword

_WRITE_OPS = {"INSERT", "UPDATE", "DELETE", "MERGE", "CREATE"}
_READ_OPS = {"SELECT"}

# lineage 추적 대상에서 제외할 스키마 접두사
_SKIP_SCHEMAS = {"information_schema", "pg_catalog", "sys", "performance_schema"}

_RE_UNLOAD_INNER = re.compile(r"UNLOAD\s*\((.+)\)\s*TO", re.IGNORECASE | re.DOTALL)
_RE_UNLOAD_TO    = re.compile(r"\bTO\s+['\"]?(s3://[^\s'\"]+)['\"]?", re.IGNORECASE)


def extract_tables(sql: str) -> tuple[list[str], list[str]]:
    """
    Returns (input_tables, output_tables).

    - SELECT → input만
    - INSERT/UPDATE/DELETE/MERGE → 첫 테이블=output, 나머지=input
    - CREATE TABLE ... AS SELECT → output=생성 테이블, input=SELECT 대상
    - UNLOAD (SELECT ...) TO 's3://...' → input=SELECT 테이블, output=S3 URN
    """
    stripped = sql.strip()

    if stripped.upper().startswith("UNLOAD"):
        return _parse_unload(stripped)

    parsed = sqlparse.parse(stripped)
    if not parsed:
        return [], []

    stmt = parsed[0]
    op = _get_dml_op(stmt)
    tables = _extract_tables_from_stmt(stmt)

    if op in _WRITE_OPS:
        return tables[1:], tables[:1]
    elif op in _READ_OPS:
        return tables, []
    return [], []


def _parse_unload(sql: str) -> tuple[list[str], list[str]]:
    # 내부 SELECT에서 input 테이블 추출
    inputs: list[str] = []
    inner_match = _RE_UNLOAD_INNER.search(sql)
    if inner_match:
        inner_sql = inner_match.group(1).strip()
        inputs, _ = extract_tables(inner_sql)

    # TO 절 S3 경로 → output URN (env는 호출 시점에 모르므로 placeholder)
    outputs: list[str] = []
    to_match = _RE_UNLOAD_TO.search(sql)
    if to_match:
        s3_path = to_match.group(1).rstrip("/").replace("s3://", "")
        # env는 hooks 레이어에서 주입 — 파서는 raw path만 반환
        outputs.append(f"__s3__{s3_path}")

    return inputs, outputs


def _get_dml_op(stmt: sqlparse.sql.Statement) -> str:
    for token in stmt.tokens:
        if token.ttype is DML:
            return token.normalized.upper()
        if token.ttype is Keyword and token.normalized.upper() == "CREATE":
            return "CREATE"
    return ""


def _extract_tables_from_stmt(stmt: sqlparse.sql.Statement) -> list[str]:
    tables: list[str] = []
    _walk(stmt.tokens, tables)

    seen: set[str] = set()
    result: list[str] = []
    for t in tables:
        normalized = t.strip('"').strip("'").lower()
        schema = normalized.split(".")[0] if "." in normalized else ""
        if normalized and normalized not in seen and schema not in _SKIP_SCHEMAS:
            seen.add(normalized)
            result.append(normalized)
    return result


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
                # Identifier 내부 서브쿼리도 탐색
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
    # 서브쿼리 (괄호로 감싸진 경우) 는 테이블명이 아님
    if any(isinstance(t, Parenthesis) for t in ident.tokens):
        return None
    name = ident.get_real_name()
    schema = ident.get_parent_name()
    if not name:
        return None
    return f"{schema}.{name}" if schema else name
