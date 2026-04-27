from __future__ import annotations

import logging

from sqlalchemy import event
from sqlalchemy.engine import Engine

from ..context import get_job
from ..emitter import emit_lineage_async
from ..parsers.sql_parser import extract_tables

logger = logging.getLogger(__name__)

_PLATFORM_MAP = {
    "postgresql": "postgres",
    "postgres": "postgres",
    "mysql": "mysql",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "sqlite": "sqlite",
    "mssql": "mssql",
}


def _infer_platform(db_url: str) -> str:
    url_lower = db_url.lower()
    for key, platform in _PLATFORM_MAP.items():
        if key in url_lower:
            return platform
    return "sql"


def _to_urn(table: str, platform: str, env: str) -> str:
    # UNLOAD의 S3 output은 파서가 __s3__{path} 형태로 반환
    if table.startswith("__s3__"):
        s3_path = table[len("__s3__"):]
        return f"urn:li:dataset:(urn:li:dataPlatform:s3,{s3_path},{env})"
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{table},{env})"


def install_sqlalchemy_hooks(env: str = "PROD") -> None:
    """
    모든 SQLAlchemy Engine 인스턴스에 lineage hook을 자동 등록합니다.
    앱 시작 시 한 번만 호출하세요.
    """

    @event.listens_for(Engine, "after_cursor_execute", named=True)
    def _after_execute(conn, cursor, statement, parameters, context, executemany, **kw):
        job = get_job()
        if not job:
            return

        try:
            db_url = str(conn.engine.url)
            platform = _infer_platform(db_url)
            inputs_raw, outputs_raw = extract_tables(statement)

            input_urns = [_to_urn(t, platform, env) for t in inputs_raw]
            output_urns = [_to_urn(t, platform, env) for t in outputs_raw]

            # job context에 누적 (중복 방지)
            for u in input_urns:
                if u not in job.inputs:
                    job.inputs.append(u)
            for u in output_urns:
                if u not in job.outputs:
                    job.outputs.append(u)

            emit_lineage_async(job, input_urns, output_urns)

        except Exception:
            logger.debug("SQLAlchemy lineage hook error", exc_info=True)

    logger.info("[aileron] SQLAlchemy hooks installed (env=%s)", env)
