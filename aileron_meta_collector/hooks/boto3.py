from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

from ..context import JobContext, get_job
from ..emitter import emit_lineage_async
from ..parsers.sql_parser import extract_tables

logger = logging.getLogger(__name__)

_S3_READ_OPS = frozenset({
    "GetObject", "HeadObject", "SelectObjectContent", "GetObjectAcl",
    "ListObjectsV2", "ListObjects",
})
_S3_WRITE_OPS = frozenset({
    "PutObject", "CopyObject", "DeleteObject",
    "CreateMultipartUpload", "CompleteMultipartUpload",
})

# Athena 쿼리 완료 전까지 SQL·context를 보관하는 저장소
@dataclass
class _PendingAthenaQuery:
    sql: str
    database: str
    catalog: str
    job: JobContext

_pending_athena: dict[str, _PendingAthenaQuery] = {}
_pending_athena_lock = threading.Lock()

# StartQueryExecution before/after 사이에 파라미터를 전달하는 thread-local
_athena_req_local = threading.local()


def _s3_urn(bucket: str, key: str, env: str) -> str:
    prefix = "/".join(key.split("/")[:-1]) if "/" in key else ""
    dataset_name = f"{bucket}/{prefix}".rstrip("/") if prefix else bucket
    return f"urn:li:dataset:(urn:li:dataPlatform:s3,{dataset_name},{env})"


def _athena_table_urn(table: str, catalog: str, database: str, env: str) -> str:
    """
    Athena 테이블을 Glue Data Catalog URN으로 변환.
    - table              → database.table
    - database.table     → 그대로 사용
    - catalog.db.table   → catalog 제거 후 database.table
    """
    table    = table.strip()
    database = database.strip()
    parts = table.split(".")
    if len(parts) == 1:
        dataset = f"{database}.{table}"
    elif len(parts) == 2:
        dataset = table
    else:
        # catalog.database.table → database.table
        dataset = ".".join(parts[1:])
    dataset = dataset.strip()
    return f"urn:li:dataset:(urn:li:dataPlatform:glue,{dataset},{env})"


def _resolve_athena_urns(
    inputs_raw: list[str],
    outputs_raw: list[str],
    catalog: str,
    database: str,
    env: str,
) -> tuple[list[str], list[str]]:
    input_urns: list[str] = []
    for t in inputs_raw:
        input_urns.append(_athena_table_urn(t, catalog, database, env))

    output_urns: list[str] = []
    for t in outputs_raw:
        if t.startswith("__s3__"):
            # UNLOAD TO 's3://...' 결과
            s3_path = t[len("__s3__"):]
            output_urns.append(
                f"urn:li:dataset:(urn:li:dataPlatform:s3,{s3_path},{env})"
            )
        else:
            output_urns.append(_athena_table_urn(t, catalog, database, env))

    return input_urns, output_urns


def install_boto3_hooks(env: str = "PROD") -> None:
    """
    boto3 default session에 S3 + Athena 이벤트 훅을 등록합니다.
    앱 시작 시 한 번만 호출하세요.
    """
    try:
        import boto3  # lazy import — boto3 미설치 시 안전하게 실패
    except ImportError:
        logger.warning(
            "[aileron] boto3가 설치되지 않아 S3/Athena 훅을 등록하지 않습니다. "
            "pip install boto3 또는 pip install 'aileron-meta-collector[boto3]'"
        )
        return

    session = boto3._get_default_session()
    events = session.events

    # ── S3 훅 ─────────────────────────────────────────────────────────────

    def _handle_s3(event_name: str, params: dict, **kwargs):
        job = get_job()
        if not job:
            return
        try:
            operation = event_name.split(".")[-1]
            bucket = params.get("Bucket", "")
            key = params.get("Key", "")

            if operation == "CopyObject":
                copy_src = params.get("CopySource", {})
                if isinstance(copy_src, dict):
                    src_urn = _s3_urn(copy_src.get("Bucket", ""), copy_src.get("Key", ""), env)
                    dst_urn = _s3_urn(bucket, key, env)
                    if src_urn not in job.inputs:
                        job.inputs.append(src_urn)
                    if dst_urn not in job.outputs:
                        job.outputs.append(dst_urn)
                    emit_lineage_async(job, [src_urn], [dst_urn])
                return

            if not bucket:
                return

            urn = _s3_urn(bucket, key, env)
            if operation in _S3_READ_OPS:
                if urn not in job.inputs:
                    job.inputs.append(urn)
                emit_lineage_async(job, [urn], [])
            elif operation in _S3_WRITE_OPS:
                if urn not in job.outputs:
                    job.outputs.append(urn)
                emit_lineage_async(job, [], [urn])

        except Exception:
            logger.debug("boto3 S3 lineage hook error", exc_info=True)

    for op in _S3_READ_OPS | _S3_WRITE_OPS:
        events.register(f"before-parameter-build.s3.{op}", _handle_s3)

    # ── Athena 훅 ──────────────────────────────────────────────────────────
    #
    # 흐름:
    #   1. before-call.StartQueryExecution  → SQL + job context를 thread-local에 저장
    #   2. after-call.StartQueryExecution   → execution_id를 키로 _pending_athena에 이관
    #   3. after-call.GetQueryExecution     → SUCCEEDED 시점에만 lineage emit
    #                                         FAILED / CANCELLED 시 pending 정리

    def _athena_before_start(params: dict, **kwargs):
        job = get_job()
        if not job:
            _athena_req_local.pending = None
            return
        ctx = params.get("QueryExecutionContext", {})
        _athena_req_local.pending = _PendingAthenaQuery(
            sql=params.get("QueryString", "").strip(),
            database=ctx.get("Database", "default").strip(),
            catalog=ctx.get("Catalog", "AwsDataCatalog").strip(),
            job=job,
        )

    def _athena_after_start(parsed_response: dict = None, parsed: dict = None, **kwargs):
        # botocore < 1.40: parsed_response / botocore >= 1.40: parsed
        response = parsed_response or parsed or {}
        pending = getattr(_athena_req_local, "pending", None)
        _athena_req_local.pending = None
        if not pending or not pending.sql:
            return
        execution_id = response.get("QueryExecutionId", "")
        if not execution_id:
            return
        with _pending_athena_lock:
            _pending_athena[execution_id] = pending

    def _athena_after_get_execution(parsed_response: dict = None, parsed: dict = None, **kwargs):
        # botocore < 1.40: parsed_response / botocore >= 1.40: parsed
        response = parsed_response or parsed or {}
        execution = response.get("QueryExecution", {})
        state = execution.get("Status", {}).get("State", "")
        execution_id = execution.get("QueryExecutionId", "")

        if state in ("RUNNING", "QUEUED", ""):
            return  # 아직 진행 중 — pending 유지

        with _pending_athena_lock:
            pending = _pending_athena.pop(execution_id, None)

        if not pending:
            return

        if state != "SUCCEEDED":
            logger.debug("Athena query %s ended with state=%s — lineage skipped", execution_id, state)
            return

        try:
            inputs_raw, outputs_raw = extract_tables(pending.sql)
            input_urns, output_urns = _resolve_athena_urns(
                inputs_raw, outputs_raw, pending.catalog, pending.database, env
            )

            for u in input_urns:
                if u not in pending.job.inputs:
                    pending.job.inputs.append(u)
            for u in output_urns:
                if u not in pending.job.outputs:
                    pending.job.outputs.append(u)

            emit_lineage_async(pending.job, input_urns, output_urns)

        except Exception:
            logger.debug("Athena lineage emit error", exc_info=True)

    # before-parameter-build: 원본 API 파라미터(QueryString 등)가 살아있는 시점
    # before-call: 이미 HTTP 직렬화된 이후라 QueryString 없음
    events.register("before-parameter-build.athena.StartQueryExecution", _athena_before_start)
    events.register("after-call.athena.StartQueryExecution",  _athena_after_start)
    events.register("after-call.athena.GetQueryExecution",    _athena_after_get_execution)

    logger.info("[aileron] boto3 S3 + Athena hooks installed (env=%s)", env)
