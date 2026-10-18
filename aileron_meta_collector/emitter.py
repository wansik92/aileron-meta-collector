from __future__ import annotations

import atexit
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor

try:
    import requests as _requests
    _REQUESTS_AVAILABLE = True
except ImportError:
    _REQUESTS_AVAILABLE = False

from .config import (
    get_env, get_gms_url, is_silent_fail, get_connect_timeout_sec, get_retry_max_times, get_cooldown_sec,
)
from .context import JobContext
from .datahub_schema import (
    ET_DATASET, ET_DATA_FLOW, ET_DATA_JOB, ET_DATA_PROCESS,
    AS_DATASET_PROPS, AS_UPSTREAM_LINEAGE, AS_FLOW_INFO, AS_JOB_INFO, AS_JOB_IO,
    AS_RUN_PROPS, AS_RUN_RELS, AS_RUN_EVENT, AS_RUN_INPUT, AS_RUN_OUTPUT,
    CT_UPSERT, CT_PATCH,
)

logger = logging.getLogger(__name__)

_EMIT_MAX_WORKERS = 2
_executor = ThreadPoolExecutor(max_workers=_EMIT_MAX_WORKERS, thread_name_prefix="aileron-emit")

atexit.register(lambda: _executor.shutdown(wait=True, cancel_futures=False))

_session: "_requests.Session | None" = None
_datahub_reachable: bool = True
_last_fail_time: float = 0.0


def flush_emit(timeout: float = 30.0) -> None:
    """모든 비동기 emit 작업이 완료될 때까지 블로킹합니다."""
    futs = [_executor.submit(lambda: None) for _ in range(_EMIT_MAX_WORKERS)]
    for f in futs:
        f.result(timeout=timeout)


def _get_session() -> "_requests.Session":
    global _session
    if not _REQUESTS_AVAILABLE:
        raise ImportError(
            "[aileron] requests 패키지가 설치되지 않았습니다. pip install requests"
        )
    if _session is None:
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter
        retry = Retry(
            total=get_retry_max_times(),
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        _session = _requests.Session()
        _session.headers.update({"Content-Type": "application/json"})
        _session.mount("http://", HTTPAdapter(max_retries=retry))
        _session.mount("https://", HTTPAdapter(max_retries=retry))
    return _session


def _ingest(entity_urn: str, entity_type: str, aspect_name: str, aspect_value: dict | list, change_type: str = CT_UPSERT) -> None:
    session = _get_session()
    proposal = {
        "entityType": entity_type,
        "entityUrn": entity_urn,
        "aspectName": aspect_name,
        "changeType": change_type,
        "aspect": {
            "contentType": "application/json",
            "value": json.dumps(aspect_value),
        },
    }
    url = f"{get_gms_url()}/aspects?action=ingestProposal"
    resp = session.post(url, json={"proposal": proposal}, timeout=(get_connect_timeout_sec(), 30))
    resp.raise_for_status()


def _should_skip() -> bool:
    if not _datahub_reachable:
        if time.time() - _last_fail_time > get_cooldown_sec():
            logger.info("[aileron] DataHub 쿨다운 종료 — 연결 재시도")
            return False
        logger.debug("[aileron] DataHub 연결 불가 상태 — emit skip")
        return True
    return False


def _on_success() -> None:
    global _datahub_reachable
    if not _datahub_reachable:
        logger.info("[aileron] DataHub 연결 복구 — emit 재개")
    _datahub_reachable = True


def _on_failure(e: Exception) -> None:
    global _datahub_reachable, _last_fail_time
    _datahub_reachable = False
    _last_fail_time = time.time()
    if is_silent_fail():
        logger.warning("[aileron] DataHub emit 실패 — %s초 후 재시도: %s", int(get_cooldown_sec()), e)
    else:
        raise e


def _flow_urn(job: JobContext, env: str) -> str:
    return f"urn:li:dataFlow:({job.platform},{job.flow},{env})"


def _job_urn(job: JobContext, env: str) -> str:
    return f"urn:li:dataJob:({_flow_urn(job, env)},{job.job_id})"


def _job_urn_by_id(job_id: str, flow: str, platform: str, env: str) -> str:
    if job_id.startswith("urn:li:"):
        return job_id
    flow_urn = f"urn:li:dataFlow:({platform},{flow},{env})"
    return f"urn:li:dataJob:({flow_urn},{job_id})"


def _instance_urn(job: JobContext) -> str:
    return f"urn:li:dataProcessInstance:{job.run_id}"


# ── Dataset Lineage ───────────────────────────────────────────────────────────

def emit_lineage_async(job: JobContext, input_urns: list[str], output_urns: list[str]) -> None:
    if not input_urns and not output_urns:
        return
    _executor.submit(_emit_lineage, input_urns, output_urns)


def _emit_lineage(input_urns: list[str], output_urns: list[str]) -> None:
    if _should_skip():
        return
    try:
        audit = {"time": int(time.time() * 1000), "actor": "urn:li:corpuser:datahub"}

        for input_urn in input_urns:
            _ingest(input_urn, ET_DATASET, AS_DATASET_PROPS, {})
            logger.debug("input dataset stub emitted: %s", input_urn)

        for output_urn in output_urns:
            upstreams = [
                {"dataset": inp, "type": "TRANSFORMED", "auditStamp": audit}
                for inp in input_urns
            ]
            if not upstreams:
                continue
            _ingest(output_urn, ET_DATASET, AS_UPSTREAM_LINEAGE, {"upstreams": upstreams})
            logger.info("[aileron] emit ok | lineage  %s → %s", input_urns, output_urn)
        _on_success()
    except Exception as e:
        _on_failure(e)


# ── Dataset Description ───────────────────────────────────────────────────────

def emit_dataset_description_async(urn: str, description: str) -> None:
    if not description:
        return
    _executor.submit(_emit_dataset_description, urn, description)


def _emit_dataset_description(urn: str, description: str) -> None:
    if _should_skip():
        return
    try:
        _ingest(urn, ET_DATASET, AS_DATASET_PROPS, {"description": description})
        _on_success()
        logger.debug("Dataset description emitted: %s", urn)
    except Exception as e:
        _on_failure(e)


# ── DataFlow ──────────────────────────────────────────────────────────────────

def emit_dataflow_async(job: JobContext, env: str) -> None:
    _executor.submit(_emit_dataflow, job, env)


def _emit_dataflow(job: JobContext, env: str) -> None:
    if _should_skip():
        return
    try:
        aspect: dict = {"name": job.flow}
        if job.flow_description:
            aspect["description"] = job.flow_description
        _ingest(_flow_urn(job, env), ET_DATA_FLOW, AS_FLOW_INFO, aspect)
        _on_success()
        logger.info("[aileron] emit ok | dataflow  flow=%s", job.flow)
    except Exception as e:
        _on_failure(e)


# ── DataJob ───────────────────────────────────────────────────────────────────

def emit_datajob_async(job: JobContext, env: str) -> None:
    _executor.submit(_emit_datajob, job, env)


def _emit_datajob(job: JobContext, env: str) -> None:
    if _should_skip():
        return
    try:
        aspect: dict = {
            "name": job.job_id,
            "flowUrn": _flow_urn(job, env),
            "type": "PYTHON",
        }
        if job.description:
            aspect["description"] = job.description
        _ingest(_job_urn(job, env), ET_DATA_JOB, AS_JOB_INFO, aspect)
        _on_success()
        logger.info("[aileron] emit ok | datajob   flow=%s  job=%s", job.flow, job.job_id)
    except Exception as e:
        _on_failure(e)


# ── DataProcessInstance ───────────────────────────────────────────────────────

def emit_run_start_async(job: JobContext, env: str) -> None:
    _executor.submit(_emit_run_start, job, env)


def _emit_run_start(job: JobContext, env: str) -> None:
    if _should_skip():
        return
    try:
        instance_urn = _instance_urn(job)
        audit = {"time": job.start_time_ms, "actor": "urn:li:corpuser:datahub"}

        _ingest(instance_urn, ET_DATA_PROCESS, AS_RUN_PROPS, {
            "name": f"{job.job_id}#{job.run_id[:8]}",
            "created": audit,
            "type": "BATCH_AD_HOC",
        })
        _ingest(instance_urn, ET_DATA_PROCESS, AS_RUN_RELS, {
            "upstreamInstances": [],
            "parentTemplate": _job_urn(job, env),
        })
        _ingest(instance_urn, ET_DATA_PROCESS, AS_RUN_EVENT, {
            "status": "STARTED",
            "timestampMillis": job.start_time_ms,
            "attempt": 1,
        })
        _on_success()
        logger.info("[aileron] emit ok | run_start  flow=%s  job=%s  run=%s", job.flow, job.job_id, job.run_id[:8])
    except Exception as e:
        _on_failure(e)


def emit_run_end_async(
    job: JobContext, env: str, success: bool, error_msg: str | None = None, patch: bool = False
) -> None:
    _executor.submit(_emit_run_end, job, env, success, error_msg, patch)


def _emit_run_end(
    job: JobContext, env: str, success: bool, error_msg: str | None, patch: bool = False
) -> None:
    if _should_skip():
        return
    try:
        instance_urn = _instance_urn(job)
        job_urn = _job_urn(job, env)
        end_time_ms = int(time.time() * 1000)
        result_type = "SUCCESS" if success else "FAILURE"

        _ingest(instance_urn, ET_DATA_PROCESS, AS_RUN_EVENT, {
            "status": "COMPLETE",
            "timestampMillis": end_time_ms,
            "attempt": 1,
            "durationMillis": end_time_ms - job.start_time_ms,
            "result": {
                "type": result_type,
                "nativeResultType": result_type,
            },
        })

        if job.inputs:
            _ingest(instance_urn, ET_DATA_PROCESS, AS_RUN_INPUT, {"inputs": job.inputs})
        if job.outputs:
            _ingest(instance_urn, ET_DATA_PROCESS, AS_RUN_OUTPUT, {"outputs": job.outputs})

        upstream_job_urns = [
            _job_urn_by_id(upstream_id, job.flow, job.platform, env)
            for upstream_id in job.upstream_job_ids
        ]

        if job.inputs or job.outputs or upstream_job_urns:
            if patch:
                patch_ops = []
                for urn in job.inputs:
                    patch_ops.append({"op": "add", "path": f"/inputDatasets/{urn}", "value": {"urn": urn}})
                for urn in job.outputs:
                    patch_ops.append({"op": "add", "path": f"/outputDatasets/{urn}", "value": {"urn": urn}})
                for urn in upstream_job_urns:
                    patch_ops.append({"op": "add", "path": f"/inputDatajobs/{urn}", "value": {"urn": urn}})
                _ingest(job_urn, ET_DATA_JOB, AS_JOB_IO, patch_ops, change_type=CT_PATCH)
            else:
                _ingest(job_urn, ET_DATA_JOB, AS_JOB_IO, {
                    "inputDatasets": job.inputs,
                    "outputDatasets": job.outputs,
                    "inputDatajobs": upstream_job_urns,
                })

        _on_success()
        logger.info("[aileron] emit ok | run_end    flow=%s  job=%s  run=%s  result=%s", job.flow, job.job_id, job.run_id[:8], result_type)
    except Exception as e:
        _on_failure(e)
