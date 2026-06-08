from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor

# datahub는 optional dependency — 미설치 시 emit 함수에서 명확한 에러 발생
try:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.specific.datajob import DataJobPatchBuilder
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        DataFlowInfoClass,
        DataJobInfoClass,
        DataJobInputOutputClass,
        DataProcessInstanceInputClass,
        DataProcessInstanceOutputClass,
        DataProcessInstancePropertiesClass,
        DataProcessInstanceRelationshipsClass,
        DataProcessInstanceRunEventClass,
        DataProcessInstanceRunResultClass,
        DataProcessRunStatusClass,
        DataProcessTypeClass,
        DatasetLineageTypeClass,
        RunResultTypeClass,
        UpstreamClass,
        UpstreamLineageClass,
    )
    from datahub.metadata.schema_classes import DatasetPropertiesClass
    _DATAHUB_AVAILABLE = True
except ImportError:
    _DATAHUB_AVAILABLE = False
    DatahubRestEmitter = None  # type: ignore[assignment,misc]

from .config import DATAHUB_ENV, DATAHUB_GMS_URL, DATAHUB_SILENT_FAIL, DATAHUB_CONNECT_TIMEOUT_SEC, DATAHUB_RETRY_MAX_TIMES
from .context import JobContext

logger = logging.getLogger(__name__)

_EMIT_MAX_WORKERS = 2
_executor = ThreadPoolExecutor(max_workers=_EMIT_MAX_WORKERS, thread_name_prefix="aileron-emit")


def flush_emit(timeout: float = 30.0) -> None:
    """모든 비동기 emit 작업이 완료될 때까지 블로킹합니다.

    max_workers 개수만큼 sentinel future를 제출하고 완료를 기다립니다.
    이렇게 하면 큐에 앞서 제출된 모든 작업이 완료된 뒤 sentinel이 실행됩니다.
    """
    futs = [_executor.submit(lambda: None) for _ in range(_EMIT_MAX_WORKERS)]
    for f in futs:
        f.result(timeout=timeout)


def _check_datahub() -> None:
    if not _DATAHUB_AVAILABLE:
        raise ImportError(
            "[aileron] datahub 패키지가 설치되지 않았습니다. "
            "pip install acryl-datahub 또는 "
            "pip install 'aileron-meta-collector[datahub]'"
        )


def _get_emitter() -> "DatahubRestEmitter":
    _check_datahub()
    return DatahubRestEmitter(
        gms_server=DATAHUB_GMS_URL,
        connect_timeout_sec=DATAHUB_CONNECT_TIMEOUT_SEC,
        retry_max_times=DATAHUB_RETRY_MAX_TIMES,
    )


def _safe_emit(emitter, mcps: list) -> None:
    for mcp in mcps:
        emitter.emit(mcp)


def _flow_urn(job: JobContext, env: str) -> str:
    return f"urn:li:dataFlow:({job.platform},{job.flow},{env})"


def _job_urn(job: JobContext, env: str) -> str:
    return f"urn:li:dataJob:({_flow_urn(job, env)},{job.job_id})"


def _job_urn_by_id(job_id: str, flow: str, platform: str, env: str) -> str:
    """job_id 또는 full URN 으로 DataJob URN 반환 (upstream_jobs 용)
    full URN(urn:li:...) 이면 그대로 사용 — cross-platform upstream 참조 가능.
    """
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
    try:
        _check_datahub()
        emitter = _get_emitter()
        audit = AuditStampClass(time=int(time.time() * 1000), actor="urn:li:corpuser:datahub")

        # input dataset stub entity 생성 — entity가 없으면 DataJob upstream 연결이 UI에 표시되지 않음
        for input_urn in input_urns:
            emitter.emit(MetadataChangeProposalWrapper(
                entityUrn=input_urn,
                aspect=DatasetPropertiesClass(),
            ))
            logger.debug("input dataset stub emitted: %s", input_urn)

        for output_urn in output_urns:
            upstreams = [
                UpstreamClass(dataset=inp, type=DatasetLineageTypeClass.TRANSFORMED, auditStamp=audit)
                for inp in input_urns
            ]
            if not upstreams:
                continue
            emitter.emit(MetadataChangeProposalWrapper(
                entityUrn=output_urn,
                aspect=UpstreamLineageClass(upstreams=upstreams),
            ))
            logger.info("[aileron] emit ok | lineage  %s → %s", input_urns, output_urn)

    except Exception:
        if DATAHUB_SILENT_FAIL:
            logger.warning("Dataset lineage emit failed (silent)", exc_info=True)
        else:
            raise


# ── Dataset Description ───────────────────────────────────────────────────────

def emit_dataset_description_async(urn: str, description: str) -> None:
    if not description:
        return
    _executor.submit(_emit_dataset_description, urn, description)


def _emit_dataset_description(urn: str, description: str) -> None:
    try:
        _check_datahub()
        emitter = _get_emitter()
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DatasetPropertiesClass(description=description),
        ))
        logger.debug("Dataset description emitted: %s", urn)
    except Exception:
        if DATAHUB_SILENT_FAIL:
            logger.warning("Dataset description emit failed (silent)", exc_info=True)
        else:
            raise


# ── DataFlow ──────────────────────────────────────────────────────────────────

def emit_dataflow_async(job: JobContext, env: str) -> None:
    _executor.submit(_emit_dataflow, job, env)


def _emit_dataflow(job: JobContext, env: str) -> None:
    try:
        _check_datahub()
        emitter = _get_emitter()
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=_flow_urn(job, env),
            aspect=DataFlowInfoClass(
                name=job.flow,
                description=job.flow_description,
            ),
        ))
        logger.info("[aileron] emit ok | dataflow  flow=%s", job.flow)
    except Exception:
        if DATAHUB_SILENT_FAIL:
            logger.warning("DataFlow emit failed (silent)", exc_info=True)
        else:
            raise


# ── DataJob ───────────────────────────────────────────────────────────────────

def emit_datajob_async(job: JobContext, env: str) -> None:
    _executor.submit(_emit_datajob, job, env)


def _emit_datajob(job: JobContext, env: str) -> None:
    try:
        _check_datahub()
        emitter = _get_emitter()
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=_job_urn(job, env),
            aspect=DataJobInfoClass(
                name=job.job_id,
                flowUrn=_flow_urn(job, env),
                type="PYTHON",
                description=job.description,
            ),
        ))
        logger.info("[aileron] emit ok | datajob   flow=%s  job=%s", job.flow, job.job_id)
    except Exception:
        if DATAHUB_SILENT_FAIL:
            logger.warning("DataJob emit failed (silent)", exc_info=True)
        else:
            raise


# ── DataProcessInstance ───────────────────────────────────────────────────────

def emit_run_start_async(job: JobContext, env: str) -> None:
    _executor.submit(_emit_run_start, job, env)


def _emit_run_start(job: JobContext, env: str) -> None:
    try:
        _check_datahub()
        emitter = _get_emitter()
        instance_urn = _instance_urn(job)
        audit = AuditStampClass(time=job.start_time_ms, actor="urn:li:corpuser:datahub")

        _safe_emit(emitter, [
            MetadataChangeProposalWrapper(
                entityUrn=instance_urn,
                aspect=DataProcessInstancePropertiesClass(
                    name=f"{job.job_id}#{job.run_id[:8]}",
                    created=audit,
                    type=DataProcessTypeClass.BATCH_AD_HOC,
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=instance_urn,
                aspect=DataProcessInstanceRelationshipsClass(
                    upstreamInstances=[],
                    parentTemplate=_job_urn(job, env),
                ),
            ),
            MetadataChangeProposalWrapper(
                entityUrn=instance_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.STARTED,
                    timestampMillis=job.start_time_ms,
                    attempt=1,
                ),
            ),
        ])
        logger.info("[aileron] emit ok | run_start  flow=%s  job=%s  run=%s", job.flow, job.job_id, job.run_id[:8])

    except Exception:
        if DATAHUB_SILENT_FAIL:
            logger.warning("DataProcessInstance start emit failed (silent)", exc_info=True)
        else:
            raise


def emit_run_end_async(
    job: JobContext, env: str, success: bool, error_msg: str | None = None, patch: bool = False
) -> None:
    _executor.submit(_emit_run_end, job, env, success, error_msg, patch)


def _emit_run_end(
    job: JobContext, env: str, success: bool, error_msg: str | None, patch: bool = False
) -> None:
    try:
        _check_datahub()
        emitter = _get_emitter()
        instance_urn = _instance_urn(job)
        job_urn = _job_urn(job, env)
        end_time_ms = int(time.time() * 1000)

        result_type = RunResultTypeClass.SUCCESS if success else RunResultTypeClass.FAILURE

        mcps: list = [
            MetadataChangeProposalWrapper(
                entityUrn=instance_urn,
                aspect=DataProcessInstanceRunEventClass(
                    status=DataProcessRunStatusClass.COMPLETE,
                    timestampMillis=end_time_ms,
                    attempt=1,
                    durationMillis=end_time_ms - job.start_time_ms,
                    result=DataProcessInstanceRunResultClass(
                        type=result_type,
                        nativeResultType=result_type,
                    ),
                ),
            ),
        ]

        if job.inputs:
            mcps.append(MetadataChangeProposalWrapper(
                entityUrn=instance_urn,
                aspect=DataProcessInstanceInputClass(inputs=job.inputs),
            ))
        if job.outputs:
            mcps.append(MetadataChangeProposalWrapper(
                entityUrn=instance_urn,
                aspect=DataProcessInstanceOutputClass(outputs=job.outputs),
            ))

        upstream_job_urns = [
            _job_urn_by_id(upstream_id, job.flow, job.platform, env)
            for upstream_id in job.upstream_job_ids
        ]

        if job.inputs or job.outputs or upstream_job_urns:
            if patch:
                patch_builder = DataJobPatchBuilder(job_urn)
                for urn in job.inputs:
                    patch_builder.add_input_dataset(urn)
                for urn in job.outputs:
                    patch_builder.add_output_dataset(urn)
                for urn in upstream_job_urns:
                    patch_builder.add_input_datajob(urn)
                mcps.extend(patch_builder.build())
            else:
                mcps.append(MetadataChangeProposalWrapper(
                    entityUrn=job_urn,
                    aspect=DataJobInputOutputClass(
                        inputDatasets=job.inputs,
                        outputDatasets=job.outputs,
                        inputDatajobs=upstream_job_urns,
                    ),
                ))

        _safe_emit(emitter, mcps)
        logger.info("[aileron] emit ok | run_end    flow=%s  job=%s  run=%s  result=%s", job.flow, job.job_id, job.run_id[:8], result_type)

    except Exception:
        if DATAHUB_SILENT_FAIL:
            logger.warning("DataProcessInstance end emit failed (silent)", exc_info=True)
        else:
            raise
