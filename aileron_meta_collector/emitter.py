from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
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

from .config import DATAHUB_ENV, DATAHUB_GMS_URL, DATAHUB_SILENT_FAIL
from .context import JobContext

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="aileron-emit")


def _get_emitter() -> DatahubRestEmitter:
    return DatahubRestEmitter(gms_server=DATAHUB_GMS_URL)


def _safe_emit(emitter: DatahubRestEmitter, mcps: list) -> None:
    for mcp in mcps:
        emitter.emit(mcp)


def _flow_urn(job: JobContext, env: str) -> str:
    return f"urn:li:dataFlow:({job.platform},{job.flow},{env})"


def _job_urn(job: JobContext, env: str) -> str:
    return f"urn:li:dataJob:({_flow_urn(job, env)},{job.job_id})"


def _job_urn_by_id(job_id: str, flow: str, platform: str, env: str) -> str:
    """job_id만 알고 있을 때 DataJob URN 생성 (upstream_jobs 용)"""
    flow_urn = f"urn:li:dataFlow:({platform},{flow},{env})"
    return f"urn:li:dataJob:({flow_urn},{job_id})"


def _instance_urn(job: JobContext) -> str:
    return f"urn:li:dataProcessInstance:{job.run_id}"


# ── Dataset Lineage ────────────────────────────���──────────────────────────────

def emit_lineage_async(job: JobContext, input_urns: list[str], output_urns: list[str]) -> None:
    if not input_urns and not output_urns:
        return
    _executor.submit(_emit_lineage, input_urns, output_urns)


def _emit_lineage(input_urns: list[str], output_urns: list[str]) -> None:
    try:
        emitter = _get_emitter()
        audit = AuditStampClass(time=int(time.time() * 1000), actor="urn:li:corpuser:datahub")

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
            logger.debug("lineage emitted: %s -> %s", input_urns, output_urn)

    except Exception:
        if DATAHUB_SILENT_FAIL:
            logger.warning("Dataset lineage emit failed (silent)", exc_info=True)
        else:
            raise


# ── DataFlow ──────────────────────────────────────────────────────────��───────

def emit_dataflow_async(job: JobContext, env: str) -> None:
    _executor.submit(_emit_dataflow, job, env)


def _emit_dataflow(job: JobContext, env: str) -> None:
    try:
        emitter = _get_emitter()
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=_flow_urn(job, env),
            aspect=DataFlowInfoClass(name=job.flow, env=env),
        ))
        logger.debug("DataFlow emitted: %s", job.flow)
    except Exception:
        if DATAHUB_SILENT_FAIL:
            logger.warning("DataFlow emit failed (silent)", exc_info=True)
        else:
            raise


# ── DataJob ──────────────────────────────���─────────────────────────────��──────

def emit_datajob_async(job: JobContext, env: str) -> None:
    _executor.submit(_emit_datajob, job, env)


def _emit_datajob(job: JobContext, env: str) -> None:
    try:
        emitter = _get_emitter()
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=_job_urn(job, env),
            aspect=DataJobInfoClass(name=job.job_id, flowUrn=_flow_urn(job, env), type="PYTHON"),
        ))
        logger.debug("DataJob emitted: %s", job.job_id)
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
        logger.debug("DataProcessInstance STARTED: %s", instance_urn)

    except Exception:
        if DATAHUB_SILENT_FAIL:
            logger.warning("DataProcessInstance start emit failed (silent)", exc_info=True)
        else:
            raise


def emit_run_end_async(
    job: JobContext, env: str, success: bool, error_msg: str | None = None
) -> None:
    _executor.submit(_emit_run_end, job, env, success, error_msg)


def _emit_run_end(
    job: JobContext, env: str, success: bool, error_msg: str | None
) -> None:
    try:
        emitter = _get_emitter()
        instance_urn = _instance_urn(job)
        job_urn = _job_urn(job, env)
        end_time_ms = int(time.time() * 1000)

        # FAILED 상태 없음 — COMPLETE + result.type=FAILURE 조합으로 표현
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

        # DataProcessInstance input/output 연결
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

        # upstream DataJob URN 목록 (같은 flow 내 job_id 기준)
        upstream_job_urns = [
            _job_urn_by_id(upstream_id, job.flow, job.platform, env)
            for upstream_id in job.upstream_job_ids
        ]

        # DataJob 최신 inlet/outlet + job 간 의존 관계 업데이트
        if job.inputs or job.outputs or upstream_job_urns:
            mcps.append(MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=job.inputs,
                    outputDatasets=job.outputs,
                    inputDatajobs=upstream_job_urns,
                ),
            ))

        _safe_emit(emitter, mcps)
        logger.debug("DataProcessInstance %s: %s", result_type, instance_urn)

    except Exception:
        if DATAHUB_SILENT_FAIL:
            logger.warning("DataProcessInstance end emit failed (silent)", exc_info=True)
        else:
            raise
