from __future__ import annotations

import functools
import logging
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Callable, Generator, Optional, TypeVar

F = TypeVar("F", bound=Callable)

_local = threading.local()
logger = logging.getLogger(__name__)


@dataclass
class JobContext:
    job_id: str
    flow: str = "default"
    platform: str = "pythonSdk"
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    upstream_job_ids: list[str] = field(default_factory=list)   # DataJob 간 의존 관계
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    start_time_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    description: str | None = None        # DataJob description
    flow_description: str | None = None   # DataFlow description


def set_job(
    job_id: str,
    flow: str = "default",
    platform: str = "pythonSdk",
    upstream_jobs: list[str] | None = None,
    description: str | None = None,
    flow_description: str | None = None,
) -> None:
    _local.job = JobContext(
        job_id=job_id,
        flow=flow,
        platform=platform,
        upstream_job_ids=upstream_jobs or [],
        description=description,
        flow_description=flow_description,
    )


def get_job() -> Optional[JobContext]:
    return getattr(_local, "job", None)


def clear_job() -> None:
    _local.job = None


@contextmanager
def datahub_job(
    job_id: str,
    flow: str = "default",
    platform: str = "pythonSdk",
    upstream_jobs: list[str] | None = None,
    description: str | None = None,
    flow_description: str | None = None,
) -> Generator[JobContext, None, None]:
    from .config import DATAHUB_ENV
    from .emitter import (
        emit_dataflow_async,
        emit_datajob_async,
        emit_run_end_async,
        emit_run_start_async,
    )

    set_job(job_id, flow, platform, upstream_jobs=upstream_jobs,
            description=description, flow_description=flow_description)
    job = get_job()

    logger.info("[aileron] job started  | flow=%s  job=%s  run=%s", flow, job_id, job.run_id[:8])

    emit_dataflow_async(job, DATAHUB_ENV)
    emit_datajob_async(job, DATAHUB_ENV)
    emit_run_start_async(job, DATAHUB_ENV)

    try:
        yield job
        elapsed = int(time.time() * 1000) - job.start_time_ms
        logger.info(
            "[aileron] job finished | flow=%s  job=%s  run=%s  elapsed=%dms  inputs=%d  outputs=%d",
            flow, job_id, job.run_id[:8], elapsed, len(job.inputs), len(job.outputs),
        )
        logger.debug("[aileron] inputs  : %s", job.inputs)
        logger.debug("[aileron] outputs : %s", job.outputs)
        emit_run_end_async(job, DATAHUB_ENV, success=True)
    except Exception as e:
        elapsed = int(time.time() * 1000) - job.start_time_ms
        logger.warning(
            "[aileron] job failed   | flow=%s  job=%s  run=%s  elapsed=%dms  error=%s",
            flow, job_id, job.run_id[:8], elapsed, e,
        )
        emit_run_end_async(job, DATAHUB_ENV, success=False, error_msg=str(e))
        raise
    finally:
        clear_job()


def datahub_job_fn(
    job_id: str,
    flow: str = "default",
    platform: str = "pythonSdk",
    upstream_jobs: list[str] | None = None,
    description: str | None = None,
    flow_description: str | None = None,
) -> Callable[[F], F]:
    """
    함수 단위 lineage 수집 데코레이터.

    Args:
        job_id:        DataHub DataJob 식별자
        flow:          DataFlow 이름 (파이프라인 단위)
        platform:      플랫폼 (기본값: pythonSdk)
        upstream_jobs: 이 job이 의존하는 상위 DataJob ID 목록.
                       같은 flow 내 다른 job_id를 지정하면 DataJob 간 리니지가 그려짐.

    사용 예::

        @datahub_job_fn("step1-extract", flow="daily-etl-pipeline")
        def step1(): ...

        @datahub_job_fn("step2-transform", flow="daily-etl-pipeline",
                        upstream_jobs=["step1-extract"])
        def step2(): ...
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with datahub_job(job_id, flow=flow, platform=platform,
                             upstream_jobs=upstream_jobs,
                             description=description,
                             flow_description=flow_description):
                return func(*args, **kwargs)
        return wrapper  # type: ignore[return-value]
    return decorator
