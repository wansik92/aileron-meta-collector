from __future__ import annotations

import functools
import logging
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generator, Optional, TypeVar

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
    patch: bool = False,
    airflow_context: Optional[Dict[str, Any]] = None,
) -> Generator[JobContext, None, None]:
    from .config import is_datahub_enabled, get_env

    # DATAHUB_ENABLED=false(기본값) 이면 emit 없이 그냥 통과 — 비즈니스 로직에 영향 없음
    if not is_datahub_enabled():
        yield JobContext(job_id, flow, platform, upstream_jobs or [], description or "", flow_description or "")
        return

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

    emit_dataflow_async(job, get_env())
    emit_datajob_async(job, get_env())
    emit_run_start_async(job, get_env())

    try:
        yield job
        elapsed = int(time.time() * 1000) - job.start_time_ms
        logger.info(
            "[aileron] job finished | flow=%s  job=%s  run=%s  elapsed=%dms  inputs=%d  outputs=%d",
            flow, job_id, job.run_id[:8], elapsed, len(job.inputs), len(job.outputs),
        )
        logger.debug("[aileron] inputs  : %s", job.inputs)
        logger.debug("[aileron] outputs : %s", job.outputs)
        emit_run_end_async(job, get_env(), success=True, patch=patch)
    except Exception as e:
        elapsed = int(time.time() * 1000) - job.start_time_ms
        logger.warning(
            "[aileron] job failed   | flow=%s  job=%s  run=%s  elapsed=%dms  error=%s",
            flow, job_id, job.run_id[:8], elapsed, e,
        )
        emit_run_end_async(job, get_env(), success=False, error_msg=str(e), patch=patch)
        raise
    finally:
        # Airflow task inlets/outlets 자동 주입
        if airflow_context:
            from .airflow_inject import inject_to_airflow
            inject_to_airflow(job, airflow_context)
        clear_job()


def propagate_job(fn: F) -> F:
    """현재 스레드의 job context를 worker 스레드로 전파합니다.

    ThreadPoolExecutor 등에서 병렬로 Athena 작업을 수행할 때
    lineage를 캡처하려면 submit할 함수를 이 데코레이터로 감싸야 합니다.

    ``propagate_job``은 **호출 시점(부모 스레드)**에서 job을 캡처하므로
    반드시 ``@datahub_job_fn`` 또는 ``datahub_job`` context 안에서 호출해야 합니다.

    사용 예::

        @datahub_job_fn("parallel-etl", flow="pipeline")
        def run():
            @propagate_job
            def task(sql):
                athena.start_query_execution(QueryString=sql, ...)
                athena.get_query_execution(...)

            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(task, sql) for sql in sqls]
                for f in futures:
                    f.result()
    """
    job = get_job()  # 부모 스레드에서 캡처

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        _local.job = job  # worker 스레드에 job 설정
        try:
            return fn(*args, **kwargs)
        finally:
            _local.job = None  # worker 스레드 정리

    return wrapper  # type: ignore[return-value]


def datahub_job_fn(
    job_id: str,
    flow: str = "default",
    platform: str = "pythonSdk",
    upstream_jobs: list[str] | None = None,
    description: str | None = None,
    flow_description: str | None = None,
    patch: bool = False,
    airflow_context_kwarg: Optional[str] = None,
) -> Callable[[F], F]:
    """
    함수 단위 lineage 수집 데코레이터.

    Args:
        job_id:                  DataHub DataJob 식별자
        flow:                    DataFlow 이름 (파이프라인 단위)
        platform:                플랫폼 (기본값: pythonSdk)
        upstream_jobs:           이 job이 의존하는 상위 DataJob ID 목록.
        patch:                   True이면 patch MCP로 emit.
        airflow_context_kwarg:   Airflow context가 담긴 kwargs 키 이름.
                                 지정 시 해당 context로 task._inlets/_outlets 자동 주입.
                                 예) airflow_context_kwarg="context" → kwargs["context"] 사용

    사용 예::

        @datahub_job_fn("step1-extract", flow="daily-etl-pipeline")
        def step1(): ...

        # Airflow PythonOperator에서 자동 주입
        @datahub_job_fn("my-etl", flow="pipeline", airflow_context_kwarg="context")
        def my_etl(**context): ...
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # airflow_context_kwarg 지정 시 해당 kwargs에서 context 추출
            af_context = kwargs.get(airflow_context_kwarg) if airflow_context_kwarg else None

            with datahub_job(job_id, flow=flow, platform=platform,
                             upstream_jobs=upstream_jobs,
                             description=description,
                             flow_description=flow_description,
                             patch=patch,
                             airflow_context=af_context):
                return func(*args, **kwargs)
        return wrapper  # type: ignore[return-value]
    return decorator
