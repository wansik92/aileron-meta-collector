from __future__ import annotations

import functools
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Callable, Generator, Optional, TypeVar

F = TypeVar("F", bound=Callable)

_local = threading.local()


@dataclass
class JobContext:
    job_id: str
    flow: str = "default"
    platform: str = "pythonSdk"
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    start_time_ms: int = field(default_factory=lambda: int(time.time() * 1000))


def set_job(job_id: str, flow: str = "default", platform: str = "pythonSdk") -> None:
    _local.job = JobContext(job_id=job_id, flow=flow, platform=platform)


def get_job() -> Optional[JobContext]:
    return getattr(_local, "job", None)


def clear_job() -> None:
    _local.job = None


@contextmanager
def datahub_job(
    job_id: str,
    flow: str = "default",
    platform: str = "pythonSdk",
) -> Generator[JobContext, None, None]:
    from .config import DATAHUB_ENV
    from .emitter import (
        emit_dataflow_async,
        emit_datajob_async,
        emit_run_end_async,
        emit_run_start_async,
    )

    set_job(job_id, flow, platform)
    job = get_job()

    emit_dataflow_async(job, DATAHUB_ENV)
    emit_datajob_async(job, DATAHUB_ENV)
    emit_run_start_async(job, DATAHUB_ENV)

    try:
        yield job
        emit_run_end_async(job, DATAHUB_ENV, success=True)
    except Exception as e:
        emit_run_end_async(job, DATAHUB_ENV, success=False, error_msg=str(e))
        raise
    finally:
        clear_job()


def datahub_job_fn(
    job_id: str,
    flow: str = "default",
    platform: str = "pythonSdk",
) -> Callable[[F], F]:
    """
    함수 단위 lineage 수집 데코레이터.

    사용 예::

        @datahub_job_fn("process-orders", flow="daily-etl-pipeline")
        def process_orders():
            df = pd.read_sql(...)
            qid = athena.start_query_execution(...)
            wait_for_query(qid)
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with datahub_job(job_id, flow=flow, platform=platform):
                return func(*args, **kwargs)
        return wrapper  # type: ignore[return-value]
    return decorator
