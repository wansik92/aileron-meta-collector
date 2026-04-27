"""
수동 lineage 주입 API.

자동 훅(SQLAlchemy / boto3)이 감지하지 못하는 경우,
또는 사용자가 명시적으로 lineage를 지정하고 싶을 때 사용합니다.

사용 예::

    from aileron_meta_collector import add_input, add_output, emit_lineage

    # 1) job context 안에서 dataset 추가 (훅 자동 emit과 병행)
    @datahub_job_fn("my-job", flow="pipeline")
    def run():
        add_input("sales_db.orders", platform="glue")
        add_output("mybucket/export/dt=2024-01-15", platform="s3")

    # 2) 직접 URN 지정
    @datahub_job_fn("my-job", flow="pipeline")
    def run():
        add_input(urn="urn:li:dataset:(urn:li:dataPlatform:glue,sales_db.orders,PROD)")

    # 3) job context 없이 즉시 emit (one-shot 리니지)
    emit_lineage(
        inputs=["sales_db.raw_events"],
        outputs=["sales_db.events_daily"],
        platform="glue",
    )
"""
from __future__ import annotations

import logging

from .config import DATAHUB_ENV

logger = logging.getLogger(__name__)


# ── URN 빌더 ─────────────────────────────────────────────────────────────────

def build_dataset_urn(
    table: str,
    platform: str,
    env: str | None = None,
) -> str:
    """
    table + platform 조합으로 DataHub dataset URN을 생성합니다.

    Args:
        table:    테이블명 또는 S3 경로.
                  - DB : "database.table"  또는 "table"
                  - S3 : "bucket/prefix"   또는 "s3://bucket/prefix" (s3:// 자동 제거)
        platform: DataHub 플랫폼명. 예: "glue", "postgres", "mysql", "s3", "snowflake"
        env:      DataHub 환경. None이면 DATAHUB_ENV 환경변수 사용.

    Returns:
        예) "urn:li:dataset:(urn:li:dataPlatform:glue,sales_db.orders,PROD)"

    Examples::

        build_dataset_urn("sales_db.orders", "glue")
        # → "urn:li:dataset:(urn:li:dataPlatform:glue,sales_db.orders,PROD)"

        build_dataset_urn("s3://my-bucket/data/events", "s3")
        # → "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data/events,PROD)"

        build_dataset_urn("public.users", "postgres", env="DEV")
        # → "urn:li:dataset:(urn:li:dataPlatform:postgres,public.users,DEV)"
    """
    _env = env or DATAHUB_ENV
    # s3:// prefix 제거
    name = table.replace("s3://", "").rstrip("/") if platform == "s3" else table
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{_env})"


# ── Job context 내 수동 주입 ──────────────────────────────────────────────────

def add_input(
    table: str | None = None,
    *,
    platform: str | None = None,
    urn: str | None = None,
    env: str | None = None,
) -> None:
    """
    현재 job context에 input dataset을 수동으로 추가합니다.
    @datahub_job_fn 데코레이터 안에서 호출해야 합니다.

    Args:
        table:    테이블명 또는 S3 경로 (platform과 함께 사용)
        platform: DataHub 플랫폼명 (table과 함께 사용)
        urn:      직접 지정할 DataHub URN (table/platform 대신 사용)
        env:      DataHub 환경 (None이면 DATAHUB_ENV 사용)

    Raises:
        ValueError: job context가 없거나 인자가 부족한 경우

    Examples::

        add_input("sales_db.orders", platform="glue")
        add_input("mybucket/raw/events", platform="s3")
        add_input(urn="urn:li:dataset:(urn:li:dataPlatform:glue,sales_db.orders,PROD)")
    """
    from .context import get_job

    job = get_job()
    if job is None:
        raise ValueError(
            "add_input()은 @datahub_job_fn 또는 datahub_job() context 안에서 호출해야 합니다."
        )

    resolved = _resolve_urn(table, platform, urn, env)
    if resolved not in job.inputs:
        job.inputs.append(resolved)
        logger.debug("manual input added: %s", resolved)


def add_output(
    table: str | None = None,
    *,
    platform: str | None = None,
    urn: str | None = None,
    env: str | None = None,
) -> None:
    """
    현재 job context에 output dataset을 수동으로 추가합니다.
    @datahub_job_fn 데코레이터 안에서 호출해야 합니다.

    Args:
        table:    테이블명 또는 S3 경로 (platform과 함께 사용)
        platform: DataHub 플랫폼명 (table과 함께 사용)
        urn:      직접 지정할 DataHub URN (table/platform 대신 사용)
        env:      DataHub 환경 (None이면 DATAHUB_ENV 사용)

    Examples::

        add_output("sales_db.order_summary", platform="glue")
        add_output("mybucket/processed/output", platform="s3")
        add_output(urn="urn:li:dataset:(urn:li:dataPlatform:s3,mybucket/out,PROD)")
    """
    from .context import get_job

    job = get_job()
    if job is None:
        raise ValueError(
            "add_output()은 @datahub_job_fn 또는 datahub_job() context 안에서 호출해야 합니다."
        )

    resolved = _resolve_urn(table, platform, urn, env)
    if resolved not in job.outputs:
        job.outputs.append(resolved)
        logger.debug("manual output added: %s", resolved)


# ── Job context 없이 즉시 emit ────────────────────────────────────────────────

def emit_lineage(
    inputs: list[str],
    outputs: list[str],
    platform: str | None = None,
    env: str | None = None,
) -> None:
    """
    job context 없이 dataset 간 lineage를 즉시 DataHub에 전송합니다.

    inputs / outputs 각 항목은:
    - "database.table" 형식 + platform 지정 → URN 자동 생성
    - "urn:li:dataset:(...)" 형식 → 그대로 사용

    Args:
        inputs:   input dataset 목록 (테이블명 또는 URN)
        outputs:  output dataset 목록 (테이블명 또는 URN)
        platform: 테이블명 방식 사용 시 DataHub 플랫폼명 (e.g. "glue", "postgres")
        env:      DataHub 환경 (None이면 DATAHUB_ENV 사용)

    Examples::

        # 테이블명 + platform 방식
        emit_lineage(
            inputs=["sales_db.orders", "sales_db.customers"],
            outputs=["sales_db.order_summary"],
            platform="glue",
        )

        # URN 직접 지정 방식
        emit_lineage(
            inputs=["urn:li:dataset:(urn:li:dataPlatform:glue,sales_db.orders,PROD)"],
            outputs=["urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/out,PROD)"],
        )

        # 혼합 방식
        emit_lineage(
            inputs=["urn:li:dataset:(urn:li:dataPlatform:glue,sales_db.orders,PROD)"],
            outputs=["my-bucket/result"],
            platform="s3",
        )
    """
    from .emitter import emit_lineage_async
    from .context import JobContext

    _env = env or DATAHUB_ENV

    def _to_urn(item: str) -> str:
        if item.startswith("urn:li:"):
            return item
        if platform is None:
            raise ValueError(
                f"URN 형식이 아닌 테이블명 '{item}'을 사용하려면 platform을 지정해야 합니다."
            )
        return build_dataset_urn(item, platform, _env)

    input_urns  = [_to_urn(t) for t in inputs]
    output_urns = [_to_urn(t) for t in outputs]

    # emit_lineage_async는 JobContext가 필요하므로 임시 context 생성
    dummy_job = JobContext(job_id="__manual__", flow="__manual__")
    emit_lineage_async(dummy_job, input_urns, output_urns)
    logger.debug("manual lineage emitted: %s → %s", input_urns, output_urns)


# ── 내부 유틸 ─────────────────────────────────────────────────────────────────

def _resolve_urn(
    table: str | None,
    platform: str | None,
    urn: str | None,
    env: str | None,
) -> str:
    if urn:
        return urn
    if table and platform:
        return build_dataset_urn(table, platform, env)
    raise ValueError(
        "table + platform 조합 또는 urn 중 하나를 반드시 지정해야 합니다.\n"
        "  예) add_input('sales_db.orders', platform='glue')\n"
        "  예) add_input(urn='urn:li:dataset:(...)')"
    )
