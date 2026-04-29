"""
E-commerce ETL 파이프라인 — 지원하는 모든 Athena 쿼리 타입을 실제 DataHub로 전송하는 테스트.

파이프라인 구조:
  raw.orders ──────────────────────────────────────────────────┐
  raw.products ────────────────────────────────────────────┐   │
  raw.customers ───────────────────────────────────────┐   │   │
                                                       │   │   │
  [Step 1] INSERT INTO          raw.orders             │   │   │
                             → staging.orders          │   │   │
                                                       │   │   │
  [Step 2] CTAS                 staging.orders         │   │   │
                              + raw.products  ─────────┘   │   │
                             → dw.order_items               │   │
                                                            │   │
  [Step 3] CREATE VIEW          dw.order_items              │   │
                              + raw.customers ──────────────┘   │
                             → analytics.customer_orders         │
                                                                 │
  [Step 4] CREATE OR REPLACE VIEW  dw.order_items ──────────────┘
                                → analytics.daily_summary

  [Step 5] CREATE TEMP VIEW  analytics.customer_orders
                           + analytics.daily_summary
                          → tmp_top_customers

  [Step 6] UNLOAD            analytics.daily_summary
                          → s3://dw-export/reports/daily/

  [Step 7] UNLOAD ×2 (병렬)  analytics.customer_orders → s3://dw-export/customer-orders/
            propagate_job    analytics.daily_summary   → s3://dw-export/daily-summary/

실행 조건:
  - DataHub GMS가 실행 중이어야 함
  - DATAHUB_GMS_URL 환경변수 설정 필요 (기본값: http://localhost:8080)
  - DATAHUB_ENV    환경변수 설정 필요 (기본값: PROD)

실행 방법:
  pytest tests/test_pipeline.py -v -s
"""
from __future__ import annotations

import os
from typing import Any

import boto3
import pytest
from botocore.stub import Stubber

from concurrent.futures import ThreadPoolExecutor

from aileron_meta_collector import datahub_job_fn, install_all_hooks
from aileron_meta_collector.context import clear_job, datahub_job, get_job, propagate_job
from aileron_meta_collector.emitter import flush_emit


# ── 환경 설정 ──────────────────────────────────────────────────────────────────

DATAHUB_GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
DATAHUB_ENV     = os.environ.get("DATAHUB_ENV", "PROD")


def _datahub_reachable() -> bool:
    try:
        import requests
        return requests.get(f"{DATAHUB_GMS_URL}/config", timeout=3).status_code == 200
    except Exception:
        return False


requires_datahub = pytest.mark.skipif(
    not _datahub_reachable(),
    reason=f"DataHub GMS({DATAHUB_GMS_URL})에 연결할 수 없습니다. DataHub를 먼저 실행하세요.",
)


# ── SQL 상수 ──────────────────────────────────────────────────────────────────

# Step 1: INSERT INTO — raw 레이어에서 staging 적재
SQL_INSERT_INTO = """
    INSERT INTO staging.orders
    SELECT order_id, customer_id, product_id, amount, created_at
    FROM raw.orders
    WHERE created_at >= DATE_ADD('day', -1, CURRENT_DATE)
      AND status != 'cancelled'
"""

# Step 2: CTAS — staging + 상품 정보 결합하여 DW 테이블 생성
SQL_CTAS = """
    CREATE TABLE dw.order_items AS
    SELECT
        o.order_id,
        o.customer_id,
        p.name        AS product_name,
        p.category,
        o.amount,
        o.created_at
    FROM staging.orders o
    JOIN raw.products p ON o.product_id = p.product_id
"""

# Step 3: CREATE VIEW — 고객별 주문 집계 뷰
SQL_CREATE_VIEW = """
    CREATE VIEW analytics.customer_orders AS
    SELECT
        c.customer_id,
        c.name,
        c.email,
        COUNT(o.order_id)  AS order_count,
        SUM(o.amount)      AS total_amount
    FROM dw.order_items o
    JOIN raw.customers c ON o.customer_id = c.customer_id
    GROUP BY c.customer_id, c.name, c.email
"""

# Step 4: CREATE OR REPLACE VIEW — 일별 카테고리 매출 요약 뷰 (갱신)
SQL_CREATE_OR_REPLACE_VIEW = """
    CREATE OR REPLACE VIEW analytics.daily_summary AS
    SELECT
        DATE(created_at)  AS order_date,
        category,
        COUNT(order_id)   AS order_count,
        SUM(amount)       AS revenue
    FROM dw.order_items
    GROUP BY DATE(created_at), category
"""

# Step 5: CREATE TEMP VIEW — 당일 상위 고객 임시 분석 뷰
SQL_CREATE_TEMP_VIEW = """
    CREATE TEMP VIEW tmp_top_customers AS
    SELECT
        co.customer_id,
        co.name,
        co.total_amount,
        ds.category,
        ds.revenue AS category_revenue
    FROM analytics.customer_orders co
    JOIN analytics.daily_summary ds ON ds.order_date = CURRENT_DATE
    ORDER BY co.total_amount DESC
    LIMIT 100
"""

# Step 6: UNLOAD TO S3 — analytics 결과를 S3로 export
SQL_UNLOAD = """
    UNLOAD (
        SELECT order_date, category, order_count, revenue
        FROM analytics.daily_summary
        WHERE order_date >= DATE_ADD('day', -7, CURRENT_DATE)
    )
    TO 's3://dw-export/reports/daily/'
    WITH (format = 'PARQUET', compression = 'SNAPPY')
"""

# Step 7: 병렬 UNLOAD — 분석 결과를 S3에 동시 export (propagate_job 사용)
SQL_UNLOAD_CUSTOMER_ORDERS = """
    UNLOAD (
        SELECT customer_id, name, email, order_count, total_amount
        FROM analytics.customer_orders
    )
    TO 's3://dw-export/customer-orders/'
    WITH (format = 'PARQUET', compression = 'SNAPPY')
"""

SQL_UNLOAD_DAILY_SUMMARY = """
    UNLOAD (
        SELECT order_date, category, order_count, revenue
        FROM analytics.daily_summary
    )
    TO 's3://dw-export/daily-summary/'
    WITH (format = 'PARQUET', compression = 'SNAPPY')
"""

# 파이프라인 스텝 정의: (job_name, sql, exec_id, database)
PIPELINE_STEPS = [
    ("step1-ingest-orders",          SQL_INSERT_INTO,            "pipe-001", "staging"),
    ("step2-build-order-items",      SQL_CTAS,                   "pipe-002", "dw"),
    ("step3-create-customer-view",   SQL_CREATE_VIEW,            "pipe-003", "analytics"),
    ("step4-update-daily-summary",   SQL_CREATE_OR_REPLACE_VIEW, "pipe-004", "analytics"),
    ("step5-top-customers-temp",     SQL_CREATE_TEMP_VIEW,       "pipe-005", "analytics"),
    ("step6-export-to-s3",           SQL_UNLOAD,                 "pipe-006", "analytics"),
]


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session", autouse=True)
def install_hooks():
    """실제 DatahubRestEmitter를 사용하는 훅 등록"""
    install_all_hooks(env=DATAHUB_ENV)
    print(f"\n[pipeline] hooks installed | gms={DATAHUB_GMS_URL}  env={DATAHUB_ENV}")


@pytest.fixture(autouse=True)
def clear_job_context():
    clear_job()
    yield
    clear_job()


# ── 공통 헬퍼 ─────────────────────────────────────────────────────────────────

def _make_athena(exec_id: str, sql: str) -> tuple:
    """Stubber로 감싼 실제 boto3 Athena 클라이언트 반환.
    HTTP 계층만 차단하므로 before-parameter-build / after-call 이벤트 훅은 정상 동작.
    """
    client = boto3.client("athena", region_name="ap-northeast-2")
    stubber = Stubber(client)
    stubber.add_response(
        "start_query_execution",
        {"QueryExecutionId": exec_id},
    )
    stubber.add_response(
        "get_query_execution",
        {
            "QueryExecution": {
                "QueryExecutionId": exec_id,
                "Status": {"State": "SUCCEEDED"},
                "Query": sql,
                "StatementType": "DDL",
                "ResultConfiguration": {"OutputLocation": "s3://results/"},
                "QueryExecutionContext": {},
                "Statistics": {},
                "WorkGroup": "primary",
                "EngineVersion": {"SelectedEngineVersion": "AUTO"},
            }
        },
    )
    return client, stubber


def _run_step(
    job_name: str,
    sql: str,
    exec_id: str,
    database: str,
    flow: str = "ecommerce-pipeline",
) -> Any:
    """파이프라인 스텝 1개를 실행하고 job snapshot을 반환."""
    athena, stubber = _make_athena(exec_id, sql)
    snapshot = {}

    with stubber:
        @datahub_job_fn(job_name, flow=flow)
        def step():
            qid = athena.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Database": database},
                ResultConfiguration={"OutputLocation": "s3://results/"},
            )["QueryExecutionId"]
            athena.get_query_execution(QueryExecutionId=qid)
            snapshot["job"] = get_job()

        step()

    return snapshot["job"]


def _run_parallel_step(
    job_name: str,
    tasks: list[tuple[str, str, str]],  # [(exec_id, sql, database), ...]
    flow: str = "ecommerce-pipeline",
) -> Any:
    """여러 Athena 쿼리를 ThreadPoolExecutor로 병렬 실행하는 파이프라인 스텝.
    propagate_job으로 worker 스레드에 job context를 전파합니다.
    """
    clients = [_make_athena(exec_id, sql) for exec_id, sql, _ in tasks]
    snapshot = {}

    with datahub_job(job_name, flow=flow) as job:
        snapshot["job"] = job

        @propagate_job
        def task(athena, stubber, sql, database):
            with stubber:
                qid = athena.start_query_execution(
                    QueryString=sql,
                    QueryExecutionContext={"Database": database},
                    ResultConfiguration={"OutputLocation": "s3://results/"},
                )["QueryExecutionId"]
                athena.get_query_execution(QueryExecutionId=qid)

        with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            futures = [
                executor.submit(task, athena, stubber, sql, db)
                for (athena, stubber), (_, sql, db) in zip(clients, tasks)
            ]
            for f in futures:
                f.result()

    return snapshot["job"]


def _assert_lineage(result: dict, inputs: list[str], outputs: list[str]) -> None:
    job_name       = result["job_name"]
    actual_inputs  = result["inputs"]
    actual_outputs = result["outputs"]

    for expected in inputs:
        assert any(expected in u for u in actual_inputs), \
            f"[{job_name}] '{expected}'가 inputs에 없음: {actual_inputs}"
    for expected in outputs:
        assert any(expected in u for u in actual_outputs), \
            f"[{job_name}] '{expected}'가 outputs에 없음: {actual_outputs}"


# ── 파이프라인 테스트 ──────────────────────────────────────────────────────────

@requires_datahub
class TestEcommercePipeline:
    """
    E-commerce ETL 파이프라인의 모든 스텝을 실제 DataHub로 전송합니다.
    Athena HTTP 호출만 Stubber로 차단하고, DataHub 전송은 실제로 수행합니다.

    DataHub UI 확인 경로:
      Pipelines → ecommerce-pipeline → 각 DataJob의 Lineage 탭
    """

    def test_step1_insert_into(self):
        """INSERT INTO: raw.orders → staging.orders"""
        job = _run_step(*PIPELINE_STEPS[0])
        flush_emit()

        print(f"\n[step1] inputs : {job.inputs}")
        print(f"[step1] outputs: {job.outputs}")

        _assert_lineage(
            {"job_name": "step1", "inputs": job.inputs, "outputs": job.outputs},
            inputs=["raw.orders"],
            outputs=["staging.orders"],
        )

    def test_step2_ctas(self):
        """CTAS: staging.orders + raw.products → dw.order_items"""
        job = _run_step(*PIPELINE_STEPS[1])
        flush_emit()

        print(f"\n[step2] inputs : {job.inputs}")
        print(f"[step2] outputs: {job.outputs}")

        _assert_lineage(
            {"job_name": "step2", "inputs": job.inputs, "outputs": job.outputs},
            inputs=["staging.orders", "raw.products"],
            outputs=["dw.order_items"],
        )

    def test_step3_create_view(self):
        """CREATE VIEW: dw.order_items + raw.customers → analytics.customer_orders"""
        job = _run_step(*PIPELINE_STEPS[2])
        flush_emit()

        print(f"\n[step3] inputs : {job.inputs}")
        print(f"[step3] outputs: {job.outputs}")

        _assert_lineage(
            {"job_name": "step3", "inputs": job.inputs, "outputs": job.outputs},
            inputs=["dw.order_items", "raw.customers"],
            outputs=["analytics.customer_orders"],
        )

    def test_step4_create_or_replace_view(self):
        """CREATE OR REPLACE VIEW: dw.order_items → analytics.daily_summary"""
        job = _run_step(*PIPELINE_STEPS[3])
        flush_emit()

        print(f"\n[step4] inputs : {job.inputs}")
        print(f"[step4] outputs: {job.outputs}")

        _assert_lineage(
            {"job_name": "step4", "inputs": job.inputs, "outputs": job.outputs},
            inputs=["dw.order_items"],
            outputs=["analytics.daily_summary"],
        )

    def test_step5_create_temp_view(self):
        """CREATE TEMP VIEW: customer_orders + daily_summary → tmp_top_customers"""
        job = _run_step(*PIPELINE_STEPS[4])
        flush_emit()

        print(f"\n[step5] inputs : {job.inputs}")
        print(f"[step5] outputs: {job.outputs}")

        _assert_lineage(
            {"job_name": "step5", "inputs": job.inputs, "outputs": job.outputs},
            inputs=["analytics.customer_orders", "analytics.daily_summary"],
            outputs=["tmp_top_customers"],
        )

    def test_step6_unload_to_s3(self):
        """UNLOAD: analytics.daily_summary → s3://dw-export/reports/daily/"""
        job = _run_step(*PIPELINE_STEPS[5])
        flush_emit()

        print(f"\n[step6] inputs : {job.inputs}")
        print(f"[step6] outputs: {job.outputs}")

        _assert_lineage(
            {"job_name": "step6", "inputs": job.inputs, "outputs": job.outputs},
            inputs=["analytics.daily_summary"],
            outputs=["dw-export"],
        )

    def test_step7_parallel_export(self):
        """UNLOAD ×2 병렬 (propagate_job):
        analytics.customer_orders → s3://dw-export/customer-orders/
        analytics.daily_summary   → s3://dw-export/daily-summary/

        DataHub UI 확인:
          Pipelines → ecommerce-pipeline → step7-parallel-export → Lineage 탭
          inputs : analytics.customer_orders, analytics.daily_summary
          outputs: s3://dw-export/customer-orders/, s3://dw-export/daily-summary/
        """
        job = _run_parallel_step(
            "step7-parallel-export",
            tasks=[
                ("parallel-exp-001", SQL_UNLOAD_CUSTOMER_ORDERS, "analytics"),
                ("parallel-exp-002", SQL_UNLOAD_DAILY_SUMMARY,   "analytics"),
            ],
        )
        flush_emit()

        print(f"\n[step7] inputs : {job.inputs}")
        print(f"[step7] outputs: {job.outputs}")

        _assert_lineage(
            {"job_name": "step7", "inputs": job.inputs, "outputs": job.outputs},
            inputs=["analytics.customer_orders", "analytics.daily_summary"],
            outputs=["dw-export/customer-orders", "dw-export/daily-summary"],
        )

    def test_full_pipeline(self):
        """
        7개 스텝 전체를 순서대로 실행하여 DataHub에 전송.
        Step 7은 propagate_job으로 병렬 실행합니다.

        DataHub UI에서 확인:
          Pipelines → ecommerce-pipeline
            step1-ingest-orders        raw.orders → staging.orders
            step2-build-order-items    staging.orders + raw.products → dw.order_items
            step3-create-customer-view dw.order_items + raw.customers → analytics.customer_orders
            step4-update-daily-summary dw.order_items → analytics.daily_summary
            step5-top-customers-temp   customer_orders + daily_summary → tmp_top_customers
            step6-export-to-s3         analytics.daily_summary → s3://dw-export/reports/daily/
            step7-parallel-export      customer_orders + daily_summary → s3 ×2 (병렬)
        """
        results = []

        # Step 1~6: 순차 실행
        for job_name, sql, exec_id, database in PIPELINE_STEPS:
            job = _run_step(job_name, sql, "full-" + exec_id, database)
            results.append({
                "job_name": job_name,
                "inputs":   list(job.inputs),
                "outputs":  list(job.outputs),
            })

        # Step 7: 병렬 실행 (propagate_job)
        job7 = _run_parallel_step(
            "step7-parallel-export",
            tasks=[
                ("full-parallel-exp-001", SQL_UNLOAD_CUSTOMER_ORDERS, "analytics"),
                ("full-parallel-exp-002", SQL_UNLOAD_DAILY_SUMMARY,   "analytics"),
            ],
        )
        results.append({
            "job_name": "step7-parallel-export",
            "inputs":   list(job7.inputs),
            "outputs":  list(job7.outputs),
        })

        flush_emit()

        print(f"\n[full-pipeline] DataHub UI: {DATAHUB_GMS_URL}")
        print("[full-pipeline] Pipelines → ecommerce-pipeline")
        for r in results:
            print(f"  {r['job_name']}")
            print(f"    inputs : {r['inputs']}")
            print(f"    outputs: {r['outputs']}")

        _assert_lineage(results[0], inputs=["raw.orders"],                                           outputs=["staging.orders"])
        _assert_lineage(results[1], inputs=["staging.orders", "raw.products"],                       outputs=["dw.order_items"])
        _assert_lineage(results[2], inputs=["dw.order_items", "raw.customers"],                      outputs=["analytics.customer_orders"])
        _assert_lineage(results[3], inputs=["dw.order_items"],                                       outputs=["analytics.daily_summary"])
        _assert_lineage(results[4], inputs=["analytics.customer_orders", "analytics.daily_summary"], outputs=["tmp_top_customers"])
        _assert_lineage(results[5], inputs=["analytics.daily_summary"],                              outputs=["dw-export"])
        _assert_lineage(results[6], inputs=["analytics.customer_orders", "analytics.daily_summary"], outputs=["dw-export/customer-orders", "dw-export/daily-summary"])
