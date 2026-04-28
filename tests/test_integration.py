"""
DataHub 실제 전송 통합 테스트.

실행 조건:
  - DataHub GMS가 실행 중이어야 함
  - DATAHUB_GMS_URL 환경변수 설정 필요 (기본값: http://localhost:8080)
  - DATAHUB_ENV 환경변수 설정 필요 (기본값: PROD)

실행 방법:
  pytest tests/test_integration.py -v -s

단위 테스트(test_ms_usage.py)와 달리 DatahubRestEmitter를 mock하지 않으므로
실제 DataHub에 lineage가 전송됩니다.
"""
from __future__ import annotations

import os
import time

import boto3
import pytest
from botocore.stub import Stubber

from aileron_meta_collector import datahub_job_fn, install_all_hooks
from aileron_meta_collector.context import clear_job, get_job


# ── 환경 확인 ─────────────────────────────────────────────────────────────────

DATAHUB_GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
DATAHUB_ENV     = os.environ.get("DATAHUB_ENV", "PROD")


def _datahub_reachable() -> bool:
    """DataHub GMS에 연결 가능한지 확인"""
    try:
        import requests
        resp = requests.get(f"{DATAHUB_GMS_URL}/config", timeout=3)
        return resp.status_code == 200
    except Exception:
        return False


requires_datahub = pytest.mark.skipif(
    not _datahub_reachable(),
    reason=f"DataHub GMS({DATAHUB_GMS_URL})에 연결할 수 없습니다. DataHub를 먼저 실행하세요.",
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session", autouse=True)
def install_hooks_real():
    """실제 DatahubRestEmitter를 사용하는 훅 등록 (mock 없음)"""
    install_all_hooks(env=DATAHUB_ENV)
    print(f"\n[integration] hooks installed | gms={DATAHUB_GMS_URL}  env={DATAHUB_ENV}")


@pytest.fixture(autouse=True)
def clear_context():
    clear_job()
    yield
    clear_job()


def _make_stubbed_athena(execution_id: str, sql: str, state: str = "SUCCEEDED"):
    """Stubber로 Athena HTTP만 차단 — boto3 이벤트 훅은 정상 트리거"""
    client = boto3.client("athena", region_name="ap-northeast-2")
    stubber = Stubber(client)
    stubber.add_response(
        "start_query_execution",
        {"QueryExecutionId": execution_id},
    )
    stubber.add_response(
        "get_query_execution",
        {
            "QueryExecution": {
                "QueryExecutionId": execution_id,
                "Status": {"State": state},
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


# ── 통합 테스트 ───────────────────────────────────────────────────────────────

@requires_datahub
class TestCreateViewLineageIntegration:
    """
    CREATE OR REPLACE VIEW lineage를 실제 DataHub에 전송하는 통합 테스트.
    Athena HTTP 호출만 Stubber로 막고, DataHub 전송은 실제로 수행합니다.
    """

    def test_create_or_replace_view_sends_to_datahub(self):
        """
        CREATE OR REPLACE VIEW 실행 시 DataHub에 lineage가 실제로 전송되는지 검증.

        DataHub UI에서 확인:
          Datasets → glue.sales_db.daily_summary → Lineage 탭
          → upstream: glue.sales_db.orders
        """
        sql = """
            CREATE OR REPLACE VIEW sales_db.daily_summary AS
            SELECT order_date, SUM(amount) AS total
            FROM sales_db.orders
            GROUP BY order_date
        """
        athena, stubber = _make_stubbed_athena(
            execution_id="integration-view-001",
            sql=sql,
        )
        snapshot = {}

        with stubber:
            @datahub_job_fn(
                "integration-create-view",
                flow="integration-test-pipeline",
                description="통합 테스트: CREATE OR REPLACE VIEW lineage",
            )
            def run():
                qid = athena.start_query_execution(
                    QueryString=sql,
                    QueryExecutionContext={"Database": "sales_db"},
                    ResultConfiguration={"OutputLocation": "s3://results/"},
                )["QueryExecutionId"]
                athena.get_query_execution(QueryExecutionId=qid)
                snapshot["job"] = get_job()

            run()

        time.sleep(0.5)  # 비동기 emit 완료 대기

        job = snapshot["job"]
        print(f"\n[integration] job.inputs  : {job.inputs}")
        print(f"[integration] job.outputs : {job.outputs}")
        print(f"\n[integration] DataHub UI에서 확인:")
        print(f"  → {DATAHUB_GMS_URL}")
        print(f"  → Datasets > glue > sales_db.daily_summary > Lineage")

        assert any("sales_db.orders" in u for u in job.inputs)
        assert any("sales_db.daily_summary" in u for u in job.outputs)

    def test_create_view_with_join_sends_to_datahub(self):
        """
        JOIN이 포함된 CREATE OR REPLACE VIEW — 복수 input이 DataHub에 전송되는지 검증.

        DataHub UI에서 확인:
          Datasets → glue.analytics.report → Lineage 탭
          → upstream: glue.analytics.orders, glue.analytics.customers
        """
        sql = """
            CREATE OR REPLACE VIEW analytics.report AS
            SELECT o.id, c.name
            FROM orders o
            LEFT JOIN customers c ON o.cid = c.id
        """
        athena, stubber = _make_stubbed_athena(
            execution_id="integration-view-002",
            sql=sql,
        )
        snapshot = {}

        with stubber:
            @datahub_job_fn(
                "integration-join-view",
                flow="integration-test-pipeline",
                description="통합 테스트: CREATE OR REPLACE VIEW + JOIN",
            )
            def run():
                qid = athena.start_query_execution(
                    QueryString=sql,
                    QueryExecutionContext={"Database": "analytics"},
                    ResultConfiguration={"OutputLocation": "s3://results/"},
                )["QueryExecutionId"]
                athena.get_query_execution(QueryExecutionId=qid)
                snapshot["job"] = get_job()

            run()

        time.sleep(0.5)

        job = snapshot["job"]
        print(f"\n[integration] job.inputs  : {job.inputs}")
        print(f"[integration] job.outputs : {job.outputs}")
        print(f"\n[integration] DataHub UI에서 확인:")
        print(f"  → {DATAHUB_GMS_URL}")
        print(f"  → Datasets > glue > analytics.report > Lineage")

        assert any("orders" in u for u in job.inputs)
        assert any("customers" in u for u in job.inputs)
        assert any("analytics.report" in u for u in job.outputs)
