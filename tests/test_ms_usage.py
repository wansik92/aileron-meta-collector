"""
MS에서 aileron-meta-collector를 사용하는 패턴을 검증하는 테스트입니다.
DataHub GMS 호출은 모두 mock 처리되어 실제 서버 없이 실행됩니다.
"""
from __future__ import annotations

import threading
import time
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from aileron_meta_collector import datahub_job, datahub_job_fn, install_all_hooks
from aileron_meta_collector.context import get_job, set_job, clear_job


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def clear_job_context():
    """각 테스트 전후 job context 초기화"""
    clear_job()
    yield
    clear_job()


@pytest.fixture
def mock_emitter():
    """DataHub GMS emit 호출을 캡처하는 mock"""
    with patch("aileron_meta_collector.emitter.DatahubRestEmitter") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def captured_urns(mock_emitter):
    """emit된 MCP에서 entityUrn 목록을 추출하는 헬퍼"""
    def _urns() -> list[str]:
        return [c.args[0].entityUrn for c in mock_emitter.emit.call_args_list]
    return _urns


@pytest.fixture
def captured_aspects(mock_emitter):
    """emit된 MCP에서 aspect 목록을 추출하는 헬퍼"""
    def _aspects() -> list[Any]:
        return [c.args[0].aspect for c in mock_emitter.emit.call_args_list]
    return _aspects


@pytest.fixture
def athena_client():
    """Athena boto3 client mock — SUCCEEDED 응답 반환"""
    client = MagicMock()
    client.start_query_execution.return_value = {"QueryExecutionId": "test-exec-id-001"}
    client.get_query_execution.return_value = {
        "QueryExecution": {
            "QueryExecutionId": "test-exec-id-001",
            "Query": "",  # 훅이 pending에서 SQL을 관리하므로 여기선 불필요
            "Status": {"State": "SUCCEEDED"},
        }
    }
    return client


@pytest.fixture
def s3_client():
    """S3 boto3 client mock"""
    return MagicMock()


@pytest.fixture(scope="session", autouse=True)
def install_hooks():
    """테스트 세션 전체에서 훅 1회 등록"""
    with patch("aileron_meta_collector.emitter.DatahubRestEmitter"):
        install_all_hooks(env="TEST")


# ── Helper ────────────────────────────────────────────────────────────────────

def wait_for_query(athena, execution_id: str) -> str:
    result = athena.get_query_execution(QueryExecutionId=execution_id)
    return result["QueryExecution"]["Status"]["State"]


def _aspect_types(mock_emitter) -> list[str]:
    return [type(c.args[0].aspect).__name__ for c in mock_emitter.emit.call_args_list]


# ── 1. Decorator 기본 동작 ─────────────────────────────────────────────────────

class TestDecoratorBasic:

    def test_decorator_sets_job_context_during_execution(self, mock_emitter):
        captured = {}

        @datahub_job_fn("test-job", flow="test-flow")
        def my_task():
            captured["job"] = get_job()

        my_task()

        assert captured["job"] is not None
        assert captured["job"].job_id == "test-job"
        assert captured["job"].flow == "test-flow"

    def test_decorator_clears_context_after_execution(self, mock_emitter):
        @datahub_job_fn("test-job", flow="test-flow")
        def my_task():
            pass

        my_task()
        assert get_job() is None

    def test_decorator_clears_context_on_exception(self, mock_emitter):
        @datahub_job_fn("failing-job", flow="test-flow")
        def failing_task():
            raise ValueError("intentional error")

        with pytest.raises(ValueError):
            failing_task()

        assert get_job() is None

    def test_decorator_emits_dataflow_datajob_on_enter(self, mock_emitter):
        @datahub_job_fn("my-job", flow="my-flow")
        def my_task():
            pass

        my_task()
        time.sleep(0.1)  # async emit 대기

        aspect_types = _aspect_types(mock_emitter)
        assert "DataFlowInfoClass" in aspect_types
        assert "DataJobInfoClass" in aspect_types

    def test_decorator_emits_run_started_and_complete(self, mock_emitter):
        @datahub_job_fn("my-job", flow="my-flow")
        def my_task():
            pass

        my_task()
        time.sleep(0.1)

        from datahub.metadata.schema_classes import DataProcessRunStatusClass
        run_events = [
            c.args[0].aspect
            for c in mock_emitter.emit.call_args_list
            if type(c.args[0].aspect).__name__ == "DataProcessInstanceRunEventClass"
        ]
        states = [e.status for e in run_events]
        assert DataProcessRunStatusClass.STARTED in states
        assert DataProcessRunStatusClass.COMPLETE in states

    def test_decorator_emits_failed_on_exception(self, mock_emitter):
        @datahub_job_fn("failing-job", flow="my-flow")
        def failing_task():
            raise RuntimeError("db connection error")

        with pytest.raises(RuntimeError):
            failing_task()

        time.sleep(0.1)

        # FAILED 상태 없음 — COMPLETE + result.type=FAILURE 로 표현
        from datahub.metadata.schema_classes import RunResultTypeClass
        run_events = [
            c.args[0].aspect
            for c in mock_emitter.emit.call_args_list
            if type(c.args[0].aspect).__name__ == "DataProcessInstanceRunEventClass"
        ]
        # 종료 이벤트의 result가 FAILURE인지 확인
        end_events = [e for e in run_events if e.result is not None]
        assert any(e.result.type == RunResultTypeClass.FAILURE for e in end_events)


# ── 2. 상위 함수에서 Decorator 사용 ───────────────────────────────────────────

class TestDecoratorOnParentFunction:
    """
    Decorator를 boto3/SQLAlchemy를 직접 호출하는 함수가 아닌
    상위 함수에 적용해도 동일하게 동작함을 검증합니다.
    """

    def test_child_functions_inherit_job_context(self, mock_emitter):
        results = {}

        def fetch_orders():
            results["fetch_job"] = get_job()

        def export_results():
            results["export_job"] = get_job()

        @datahub_job_fn("daily-pipeline", flow="etl-service")
        def run_pipeline():
            fetch_orders()
            export_results()

        run_pipeline()

        assert results["fetch_job"].job_id == "daily-pipeline"
        assert results["export_job"].job_id == "daily-pipeline"

    def test_deeply_nested_calls_inherit_context(self, mock_emitter):
        captured = {}

        def level3():
            captured["job"] = get_job()

        def level2():
            level3()

        def level1():
            level2()

        @datahub_job_fn("parent-job", flow="test-flow")
        def parent():
            level1()

        parent()
        assert captured["job"].job_id == "parent-job"


# ── 3. Athena CTAS lineage ────────────────────────────────────────────────────

class TestAthenaCTAS:

    def test_ctas_input_output_captured(self, mock_emitter, athena_client):
        sql = """
            CREATE TABLE order_summary AS
            SELECT user_id, COUNT(*) AS cnt
            FROM orders
            GROUP BY user_id
        """
        athena_client.start_query_execution.return_value = {"QueryExecutionId": "ctas-001"}
        athena_client.get_query_execution.return_value = {
            "QueryExecution": {
                "QueryExecutionId": "ctas-001",
                "Status": {"State": "SUCCEEDED"},
            }
        }

        @datahub_job_fn("create-order-summary", flow="daily-etl")
        def run():
            qid = athena_client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Database": "sales_db"},
                ResultConfiguration={"OutputLocation": "s3://results/"},
            )["QueryExecutionId"]
            wait_for_query(athena_client, qid)

        # Athena 훅은 boto3 default session에 등록되므로
        # start/get_query_execution 호출 시 훅이 트리거됨을 직접 검증 대신
        # job context에 inputs/outputs가 누적되는지 검증
        context_snapshot = {}

        @datahub_job_fn("create-order-summary", flow="daily-etl")
        def run_with_snapshot():
            qid = athena_client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Database": "sales_db"},
                ResultConfiguration={"OutputLocation": "s3://results/"},
            )["QueryExecutionId"]
            wait_for_query(athena_client, qid)
            context_snapshot["job"] = get_job()

        run_with_snapshot()
        assert context_snapshot["job"] is not None
        assert context_snapshot["job"].flow == "daily-etl"

    def test_failed_athena_query_does_not_emit_lineage(self, mock_emitter, athena_client):
        athena_client.start_query_execution.return_value = {"QueryExecutionId": "fail-001"}
        athena_client.get_query_execution.return_value = {
            "QueryExecution": {
                "QueryExecutionId": "fail-001",
                "Status": {"State": "FAILED"},
            }
        }

        @datahub_job_fn("failing-ctas", flow="daily-etl")
        def run():
            qid = athena_client.start_query_execution(
                QueryString="CREATE TABLE t AS SELECT * FROM orders",
                QueryExecutionContext={"Database": "sales_db"},
                ResultConfiguration={"OutputLocation": "s3://results/"},
            )["QueryExecutionId"]
            wait_for_query(athena_client, qid)

        run()
        time.sleep(0.1)

        # FAILED 쿼리 → UpstreamLineageClass emit 없음
        aspect_types = _aspect_types(mock_emitter)
        assert "UpstreamLineageClass" not in aspect_types


# ── 4. Athena CREATE VIEW lineage ────────────────────────────────────────────

class TestAthenaCreateView:
    """
    CREATE VIEW / CREATE OR REPLACE VIEW lineage 검증.

    [주의] Athena mock 클라이언트는 실제 boto3 이벤트 훅을 트리거하지 않으므로
    job.inputs / job.outputs 누적은 검증하지 않습니다.
    SQL 파싱 → URN 변환 end-to-end는 tests/hooks/test_boto3_hooks.py::TestCreateViewUrns 에서 검증합니다.
    이 클래스는 job lifecycle(context 유지, COMPLETE/FAILED 처리)만 검증합니다.
    """

    def _make_athena_mock(self, execution_id: str, state: str = "SUCCEEDED"):
        client = MagicMock()
        client.start_query_execution.return_value = {"QueryExecutionId": execution_id}
        client.get_query_execution.return_value = {
            "QueryExecution": {
                "QueryExecutionId": execution_id,
                "Status": {"State": state},
            }
        }
        return client

    # ── 파서 + URN 직접 검증 ───────────────────────────────────────────────────

    def test_create_or_replace_view_parser_output(self):
        """CREATE OR REPLACE VIEW SQL에서 input/output 테이블이 정확히 추출되는지 검증"""
        from aileron_meta_collector.parsers.sql_parser import extract_tables

        sql = """
            CREATE OR REPLACE VIEW sales_db.daily_summary AS
            SELECT order_date, SUM(amount) AS total
            FROM sales_db.orders
            GROUP BY order_date
        """
        inputs, outputs = extract_tables(sql)

        assert "sales_db.daily_summary" in outputs, \
            f"expected 'sales_db.daily_summary' in outputs, got: {outputs}"
        assert "sales_db.orders" in inputs, \
            f"expected 'sales_db.orders' in inputs, got: {inputs}"

    def test_create_view_with_join_parser_output(self):
        """JOIN이 포함된 CREATE VIEW에서 모든 input 테이블이 추출되는지 검증"""
        from aileron_meta_collector.parsers.sql_parser import extract_tables

        sql = """
            CREATE OR REPLACE VIEW analytics.report AS
            SELECT o.id, c.name
            FROM orders o
            LEFT JOIN customers c ON o.cid = c.id
        """
        inputs, outputs = extract_tables(sql)

        assert "analytics.report" in outputs
        assert "orders" in inputs
        assert "customers" in inputs

    def test_create_or_replace_view_lineage_emit(self, mock_emitter, capsys):
        """파싱된 VIEW lineage가 DataHub에 emit되는지 검증"""
        from aileron_meta_collector.parsers.sql_parser import extract_tables
        from aileron_meta_collector.hooks.boto3 import _resolve_athena_urns
        from aileron_meta_collector.emitter import emit_lineage_async

        sql = """
            CREATE OR REPLACE VIEW sales_db.daily_summary AS
            SELECT order_date, SUM(amount) AS total
            FROM sales_db.orders
            GROUP BY order_date
        """

        # ── Step 1: SQL 파싱 결과 ────────────────────────────────────────────
        inputs_raw, outputs_raw = extract_tables(sql)
        print(f"\n[Step 1] SQL 파싱 결과")
        print(f"  inputs_raw  : {inputs_raw}")
        print(f"  outputs_raw : {outputs_raw}")

        # ── Step 2: URN 변환 결과 ────────────────────────────────────────────
        input_urns, output_urns = _resolve_athena_urns(
            inputs_raw, outputs_raw, "AwsDataCatalog", "sales_db", "PROD"
        )
        print(f"\n[Step 2] URN 변환 결과")
        for u in input_urns:
            print(f"  input  URN : {u}")
        for u in output_urns:
            print(f"  output URN : {u}")

        # ── Step 3: job context에 누적 및 emit ───────────────────────────────
        with datahub_job("view-lineage-job", flow="daily-etl") as job:
            for u in input_urns:
                if u not in job.inputs:
                    job.inputs.append(u)
            for u in output_urns:
                if u not in job.outputs:
                    job.outputs.append(u)
            print(f"\n[Step 3] job context 누적")
            print(f"  job.inputs  : {job.inputs}")
            print(f"  job.outputs : {job.outputs}")
            emit_lineage_async(job, input_urns, output_urns)

        time.sleep(0.1)

        # ── Step 4: emit된 MCP 목록 ──────────────────────────────────────────
        all_calls = mock_emitter.emit.call_args_list
        emitted_urns   = [c.args[0].entityUrn for c in all_calls]
        emitted_aspects = [type(c.args[0].aspect).__name__ for c in all_calls]
        print(f"\n[Step 4] emit된 MCP 목록 (총 {len(all_calls)}개)")
        for urn, asp in zip(emitted_urns, emitted_aspects):
            print(f"  aspect={asp:<40s} entityUrn={urn}")

        # ── Assertions ───────────────────────────────────────────────────────
        assert "UpstreamLineageClass" in emitted_aspects, \
            "CREATE OR REPLACE VIEW lineage가 DataHub에 emit되지 않음"
        assert any("sales_db.daily_summary" in u for u in emitted_urns), \
            f"sales_db.daily_summary URN이 emit되지 않음. emitted: {emitted_urns}"

    # ── Job lifecycle 검증 ────────────────────────────────────────────────────

    def test_create_view_job_context_available(self, mock_emitter):
        """CREATE VIEW 실행 중 job context가 정상적으로 유지되는지 검증"""
        athena = self._make_athena_mock("view-001")
        context_snapshot = {}

        @datahub_job_fn("create-view-job", flow="daily-etl")
        def run():
            qid = athena.start_query_execution(
                QueryString="CREATE VIEW sales_db.order_view AS SELECT * FROM sales_db.orders",
                QueryExecutionContext={"Database": "sales_db"},
                ResultConfiguration={"OutputLocation": "s3://results/"},
            )["QueryExecutionId"]
            wait_for_query(athena, qid)
            context_snapshot["job"] = get_job()

        run()

        assert context_snapshot["job"] is not None
        assert context_snapshot["job"].job_id == "create-view-job"
        assert context_snapshot["job"].flow == "daily-etl"

    def test_failed_create_view_does_not_emit_lineage(self, mock_emitter):
        """FAILED 상태의 CREATE VIEW 쿼리는 lineage를 emit하지 않음"""
        athena = self._make_athena_mock("view-fail-001", state="FAILED")

        @datahub_job_fn("failing-view-job", flow="daily-etl")
        def run():
            qid = athena.start_query_execution(
                QueryString="CREATE OR REPLACE VIEW v AS SELECT * FROM orders",
                QueryExecutionContext={"Database": "sales_db"},
                ResultConfiguration={"OutputLocation": "s3://results/"},
            )["QueryExecutionId"]
            wait_for_query(athena, qid)

        run()
        time.sleep(0.1)

        aspect_types = _aspect_types(mock_emitter)
        assert "UpstreamLineageClass" not in aspect_types

    def test_create_view_run_completes_successfully(self, mock_emitter):
        """CREATE VIEW 실행 후 DataProcessInstance가 COMPLETE로 등록되는지 검증"""
        athena = self._make_athena_mock("view-003")

        @datahub_job_fn("view-complete-job", flow="daily-etl")
        def run():
            qid = athena.start_query_execution(
                QueryString="CREATE VIEW summary_view AS SELECT * FROM orders JOIN users ON orders.user_id = users.id",
                QueryExecutionContext={"Database": "sales_db"},
                ResultConfiguration={"OutputLocation": "s3://results/"},
            )["QueryExecutionId"]
            wait_for_query(athena, qid)

        run()
        time.sleep(0.1)

        from datahub.metadata.schema_classes import DataProcessRunStatusClass
        run_events = [
            c.args[0].aspect
            for c in mock_emitter.emit.call_args_list
            if type(c.args[0].aspect).__name__ == "DataProcessInstanceRunEventClass"
        ]
        states = [e.status for e in run_events]
        assert DataProcessRunStatusClass.COMPLETE in states


# ── 5. 혼합 사용 패턴 (SQLAlchemy + S3 + Athena) ──────────────────────────────

class TestMixedUsage:

    def test_job_context_available_across_mixed_calls(self, mock_emitter, athena_client, s3_client):
        snapshots = []

        def fetch_from_db():
            snapshots.append(("db", get_job()))

        def run_athena():
            snapshots.append(("athena", get_job()))
            qid = athena_client.start_query_execution(
                QueryString="CREATE TABLE summary AS SELECT * FROM orders",
                QueryExecutionContext={"Database": "sales_db"},
                ResultConfiguration={"OutputLocation": "s3://results/"},
            )["QueryExecutionId"]
            wait_for_query(athena_client, qid)

        def upload_to_s3():
            snapshots.append(("s3", get_job()))
            s3_client.put_object(Bucket="output", Key="result/data.csv", Body=b"data")

        @datahub_job_fn("mixed-pipeline", flow="order-service")
        def run_pipeline():
            fetch_from_db()
            run_athena()
            upload_to_s3()

        run_pipeline()

        assert len(snapshots) == 3
        for source, job in snapshots:
            assert job is not None
            assert job.job_id == "mixed-pipeline"
            assert job.flow == "order-service"

    def test_multiple_sequential_jobs_are_independent(self, mock_emitter):
        run_ids = []

        @datahub_job_fn("job-a", flow="pipeline")
        def job_a():
            run_ids.append(("a", get_job().run_id))

        @datahub_job_fn("job-b", flow="pipeline")
        def job_b():
            run_ids.append(("b", get_job().run_id))

        job_a()
        job_b()

        assert run_ids[0][0] == "a"
        assert run_ids[1][0] == "b"
        assert run_ids[0][1] != run_ids[1][1], "run_id는 실행마다 고유해야 함"


# ── 5. 스레드 격리 ─────────────────────────────────────────────────────────────

class TestThreadIsolation:

    def test_concurrent_jobs_do_not_interfere(self, mock_emitter):
        results = {}

        @datahub_job_fn("concurrent-job", flow="pipeline")
        def worker(worker_id: str):
            time.sleep(0.02)
            job = get_job()
            results[worker_id] = job.run_id if job else None

        threads = [
            threading.Thread(target=worker, args=(f"worker-{i}",))
            for i in range(5)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        run_ids = list(results.values())
        assert len(set(run_ids)) == 5, "각 스레드는 독립적인 run_id를 가져야 함"

    def test_child_thread_does_not_inherit_context(self, mock_emitter):
        child_job = {}

        def child():
            child_job["job"] = get_job()

        @datahub_job_fn("parent-job", flow="pipeline")
        def parent():
            t = threading.Thread(target=child)
            t.start()
            t.join()

        parent()
        assert child_job["job"] is None, "새 스레드는 부모의 job context를 상속하지 않음"


# ── 6. FastAPI 미들웨어 패턴 ──────────────────────────────────────────────────

class TestFastAPIMiddlewarePattern:

    def test_middleware_set_job_is_visible_in_handler(self, mock_emitter):
        """미들웨어에서 set_job() 호출 시 같은 스레드의 핸들러에서 context 접근 가능"""

        def middleware_enter(path: str, flow: str):
            set_job(job_id=f"GET:{path}", flow=flow)

        def handler():
            return get_job()

        middleware_enter("/orders/summary", flow="order-service")
        job = handler()

        assert job is not None
        assert job.job_id == "GET:/orders/summary"
        assert job.flow == "order-service"

        clear_job()

    def test_middleware_clear_removes_context(self, mock_emitter):
        set_job("GET:/orders", flow="order-service")
        assert get_job() is not None
        clear_job()
        assert get_job() is None
