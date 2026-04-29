import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from unittest.mock import MagicMock, patch

from aileron_meta_collector.context import datahub_job, get_job, propagate_job
from aileron_meta_collector.hooks.boto3 import (
    _s3_urn,
    _athena_table_urn,
    _resolve_athena_urns,
    _pending_athena,
)


class TestS3Urn:
    def test_key_with_prefix(self):
        urn = _s3_urn("my-bucket", "data/2024/01/file.parquet", "PROD")
        assert "my-bucket/data/2024/01" in urn
        assert "PROD" in urn

    def test_key_without_prefix(self):
        urn = _s3_urn("my-bucket", "file.parquet", "PROD")
        assert "my-bucket" in urn

    def test_env_reflected(self):
        urn = _s3_urn("bucket", "key/file.csv", "DEV")
        assert "DEV" in urn


class TestAthenaTableUrn:
    def test_table_only(self):
        urn = _athena_table_urn("orders", "AwsDataCatalog", "sales_db", "PROD")
        assert "sales_db.orders" in urn
        assert "glue" in urn

    def test_database_dot_table(self):
        urn = _athena_table_urn("sales_db.orders", "AwsDataCatalog", "default", "PROD")
        assert "sales_db.orders" in urn

    def test_three_part_name_drops_catalog(self):
        urn = _athena_table_urn("AwsDataCatalog.sales_db.orders", "AwsDataCatalog", "default", "PROD")
        assert "sales_db.orders" in urn
        assert "AwsDataCatalog" not in urn

    def test_env_reflected(self):
        urn = _athena_table_urn("orders", "AwsDataCatalog", "db", "DEV")
        assert "DEV" in urn


class TestResolveAthenaUrns:
    def test_select_query_urns(self):
        input_urns, output_urns = _resolve_athena_urns(
            inputs_raw=["orders", "users"],
            outputs_raw=[],
            catalog="AwsDataCatalog",
            database="sales_db",
            env="PROD",
        )
        assert len(input_urns) == 2
        assert all("glue" in u for u in input_urns)
        assert all("sales_db" in u for u in input_urns)
        assert output_urns == []

    def test_unload_output_becomes_s3_urn(self):
        _, output_urns = _resolve_athena_urns(
            inputs_raw=["orders"],
            outputs_raw=["__s3__my-bucket/output/orders"],
            catalog="AwsDataCatalog",
            database="sales_db",
            env="PROD",
        )
        assert len(output_urns) == 1
        assert "s3" in output_urns[0]
        assert "my-bucket/output/orders" in output_urns[0]

    def test_ctas_output_becomes_glue_urn(self):
        _, output_urns = _resolve_athena_urns(
            inputs_raw=[],
            outputs_raw=["order_summary"],
            catalog="AwsDataCatalog",
            database="sales_db",
            env="PROD",
        )
        assert len(output_urns) == 1
        assert "glue" in output_urns[0]
        assert "order_summary" in output_urns[0]



class TestCreateViewUrns:
    """CREATE VIEW / CREATE OR REPLACE VIEW SQL → URN 변환 end-to-end 검증"""

    def test_create_view_lineage_urns(self):
        from aileron_meta_collector.parsers.sql_parser import extract_tables

        sql = "CREATE VIEW sales_db.order_view AS SELECT * FROM sales_db.orders"
        inputs_raw, outputs_raw = extract_tables(sql)

        assert "sales_db.order_view" in outputs_raw
        assert "sales_db.orders" in inputs_raw

        input_urns, output_urns = _resolve_athena_urns(
            inputs_raw=inputs_raw,
            outputs_raw=outputs_raw,
            catalog="AwsDataCatalog",
            database="sales_db",
            env="PROD",
        )
        assert any("sales_db.order_view" in u for u in output_urns)
        assert any("sales_db.orders" in u for u in input_urns)
        assert all("glue" in u for u in input_urns + output_urns)

    def test_create_or_replace_view_lineage_urns(self):
        from aileron_meta_collector.parsers.sql_parser import extract_tables

        sql = """
            CREATE OR REPLACE VIEW sales_db.daily_summary AS
            SELECT order_date, SUM(amount) AS total
            FROM sales_db.orders
            GROUP BY order_date
        """
        inputs_raw, outputs_raw = extract_tables(sql)

        assert "sales_db.daily_summary" in outputs_raw, \
            f"expected 'sales_db.daily_summary' in outputs, got: {outputs_raw}"
        assert "sales_db.orders" in inputs_raw, \
            f"expected 'sales_db.orders' in inputs, got: {inputs_raw}"

        input_urns, output_urns = _resolve_athena_urns(
            inputs_raw=inputs_raw,
            outputs_raw=outputs_raw,
            catalog="AwsDataCatalog",
            database="sales_db",
            env="PROD",
        )
        assert any("sales_db.daily_summary" in u for u in output_urns)
        assert any("sales_db.orders" in u for u in input_urns)

    def test_create_or_replace_view_multi_join_urns(self):
        from aileron_meta_collector.parsers.sql_parser import extract_tables

        sql = """
            CREATE OR REPLACE VIEW analytics.report AS
            SELECT o.id, c.name
            FROM orders o
            LEFT JOIN customers c ON o.cid = c.id
        """
        inputs_raw, outputs_raw = extract_tables(sql)

        assert "analytics.report" in outputs_raw
        assert "orders" in inputs_raw
        assert "customers" in inputs_raw

        input_urns, output_urns = _resolve_athena_urns(
            inputs_raw=inputs_raw,
            outputs_raw=outputs_raw,
            catalog="AwsDataCatalog",
            database="analytics",
            env="PROD",
        )
        assert any("analytics.report" in u for u in output_urns)
        assert len(input_urns) == 2


class TestJobContext:
    def test_context_manager_clears_on_exit(self):
        with datahub_job("test-job"):
            assert get_job() is not None
        assert get_job() is None

    def test_context_manager_yields_job(self):
        with datahub_job("my-job", platform="spark") as job:
            assert job.job_id == "my-job"
            assert job.platform == "spark"

    def test_nested_jobs_are_isolated(self):
        import threading

        results = {}

        def worker(name):
            with datahub_job(name):
                import time; time.sleep(0.01)
                results[name] = get_job().job_id

        threads = [threading.Thread(target=worker, args=(f"job-{i}",)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for i in range(3):
            assert results[f"job-{i}"] == f"job-{i}"


class TestPropagateJob:

    def test_worker_thread_has_no_job_without_propagate(self):
        """propagate_job 없이 새 스레드에서는 job context가 None"""
        result = {}

        with datahub_job("parent-job"):
            def worker():
                result["job"] = get_job()

            t = threading.Thread(target=worker)
            t.start()
            t.join()

        assert result["job"] is None

    def test_propagate_job_passes_context_to_worker_thread(self):
        """propagate_job으로 감싸면 worker 스레드에서 job context 접근 가능"""
        result = {}

        with datahub_job("parent-job"):
            @propagate_job
            def worker():
                result["job"] = get_job()

            t = threading.Thread(target=worker)
            t.start()
            t.join()

        assert result["job"] is not None
        assert result["job"].job_id == "parent-job"

    def test_propagate_job_with_thread_pool_executor(self):
        """ThreadPoolExecutor 병렬 작업에서 모든 worker가 동일 job context 공유"""
        results = {}

        with datahub_job("parallel-job"):
            @propagate_job
            def worker(idx):
                results[idx] = get_job().job_id if get_job() else None

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(worker, i) for i in range(3)]
                for f in futures:
                    f.result()

        assert len(results) == 3
        assert all(job_id == "parallel-job" for job_id in results.values())

    def test_propagate_job_cleans_up_after_worker(self):
        """worker 스레드 종료 후 해당 스레드의 job context가 정리됨"""
        thread_job_after = {}

        with datahub_job("parent-job"):
            @propagate_job
            def worker():
                pass  # 작업 후 finally에서 _local.job = None

            def check_after():
                worker()
                thread_job_after["job"] = get_job()  # worker 실행 후 확인

            t = threading.Thread(target=check_after)
            t.start()
            t.join()

        assert thread_job_after["job"] is None
