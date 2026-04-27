import pytest
from unittest.mock import MagicMock, patch

from aileron_meta_collector.context import datahub_job, get_job
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
