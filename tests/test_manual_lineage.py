"""
수동 lineage 주입 API 테스트 (add_input / add_output / emit_lineage / build_dataset_urn)
"""
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from aileron_meta_collector import (
    add_input,
    add_output,
    build_dataset_urn,
    datahub_job_fn,
    emit_lineage,
)
from aileron_meta_collector.context import clear_job, get_job


@pytest.fixture(autouse=True)
def reset_job():
    clear_job()
    yield
    clear_job()


# ── build_dataset_urn ─────────────────────────────────────────────────────────

class TestBuildDatasetUrn:

    def test_glue_table(self):
        urn = build_dataset_urn("sales_db.orders", "glue", env="PROD")
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:glue,sales_db.orders,PROD)"

    def test_postgres_table(self):
        urn = build_dataset_urn("public.users", "postgres", env="DEV")
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:postgres,public.users,DEV)"

    def test_s3_path_plain(self):
        urn = build_dataset_urn("my-bucket/data/events", "s3", env="PROD")
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data/events,PROD)"

    def test_s3_path_with_prefix(self):
        urn = build_dataset_urn("s3://my-bucket/data/events", "s3", env="PROD")
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data/events,PROD)"

    def test_s3_trailing_slash_removed(self):
        urn = build_dataset_urn("my-bucket/data/events/", "s3", env="PROD")
        assert not urn.endswith("/")

    def test_env_fallback_to_config(self, monkeypatch):
        monkeypatch.setenv("DATAHUB_ENV", "STAGING")
        import importlib
        import aileron_meta_collector.config as cfg
        importlib.reload(cfg)
        import aileron_meta_collector.lineage as lin
        importlib.reload(lin)
        from aileron_meta_collector.lineage import build_dataset_urn as bdu
        urn = bdu("sales_db.orders", "glue")
        assert "STAGING" in urn


# ── add_input / add_output ────────────────────────────────────────────────────

class TestAddInputOutput:

    def test_add_input_with_table_and_platform(self):
        with patch("aileron_meta_collector.emitter.DatahubRestEmitter"):
            @datahub_job_fn("test-job", flow="test-flow")
            def run():
                add_input("sales_db.orders", platform="glue")
                assert any("sales_db.orders" in u for u in get_job().inputs)

            run()

    def test_add_output_with_table_and_platform(self):
        with patch("aileron_meta_collector.emitter.DatahubRestEmitter"):
            @datahub_job_fn("test-job", flow="test-flow")
            def run():
                add_output("sales_db.summary", platform="glue")
                assert any("sales_db.summary" in u for u in get_job().outputs)

            run()

    def test_add_input_with_urn(self):
        with patch("aileron_meta_collector.emitter.DatahubRestEmitter"):
            custom_urn = "urn:li:dataset:(urn:li:dataPlatform:glue,sales_db.orders,PROD)"

            @datahub_job_fn("test-job", flow="test-flow")
            def run():
                add_input(urn=custom_urn)
                assert custom_urn in get_job().inputs

            run()

    def test_add_output_with_urn(self):
        with patch("aileron_meta_collector.emitter.DatahubRestEmitter"):
            custom_urn = "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/out,PROD)"

            @datahub_job_fn("test-job", flow="test-flow")
            def run():
                add_output(urn=custom_urn)
                assert custom_urn in get_job().outputs

            run()

    def test_no_duplicate_inputs(self):
        with patch("aileron_meta_collector.emitter.DatahubRestEmitter"):
            @datahub_job_fn("test-job", flow="test-flow")
            def run():
                add_input("sales_db.orders", platform="glue")
                add_input("sales_db.orders", platform="glue")  # 중복
                assert sum(1 for u in get_job().inputs if "sales_db.orders" in u) == 1

            run()

    def test_raises_outside_job_context(self):
        with pytest.raises(ValueError, match="@datahub_job_fn"):
            add_input("sales_db.orders", platform="glue")

    def test_raises_without_table_or_urn(self):
        with patch("aileron_meta_collector.emitter.DatahubRestEmitter"):
            @datahub_job_fn("test-job", flow="test-flow")
            def run():
                with pytest.raises(ValueError, match="platform"):
                    add_input("sales_db.orders")  # platform 없음

            run()

    def test_mixed_with_auto_hooks(self):
        """자동 훅 + 수동 주입 병행 사용"""
        with patch("aileron_meta_collector.emitter.DatahubRestEmitter"):
            @datahub_job_fn("test-job", flow="test-flow")
            def run():
                # 수동으로 외부 시스템 데이터셋 추가
                add_input("external_db.raw_events", platform="postgres")
                add_output("sales_db.processed_events", platform="glue")
                job = get_job()
                assert len(job.inputs) == 1
                assert len(job.outputs) == 1

            run()


# ── emit_lineage ──────────────────────────────────────────────────────────────

class TestEmitLineage:

    def test_emit_lineage_with_platform(self):
        with patch("aileron_meta_collector.emitter.DatahubRestEmitter") as mock_cls:
            mock_instance = MagicMock()
            mock_cls.return_value = mock_instance

            emit_lineage(
                inputs=["sales_db.orders"],
                outputs=["sales_db.order_summary"],
                platform="glue",
                env="PROD",
            )
            time.sleep(0.1)

            emitted_urns = [c.args[0].entityUrn for c in mock_instance.emit.call_args_list]
            assert any("sales_db.order_summary" in u for u in emitted_urns)

    def test_emit_lineage_with_urns_directly(self):
        with patch("aileron_meta_collector.emitter.DatahubRestEmitter") as mock_cls:
            mock_instance = MagicMock()
            mock_cls.return_value = mock_instance

            input_urn  = "urn:li:dataset:(urn:li:dataPlatform:glue,sales_db.orders,PROD)"
            output_urn = "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/out,PROD)"

            emit_lineage(inputs=[input_urn], outputs=[output_urn])
            time.sleep(0.1)

            emitted_urns = [c.args[0].entityUrn for c in mock_instance.emit.call_args_list]
            assert output_urn in emitted_urns

    def test_emit_lineage_mixed_urn_and_table(self):
        with patch("aileron_meta_collector.emitter.DatahubRestEmitter") as mock_cls:
            mock_instance = MagicMock()
            mock_cls.return_value = mock_instance

            emit_lineage(
                inputs=["urn:li:dataset:(urn:li:dataPlatform:glue,sales_db.orders,PROD)"],
                outputs=["my-bucket/result"],
                platform="s3",
                env="PROD",
            )
            time.sleep(0.1)
            assert mock_instance.emit.called

    def test_emit_lineage_raises_without_platform_for_table(self):
        with pytest.raises(ValueError, match="platform"):
            emit_lineage(
                inputs=["sales_db.orders"],  # URN 아님
                outputs=["sales_db.summary"],
                # platform 없음
            )
