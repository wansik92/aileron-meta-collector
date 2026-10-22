"""s3fs S3FileSystem.copy / move 가 job 의 input/output 과 lineage emit 으로 수집되는지 검증한다.

실제 AWS 대신 moto 서버를 사용하고, DataHub 로의 emit 은 mock 으로 가로채 호출 인자만 확인한다.

xfail(strict) 로 표시한 두 테스트는 현재 훅의 알려진 한계를 기록한 것이다. 고쳐지면 XPASS 로 실패하므로
그때 xfail 표시를 지우면 된다.
"""

import pytest
from unittest.mock import patch

s3fs = pytest.importorskip("s3fs")
moto_server = pytest.importorskip("moto.server")
boto3 = pytest.importorskip("boto3")

from aileron_meta_collector.context import datahub_job, get_job
from aileron_meta_collector.hooks.s3fs import install_s3fs_hooks

BUCKET = "ail-s3fs-test"


def _urn(prefix: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:s3,{prefix},PROD)"


@pytest.fixture(scope="module")
def s3_endpoint():
    server = moto_server.ThreadedMotoServer(port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def make_fs(s3_endpoint, monkeypatch):
    """S3FileSystem 을 새로 만드는 함수를 돌려준다. 훅 설치 전/후 인스턴스를 구분해 테스트하기 위해 팩토리로 둔다."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    client = boto3.client("s3", endpoint_url=s3_endpoint, region_name="us-east-1")
    # us-east-1 에서는 LocationConstraint 를 보내면 안 되므로 s3fs.mkdirs 대신 boto3 로 만든다
    if BUCKET not in [b["Name"] for b in client.list_buckets()["Buckets"]]:
        client.create_bucket(Bucket=BUCKET)

    created = []

    def factory():
        filesystem = s3fs.S3FileSystem(
            key="testing", secret="testing",
            client_kwargs={"endpoint_url": s3_endpoint, "region_name": "us-east-1"},
            skip_instance_cache=True,
        )
        filesystem.pipe(f"{BUCKET}/in/a.csv", b"id\n1\n")
        filesystem.pipe(f"{BUCKET}/in/b.csv", b"id\n2\n")
        created.append(filesystem)
        return filesystem

    yield factory
    for filesystem in created:
        for path in filesystem.find(BUCKET):
            filesystem.rm(path)


@pytest.fixture
def emit_lineage(monkeypatch):
    """DataHub emit 을 가로채고, 테스트가 끝나면 s3fs 의 원래 메서드로 되돌린다(훅은 아직 설치하지 않음)."""
    monkeypatch.setenv("AIL_DATAHUB_ENABLED", "true")
    original_copy, original_move = s3fs.S3FileSystem.copy, s3fs.S3FileSystem.move
    with patch("aileron_meta_collector.hooks.s3fs.emit_lineage_async") as emit, \
         patch("aileron_meta_collector.emitter.emit_dataflow_async"), \
         patch("aileron_meta_collector.emitter.emit_datajob_async"), \
         patch("aileron_meta_collector.emitter.emit_run_start_async"), \
         patch("aileron_meta_collector.emitter.emit_run_end_async"):
        yield emit
    s3fs.S3FileSystem.copy, s3fs.S3FileSystem.move = original_copy, original_move


@pytest.fixture
def fs(emit_lineage, make_fs):
    """실제 사용 순서대로: 훅을 먼저 설치한 뒤 S3FileSystem 인스턴스를 만든다."""
    install_s3fs_hooks(env="PROD")
    return make_fs()


class TestS3fsCopy:
    def test_copy_records_src_as_input_and_dst_as_output(self, fs, emit_lineage):
        with datahub_job("copy-job", flow="s3fs-flow") as job:
            fs.copy(f"{BUCKET}/in/a.csv", f"{BUCKET}/out/a.csv")

        assert job.inputs == [_urn(f"{BUCKET}/in")]
        assert job.outputs == [_urn(f"{BUCKET}/out")]
        emit_lineage.assert_called_once_with(job, [_urn(f"{BUCKET}/in")], [_urn(f"{BUCKET}/out")])

    def test_copy_still_copies_the_object(self, fs):
        with datahub_job("copy-job", flow="s3fs-flow"):
            fs.copy(f"{BUCKET}/in/a.csv", f"{BUCKET}/out/a.csv")

        assert fs.exists(f"{BUCKET}/in/a.csv")
        assert fs.cat(f"{BUCKET}/out/a.csv") == b"id\n1\n"

    def test_s3_scheme_paths(self, fs):
        with datahub_job("copy-job", flow="s3fs-flow") as job:
            fs.copy(f"s3://{BUCKET}/in/a.csv", f"s3://{BUCKET}/out/a.csv")

        assert job.inputs == [_urn(f"{BUCKET}/in")]
        assert job.outputs == [_urn(f"{BUCKET}/out")]


class TestS3fsMove:
    def test_move_records_src_as_input_and_dst_as_output(self, fs):
        with datahub_job("move-job", flow="s3fs-flow") as job:
            fs.move(f"{BUCKET}/in/b.csv", f"{BUCKET}/archive/b.csv")

        assert job.inputs == [_urn(f"{BUCKET}/in")]
        assert job.outputs == [_urn(f"{BUCKET}/archive")]

    def test_move_still_moves_the_object(self, fs):
        with datahub_job("move-job", flow="s3fs-flow"):
            fs.move(f"{BUCKET}/in/b.csv", f"{BUCKET}/archive/b.csv")

        assert not fs.exists(f"{BUCKET}/in/b.csv")
        assert fs.cat(f"{BUCKET}/archive/b.csv") == b"id\n2\n"

    @pytest.mark.xfail(
        strict=True,
        reason="move 가 내부에서 패치된 copy 를 다시 호출해 emit_lineage_async 가 2번 호출된다 (job.inputs/outputs 는 중복 제거됨)",
    )
    def test_move_emits_lineage_once(self, fs, emit_lineage):
        with datahub_job("move-job", flow="s3fs-flow"):
            fs.move(f"{BUCKET}/in/b.csv", f"{BUCKET}/archive/b.csv")

        assert emit_lineage.call_count == 1


class TestS3fsMultipleOperations:
    def test_copy_and_move_accumulate_without_duplicates(self, fs):
        with datahub_job("mixed-job", flow="s3fs-flow") as job:
            fs.copy(f"{BUCKET}/in/a.csv", f"{BUCKET}/out/a.csv")
            fs.move(f"{BUCKET}/in/b.csv", f"{BUCKET}/out/b.csv")

        assert job.inputs == [_urn(f"{BUCKET}/in")]
        assert job.outputs == [_urn(f"{BUCKET}/out")]


class TestHookInstallOrder:
    @pytest.mark.xfail(
        strict=True,
        reason="s3fs 는 인스턴스 생성 시 copy 를 인스턴스 속성으로 붙이므로, 훅 설치 이전에 만든 인스턴스의 copy 는 수집되지 않는다",
    )
    def test_copy_on_instance_created_before_hook_install(self, emit_lineage, make_fs):
        fs_before_hook = make_fs()
        install_s3fs_hooks(env="PROD")

        with datahub_job("copy-job", flow="s3fs-flow") as job:
            fs_before_hook.copy(f"{BUCKET}/in/a.csv", f"{BUCKET}/out/a.csv")

        assert job.inputs == [_urn(f"{BUCKET}/in")]
        assert job.outputs == [_urn(f"{BUCKET}/out")]


class TestNoActiveJob:
    def test_copy_outside_job_does_not_emit(self, fs, emit_lineage):
        assert get_job() is None
        fs.copy(f"{BUCKET}/in/a.csv", f"{BUCKET}/out/a.csv")

        emit_lineage.assert_not_called()
        assert fs.exists(f"{BUCKET}/out/a.csv")
