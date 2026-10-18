from __future__ import annotations

import logging

from ..context import get_job
from ..emitter import emit_lineage_async

logger = logging.getLogger(__name__)


def _s3fs_path_to_urn(path: str, env: str) -> str:
    """s3fs 경로(s3://bucket/key 또는 bucket/key)를 DataHub S3 URN으로 변환."""
    normalized = path.replace("s3://", "").replace("s3a://", "").rstrip("/")
    parts = normalized.split("/")
    # 파일명 제거 → 디렉터리(prefix) 단위로 dataset 구성
    prefix_parts = parts[:-1] if len(parts) > 1 else parts
    dataset_name = "/".join(prefix_parts)
    return f"urn:li:dataset:(urn:li:dataPlatform:s3,{dataset_name},{env})"


def install_s3fs_hooks(env: str = "PROD") -> None:
    """
    s3fs S3FileSystem의 copy/mv 메서드를 패치하여 lineage를 자동 수집합니다.
    앱 시작 시 한 번만 호출하세요.
    """
    try:
        import s3fs
    except ImportError:
        logger.warning(
            "[aileron] s3fs가 설치되지 않아 s3fs 훅을 등록하지 않습니다. "
            "pip install s3fs 또는 pip install 'aileron-meta-collector[s3fs]'"
        )
        return

    fs_cls = s3fs.S3FileSystem

    _original_copy = fs_cls.copy
    _original_move = fs_cls.move

    def _patched_copy(self, path1, path2, *args, **kwargs):
        result = _original_copy(self, path1, path2, *args, **kwargs)
        _record_lineage(path1, path2, "copy", env)
        return result

    def _patched_move(self, path1, path2, *args, **kwargs):
        result = _original_move(self, path1, path2, *args, **kwargs)
        _record_lineage(path1, path2, "move", env)
        return result

    fs_cls.copy = _patched_copy
    fs_cls.move = _patched_move

    logger.info("[aileron] s3fs hooks installed (env=%s)", env)


def _record_lineage(src: str, dst: str, op: str, env: str) -> None:
    job = get_job()
    if not job:
        return
    try:
        src_urn = _s3fs_path_to_urn(src, env)
        dst_urn = _s3fs_path_to_urn(dst, env)

        if src_urn not in job.inputs:
            job.inputs.append(src_urn)
        if dst_urn not in job.outputs:
            job.outputs.append(dst_urn)

        logger.debug("[aileron] s3fs %s | src=%s  dst=%s", op, src_urn, dst_urn)
        emit_lineage_async(job, [src_urn], [dst_urn])
    except Exception:
        logger.warning("[aileron] s3fs lineage hook 오류 | op=%s  src=%s  dst=%s", op, src, dst, exc_info=True)
