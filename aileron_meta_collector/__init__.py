from .hooks.sqlalchemy import install_sqlalchemy_hooks
from .hooks.boto3 import install_boto3_hooks
from .context import set_job, clear_job, datahub_job, datahub_job_fn
from .lineage import add_input, add_output, emit_lineage, build_dataset_urn


def install_all_hooks(env: str = "PROD") -> None:
    install_sqlalchemy_hooks(env=env)
    install_boto3_hooks(env=env)


__all__ = [
    # 훅 설치
    "install_all_hooks",
    "install_sqlalchemy_hooks",
    "install_boto3_hooks",
    # job context
    "set_job",
    "clear_job",
    "datahub_job",
    "datahub_job_fn",
    # 수동 lineage 주입
    "add_input",
    "add_output",
    "emit_lineage",
    "build_dataset_urn",
]
