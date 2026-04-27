from .hooks.sqlalchemy import install_sqlalchemy_hooks
from .hooks.boto3 import install_boto3_hooks
from .context import set_job, clear_job, datahub_job, datahub_job_fn


def install_all_hooks(env: str = "PROD") -> None:
    install_sqlalchemy_hooks(env=env)
    install_boto3_hooks(env=env)


__all__ = [
    "install_all_hooks",
    "install_sqlalchemy_hooks",
    "install_boto3_hooks",
    "set_job",
    "clear_job",
    "datahub_job",
    "datahub_job_fn",
]
