import os


def _get(airflow_key: str, env_key: str, default: str) -> str:
    """Airflow Variable → 환경변수 순으로 설정값을 읽습니다.

    MWAA 환경에서는 Airflow UI(Admin → Variables)에서 즉시 변경 가능.
    일반 Python 환경에서는 환경변수로 fallback.
    """
    try:
        from airflow.models import Variable
        return Variable.get(airflow_key, default_var=default)
    except Exception:
        return os.getenv(env_key, default)


def is_datahub_enabled() -> bool:
    return _get("datahub_enabled", "DATAHUB_ENABLED", "false").lower() == "true"


def get_gms_url() -> str:
    return _get("datahub_gms_url", "DATAHUB_GMS_URL", "http://localhost:8080")


def get_env() -> str:
    return _get("datahub_env", "DATAHUB_ENV", "PROD")


def is_silent_fail() -> bool:
    return _get("datahub_silent_fail", "DATAHUB_SILENT_FAIL", "true").lower() == "true"


def get_connect_timeout_sec() -> float:
    return float(_get("datahub_connect_timeout_sec", "DATAHUB_CONNECT_TIMEOUT_SEC", "3"))


def get_retry_max_times() -> int:
    return int(_get("datahub_retry_max_times", "DATAHUB_RETRY_MAX_TIMES", "0"))


def get_cooldown_sec() -> float:
    return float(_get("datahub_cooldown_sec", "DATAHUB_COOLDOWN_SEC", "60"))
