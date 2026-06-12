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
    return _get("ail_datahub_enabled", "AIL_DATAHUB_ENABLED", "false").lower() == "true"


def get_gms_url() -> str:
    return _get("ail_datahub_gms_url", "AIL_DATAHUB_GMS_URL", "http://aileron.universe.com/gms")


def get_env() -> str:
    return _get("ail_datahub_env", "AIL_DATAHUB_ENV", "PROD")


def is_silent_fail() -> bool:
    return _get("ail_datahub_silent_fail", "AIL_DATAHUB_SILENT_FAIL", "true").lower() == "true"


def get_connect_timeout_sec() -> float:
    return float(_get("ail_datahub_connect_timeout_sec", "AIL_DATAHUB_CONNECT_TIMEOUT_SEC", "3"))


def get_retry_max_times() -> int:
    return int(_get("ail_datahub_retry_max_times", "AIL_DATAHUB_RETRY_MAX_TIMES", "0"))


def get_cooldown_sec() -> float:
    return float(_get("ail_datahub_cooldown_sec", "AIL_DATAHUB_COOLDOWN_SEC", "60"))
