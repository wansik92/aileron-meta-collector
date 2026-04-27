import os

DATAHUB_GMS_URL: str = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
DATAHUB_ENV: str = os.getenv("DATAHUB_ENV", "PROD")

# emit 실패를 무시할지 여부 (기본: True — MS 장애 전파 방지)
DATAHUB_SILENT_FAIL: bool = os.getenv("DATAHUB_SILENT_FAIL", "true").lower() == "true"
