import os

# DataHub 연동 활성화 여부 (기본: True) — False 시 모든 emit skip, 비즈니스 로직에 영향 없음
DATAHUB_ENABLED: bool = os.getenv("DATAHUB_ENABLED", "true").lower() == "true"

DATAHUB_GMS_URL: str = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
DATAHUB_ENV: str = os.getenv("DATAHUB_ENV", "PROD")

# emit 실패를 무시할지 여부 (기본: True — 서비스 장애 전파 방지)
DATAHUB_SILENT_FAIL: bool = os.getenv("DATAHUB_SILENT_FAIL", "true").lower() == "true"

# 연결 타임아웃 (기본: 3초) — DNS 실패 / 연결 불가 시 빠르게 포기
DATAHUB_CONNECT_TIMEOUT_SEC: float = float(os.getenv("DATAHUB_CONNECT_TIMEOUT_SEC", "3"))

# 최대 재시도 횟수 (기본: 0 — 재시도 없이 즉시 실패)
DATAHUB_RETRY_MAX_TIMES: int = int(os.getenv("DATAHUB_RETRY_MAX_TIMES", "0"))
