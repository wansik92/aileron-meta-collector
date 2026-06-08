# Changelog

## [0.1.15] - 2026-06-08

### Added
- `DATAHUB_ENABLED` 환경변수 추가 (기본: true)
  - `false` 설정 시 모든 emit skip — 비즈니스 로직에 영향 없음
  - DataHub 연동이 필요 없는 환경에서 완전히 디펜던시 제거 가능

---

## [0.1.14] - 2026-06-08

### Fixed
- DataHub 복구 시 자동으로 emit 재개되지 않던 문제 수정
  - 실패 후 `DATAHUB_COOLDOWN_SEC`(기본 60초) 경과 시 자동 재시도
  - 재시도 성공 시 정상 emit 재개, 실패 시 쿨다운 재시작

---

## [0.1.13] - 2026-06-08

### Fixed
- DataHub down 시 emit 즉시 skip 처리
  - 첫 실패 감지 시 `_datahub_reachable=False` 플래그 설정 → 이후 요청은 큐 적재 없이 즉시 반환
  - emit 성공 시 자동 복구 (`_datahub_reachable=True`)
  - 요청 폭주 시 큐 누적 및 스레드 블로킹 문제 해결

---

## [0.1.12] - 2026-06-08

### Fixed
- `DatahubRestEmitter` 싱글턴으로 관리 — 요청마다 새 커넥션풀 생성으로 인한 소켓 고갈 방지

---

## [0.1.11] - 2026-06-08

### Fixed
- DataHub 연결 불가(DNS 실패 / 타임아웃) 시 스레드 블로킹 방지
  - `connect_timeout_sec` 기본값 3초 설정 (기존: urllib3 기본 무제한)
  - `retry_max_times` 기본값 0으로 설정 (기존: 3회 재시도)
  - 환경변수 `DATAHUB_CONNECT_TIMEOUT_SEC`, `DATAHUB_RETRY_MAX_TIMES` 로 조정 가능

---

## [0.1.10] - 2026-06-08

### Added
- `datahub_job(patch=True)` 파라미터 추가
  - `patch=True`: `DataJobInputOutput` 을 patch MCP 로 emit — 기존 데이터에 추가 (덮어쓰지 않음)
  - `patch=False` (기본값): 기존 replace 방식 유지
- `datahub_job_fn(patch=True)` 동일 파라미터 추가

---

## [0.1.9] - 2026-06-04

### Added
- `upstream_jobs` 에 full URN 지정 지원
  - `urn:li:...` 형식이면 그대로 사용 — cross-platform upstream 참조 가능
  - 예) Airflow DataJob → pythonSdk DataJob 연결

---

## [0.1.7] - 2026-06-04

### Changed
- `DataFlowInfoClass` 에서 `env` 필드 제거
  - DataHub 권장 방식: 환경 구분은 URN cluster 로만 관리
  - `DataFlowInfoClass.env` 는 FabricType enum 만 허용하므로 임의 cluster 명 사용 시 충돌 발생

---

## [0.1.6] - 2026-06-04

### Fixed
- `DataFlowInfoClass.env` 에 유효하지 않은 FabricType 전달 시 `AvroTypeException` 발생 수정
  - PROD/EI/DEV/CORP/STAGING/TEST 외 임의 값은 PROD 로 fallback 처리
  - DataFlow URN 의 클러스터명은 임의 값 그대로 유지

---

## [0.1.5] - 2026-06-02

### Changed
- sqlparse 의존성 완화: `>=0.5.0` → `>=0.4.0` (MWAA 등 관리형 환경 호환성 개선)

---

## [0.1.4] - 2026-04-30

### Changed
- emit 성공 시 로그 레벨 `DEBUG` → `INFO` 변경
  - `emit ok | dataflow`, `emit ok | datajob`, `emit ok | run_start`, `emit ok | run_end`, `emit ok | lineage`

---

## [0.1.3] - 2026-04-30

### Added
- `propagate_job` 유틸리티 추가
  - `ThreadPoolExecutor` worker 스레드에 부모 스레드의 job context 전파
  - `__init__.py` 공개 API에 노출
- 파이프라인 테스트 Step 7 추가 (병렬 UNLOAD × 2, `propagate_job` 사용)
- `TestPropagateJob` 단위 테스트 4개 추가
- README Section 6 (단일 Job 내 다수 쿼리 누적), Section 7 (`propagate_job` 사용법) 추가

---

## [0.1.2] - 2026-04-29

### Fixed
- DataJob upstream 연결이 DataHub UI에 표시되지 않던 문제 수정
  - input dataset에 `DatasetPropertiesClass()` stub entity 선 emit

### Added
- `flush_emit()` 추가 — 비동기 emit 작업 전체 완료 대기
- `propagate_job` 추가 (context.py)
- E-commerce 파이프라인 통합 테스트 (`test_pipeline.py`) 추가
  - INSERT INTO, CTAS, CREATE VIEW, CREATE OR REPLACE VIEW, CREATE TEMP VIEW, UNLOAD 전 타입 커버
- `test_ms_usage.py`, `test_integration.py` 삭제, `test_manual_lineage.py` 유지

---

## [0.1.1] - 2026-04-28

### Added
- DataJob 간 리니지 (`upstream_jobs`) 지원
- 수동 lineage 주입 API (`add_input`, `add_output`, `emit_lineage`, `build_dataset_urn`)
- `CREATE [OR REPLACE] VIEW` SQL 파싱 지원
- `DataJob` / `DataFlow` / `Dataset` description 파라미터 추가
- 로그 강화 (job lifecycle / Athena / S3 / SQLAlchemy)

### Fixed
- Python 3.12 호환성 및 botocore 신버전 대응
- URN에 공백 포함 시 DataHub GMS 유효성 검사 실패 방지
- optional dependency 미설치 시 `ImportError` 방지 (lazy import)

### Changed
- 의존성을 optional extras로 재구성 (`datahub`, `boto3`, `sqlalchemy`, `all`)

---

## [0.1.0] - 2026-04-27

### Added
- 최초 릴리즈
- SQLAlchemy Engine 전역 훅 (`install_sqlalchemy_hooks`)
- boto3 Athena / S3 이벤트 훅 (`install_boto3_hooks`)
- Thread-local `JobContext` 관리 (`datahub_job`, `datahub_job_fn`, `set_job`, `clear_job`)
- DataHub DataFlow / DataJob / DataProcessInstance 비동기 emit
- Athena 쿼리 타입 지원: `INSERT INTO`, `CTAS`, `UNLOAD`
- S3 URN / Glue URN 자동 변환
