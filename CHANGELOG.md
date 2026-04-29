# Changelog

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
