# aileron-meta-collector

DataHub 기반 메타데이터 자동 수집 라이브러리입니다.  
Python 마이크로서비스에서 **SQLAlchemy / boto3 이벤트 훅**을 통해 소스코드 변경을 최소화하면서 데이터 lineage를 자동으로 수집합니다.

---

## 목적

사내 데이터 플랫폼에 DataHub를 도입할 때, 각 마이크로서비스(MS)에 `acryl-datahub` SDK를 직접 심으면 다음 문제가 발생합니다.

- MS마다 반복적인 emit 코드 작성
- DataHub 스펙 변경 시 모든 MS 수정 필요
- 비즈니스 로직과 메타데이터 수집 로직의 혼재

`aileron-meta-collector`는 이 문제를 **라이브러리 계층에서 흡수**합니다.  
MS 개발자는 의존성 추가 후 함수에 **데코레이터 한 줄**만 추가하면 lineage 수집이 완료됩니다.

---

## 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│                    Python Microservice                      │
│                                                             │
│   @datahub_job_fn("process-orders", flow="etl-pipeline")   │
│   def process_orders():                                     │
│       df  = pd.read_sql("SELECT ...", engine)         ─┐   │
│       resp = s3.get_object(Bucket="..", Key="..")      ─┤   │
│       qid  = athena.start_query_execution(...)         ─┤   │
│       df.to_sql("output_table", engine)                ─┘   │
│                                                             │
└──────────────────────┬──────────────────────────────────────┘
                       │ I/O 발생 시 자동 감지
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  aileron-meta-collector                     │
│                                                             │
│  ┌──────────────────┐  ┌───────────────┐  ┌─────────────┐  │
│  │ SQLAlchemy Hook  │  │  S3 Hook      │  │ Athena Hook │  │
│  │                  │  │               │  │             │  │
│  │ after_cursor_    │  │ before-call   │  │ before/after│  │
│  │ execute 이벤트    │  │ .s3.* 이벤트  │  │ StartQuery  │  │
│  └────────┬─────────┘  └──────┬────────┘  │ GetQuery    │  │
│           │                   │           └──────┬──────┘  │
│           └─────────┬─────────┘                  │         │
│                     │         ┌───────────────────┘         │
│                     ▼         ▼                             │
│            ┌──────────────────────┐                         │
│            │      SQL Parser      │                         │
│            │  SELECT  → input     │                         │
│            │  INSERT  → output    │                         │
│            │  UNLOAD  → S3 output │                         │
│            └──────────┬───────────┘                         │
│                       ▼                                     │
│            ┌──────────────────────┐                         │
│            │  JobContext          │  (Thread-local)         │
│            │  inputs[]            │                         │
│            │  outputs[]           │                         │
│            └──────────┬───────────┘                         │
│                       ▼                                     │
│            ┌──────────────────────┐                         │
│            │   Async Emitter      │  (ThreadPoolExecutor)   │
│            └──────────┬───────────┘                         │
└───────────────────────┼─────────────────────────────────────┘
                        │ REST (비동기)
                        ▼
           ┌────────────────────────┐
           │     DataHub GMS        │
           └────────────────────────┘
```

### 핵심 설계 원칙

| 원칙 | 구현 방식 |
|------|----------|
| **MS 침투 최소화** | 함수 데코레이터 한 줄 — 비즈니스 로직 변경 없음 |
| **멀티스레드 안전** | `threading.local()` 로 job context 스레드 격리 |
| **성능 영향 없음** | `ThreadPoolExecutor` 비동기 emit — 메인 스레드 블로킹 없음 |
| **장애 전파 방지** | emit 실패 시 `logger.warning` 흡수 — MS 정상 동작 유지 |
| **플랫폼 자동 추론** | DB URL에서 postgres/mysql/redshift 등 자동 감지 |

---

## 프로젝트 구조

```
aileron-meta-collector/
├── pyproject.toml
├── aileron_meta_collector/
│   ├── __init__.py              # 공개 API (install_all_hooks, datahub_job_fn 등)
│   ├── config.py                # 환경변수 기반 설정
│   ├── context.py               # Thread-local JobContext, 데코레이터
│   ├── emitter.py               # 비동기 DataHub REST emitter
│   ├── hooks/
│   │   ├── sqlalchemy.py        # SQLAlchemy Engine 전역 훅
│   │   └── boto3.py             # boto3 S3 / Athena 이벤트 훅
│   └── parsers/
│       └── sql_parser.py        # SQL → (input_tables, output_tables)
└── tests/
    ├── test_ms_usage.py         # MS 사용 패턴 통합 테스트
    ├── parsers/
    │   └── test_sql_parser.py
    └── hooks/
        └── test_boto3_hooks.py
```

---

## 주요 기능

### 1. SQLAlchemy 자동 lineage 수집

모든 SQLAlchemy `Engine`에 `after_cursor_execute` 이벤트 리스너를 전역 등록합니다.  
쿼리 실행 시점에 SQL을 파싱하여 입출력 테이블을 자동으로 DataHub에 등록합니다.

**지원 SQL 패턴**

| SQL 패턴 | input | output |
|---------|-------|--------|
| `SELECT ... FROM table` | table | — |
| `SELECT ... FROM a JOIN b` | a, b | — |
| `INSERT INTO out SELECT ... FROM in` | in | out |
| `UPDATE table SET ...` | — | table |
| `CREATE TABLE out AS SELECT ... FROM in` | in | out |

**지원 DB 플랫폼**

`postgresql`, `mysql`, `redshift`, `snowflake`, `bigquery`, `mssql`, `sqlite`  
→ DB URL에서 자동 추론하여 DataHub platform URN 생성

---

### 2. boto3 S3 자동 lineage 수집

boto3 default session의 이벤트 시스템을 통해 S3 작업을 자동 감지합니다.  
파일 단위가 아닌 **디렉토리(prefix) 단위**로 lineage를 추적합니다.

| S3 작업 | 방향 |
|--------|------|
| `GetObject`, `HeadObject`, `ListObjects*`, `SelectObjectContent` | input |
| `PutObject`, `CompleteMultipartUpload` | output |
| `CopyObject` | source → input, destination → output |
| `DeleteObject` | output |

---

### 3. boto3 Athena 자동 lineage 수집

`StartQueryExecution` / `GetQueryExecution` 이벤트를 조합하여  
**쿼리가 SUCCEEDED 상태가 된 시점에만** lineage를 emit합니다.

#### Athena 훅 동작 흐름

```
before-call.StartQueryExecution  →  SQL + JobContext → thread-local 저장
after-call.StartQueryExecution   →  execution_id 확보 → _pending_athena dict에 이관
after-call.GetQueryExecution     →  SUCCEEDED 시 emit / FAILED·CANCELLED 시 pending 정리
```

> **주의**: MS가 `GetQueryExecution`을 폴링하지 않고 fire-and-forget으로 사용하는 경우  
> 훅이 트리거되지 않습니다. 이 경우 Airflow나 별도 완료 체크 로직이 필요합니다.

#### 수집 가능한 Athena 쿼리 유형

| 쿼리 유형 | 예시 | input | output |
|----------|------|-------|--------|
| **SELECT** | `SELECT * FROM orders` | Glue 테이블 | — (임시 S3 결과 무시) |
| **SELECT + JOIN** | `SELECT ... FROM a JOIN b ON ...` | Glue 테이블 복수 | — |
| **CTAS** | `CREATE TABLE summary AS SELECT ... FROM orders` | Glue 테이블 | Glue 테이블 |
| **INSERT INTO** | `INSERT INTO out SELECT ... FROM in` | Glue 테이블 | Glue 테이블 |
| **UNLOAD** | `UNLOAD (SELECT ... FROM orders) TO 's3://bucket/path/'` | Glue 테이블 | S3 prefix |

#### 수집 불가능한 경우

| 케이스 | 이유 |
|--------|------|
| `GetQueryExecution` 미호출 (fire-and-forget) | 완료 훅이 트리거되지 않음 |
| Athena Federated Query (외부 데이터소스) | SQL 파서가 외부 커넥터 테이블명을 인식하지 못함 |
| 복잡한 CTE 중첩 | SQL 파서의 파싱 한계 |

---

### 4. DataFlow / DataJob / DataProcessInstance 자동 등록

`@datahub_job_fn` 데코레이터 진입/종료 시점에 파이프라인 실행 이력을 자동으로 등록합니다.

```
함수 진입  →  DataFlow upsert (파이프라인 단위)
          →  DataJob  upsert (태스크 단위)
          →  DataProcessInstance STARTED emit

실행 중    →  I/O 훅이 inputs / outputs 누적

함수 종료  →  DataProcessInstance COMPLETE / FAILED emit
          →  DataJob inlet/outlet 최신 상태 업데이트
```

**DataHub에 등록되는 엔티티**

| 엔티티 | URN 형식 | 설명 |
|--------|---------|------|
| DataFlow | `urn:li:dataFlow:(pythonSdk,{flow},{env})` | 파이프라인 (최초 1회 upsert) |
| DataJob | `urn:li:dataJob:(urn:li:dataFlow:(...),{job_id})` | 태스크 (최초 1회 upsert) |
| DataProcessInstance | `urn:li:dataProcessInstance:{run_id}` | 실행마다 신규 생성 |

**DataHub UI Run History 예시**

```
DataFlow: daily-etl-pipeline
└── DataJob: create-order-summary
      inlet:  glue.sales_db.orders
      outlet: glue.sales_db.order_summary
      │
      └── Run History
            ┌──────────────────────────────────────┐
            │ run-a3f2b1c0  COMPLETE  13.4s        │
            │   inputs:  sales_db.orders           │
            │   outputs: sales_db.order_summary    │
            ├──────────────────────────────────────┤
            │ run-9d4e7f2a  FAILED    2.1s         │
            │   inputs:  sales_db.orders           │
            │   outputs: —                         │
            └──────────────────────────────────────┘
```

---

### 5. Thread-safe Job Context

`threading.local()` 기반으로 스레드별 독립적인 job context를 관리합니다.  
멀티스레드 환경(FastAPI, Celery 등)에서 각 요청/태스크가 서로 간섭하지 않습니다.

### 6. 비동기 emit

DataHub GMS로의 HTTP 요청은 `ThreadPoolExecutor`로 비동기 처리됩니다.  
emit 실패는 `DATAHUB_SILENT_FAIL` 설정에 따라 무시하거나 예외를 발생시킵니다.

---

## 설치

```bash
pip install aileron-meta-collector
```

개발 환경:

```bash
pip install "aileron-meta-collector[dev]"
```

---

## 환경변수

| 변수명 | 기본값 | 설명 |
|--------|--------|------|
| `DATAHUB_GMS_URL` | `http://localhost:8080` | DataHub GMS REST 엔드포인트 |
| `DATAHUB_ENV` | `PROD` | DataHub 환경 (`PROD` / `DEV` / `STAGING`) |
| `DATAHUB_SILENT_FAIL` | `true` | emit 실패 시 예외 전파 여부 (`false`로 설정 시 예외 발생) |

---

## 사용 방법

### 기본 설정 (앱 시작 시 1회)

```python
# main.py 또는 settings.py
import os
from aileron_meta_collector import install_all_hooks

os.environ["DATAHUB_GMS_URL"] = "http://datahub-gms.internal:8080"
os.environ["DATAHUB_ENV"] = "PROD"

install_all_hooks()  # SQLAlchemy + boto3(S3 + Athena) 훅 일괄 등록
```

훅을 개별 등록하려면:

```python
from aileron_meta_collector import install_sqlalchemy_hooks, install_boto3_hooks

install_sqlalchemy_hooks(env="PROD")
install_boto3_hooks(env="PROD")
```

---

### 함수 데코레이터 — 기본 사용법

기존 함수에 `@datahub_job_fn` 한 줄만 추가합니다.  
함수 내부의 SQLAlchemy / S3 / Athena 호출이 자동으로 감지됩니다.

```python
from aileron_meta_collector import datahub_job_fn
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("postgresql://user:pass@host/db")

@datahub_job_fn("order-daily-aggregation", flow="order-processing-service")
def aggregate_orders():
    # SELECT → orders가 input으로 자동 등록
    df = pd.read_sql("SELECT * FROM orders WHERE status = 'done'", engine)

    df_agg = df.groupby("user_id").size().reset_index(name="count")

    # INSERT → order_summary가 output으로 자동 등록
    df_agg.to_sql("order_summary", engine, if_exists="replace", index=False)
```

---

### 함수 데코레이터 — S3

```python
import boto3
from aileron_meta_collector import datahub_job_fn

s3 = boto3.client("s3")

@datahub_job_fn("user-event-etl", flow="user-event-service")
def process_user_events():
    # GetObject → s3://raw-data/events/2024/01 이 input으로 자동 등록
    response = s3.get_object(Bucket="raw-data", Key="events/2024/01/data.parquet")

    processed = transform(response["Body"].read())

    # PutObject → s3://processed/user-events/2024/01 이 output으로 자동 등록
    s3.put_object(
        Bucket="processed",
        Key="user-events/2024/01/result.parquet",
        Body=processed,
    )
```

---

### 함수 데코레이터 — Athena CTAS

```python
import time
import boto3
from aileron_meta_collector import datahub_job_fn

athena = boto3.client("athena", region_name="ap-northeast-2")

def wait_for_query(execution_id: str) -> str:
    while True:
        result = athena.get_query_execution(QueryExecutionId=execution_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return state
        time.sleep(2)

@datahub_job_fn("create-order-summary", flow="daily-etl-pipeline")
def create_order_summary():
    # input: sales_db.orders / output: sales_db.order_summary
    qid = athena.start_query_execution(
        QueryString="""
            CREATE TABLE order_summary
            WITH (
                format = 'PARQUET',
                external_location = 's3://data-lake/order_summary/'
            )
            AS SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total
               FROM orders
               GROUP BY user_id
        """,
        QueryExecutionContext={"Database": "sales_db"},
        ResultConfiguration={"OutputLocation": "s3://athena-results/tmp/"},
    )["QueryExecutionId"]

    wait_for_query(qid)  # GetQueryExecution 호출 시점에 SUCCEEDED 감지 → lineage emit
```

---

### 함수 데코레이터 — Athena UNLOAD

```python
@datahub_job_fn("export-orders-parquet", flow="daily-etl-pipeline")
def export_orders():
    # input: sales_db.orders + sales_db.users / output: s3://data-lake/exports/orders/
    qid = athena.start_query_execution(
        QueryString="""
            UNLOAD (
                SELECT o.id, o.amount, u.name
                FROM orders o
                JOIN users u ON o.user_id = u.id
            )
            TO 's3://data-lake/exports/orders/'
            WITH (format = 'PARQUET', compression = 'SNAPPY')
        """,
        QueryExecutionContext={"Database": "sales_db"},
        ResultConfiguration={"OutputLocation": "s3://athena-results/tmp/"},
    )["QueryExecutionId"]

    wait_for_query(qid)
```

---

### 함수 데코레이터 — 혼합 사용 (SQLAlchemy + Athena + S3)

비즈니스 로직과 여러 종류의 I/O가 혼재된 경우, 데코레이터를 상위 함수에 적용하면  
내부에서 호출되는 모든 함수의 I/O가 **같은 job context**로 수집됩니다.

```python
@datahub_job_fn("daily-pipeline", flow="order-service")
def run_daily_pipeline():
    fetch_from_rds()      # 내부에서 pd.read_sql() → 자동 감지
    run_athena_summary()  # 내부에서 start/get_query_execution() → 자동 감지
    export_to_s3()        # 내부에서 s3.put_object() → 자동 감지

def fetch_from_rds():
    pd.read_sql("SELECT * FROM orders", engine)

def run_athena_summary():
    qid = athena.start_query_execution(...)["QueryExecutionId"]
    wait_for_query(qid)

def export_to_s3():
    s3.put_object(Bucket="output", Key="result/data.csv", Body=...)
```

---

### 함수 데코레이터 — 예외 처리

함수 실행 중 예외가 발생하면 DataProcessInstance가 자동으로 `FAILED`로 등록됩니다.

```python
@datahub_job_fn("risky-job", flow="daily-etl-pipeline")
def risky_job():
    raise ValueError("something went wrong")
# → DataProcessInstance: result=FAILURE
```

---

### FastAPI 미들웨어 연동 (코드 변경 0줄)

엔드포인트 함수에 데코레이터 없이, 미들웨어에서 job context를 자동으로 설정합니다.

```python
from fastapi import FastAPI, Request
from aileron_meta_collector.context import set_job, clear_job
import uuid

app = FastAPI()

@app.middleware("http")
async def datahub_lineage_middleware(request: Request, call_next):
    set_job(
        job_id=f"{request.method}:{request.url.path}:{uuid.uuid4().hex[:8]}",
        flow="order-processing-service",
    )
    try:
        return await call_next(request)
    finally:
        clear_job()

# 비즈니스 코드 변경 없음
@app.post("/orders/summarize")
def summarize_orders():
    df = pd.read_sql("SELECT * FROM orders", engine)  # 자동 감지
    df.to_sql("order_summary", engine)                # 자동 감지
```

---

### Celery 태스크 연동 (코드 변경 0줄)

```python
from celery import signals
from aileron_meta_collector.context import set_job, clear_job

@signals.task_prerun.connect
def on_task_start(task_id, task, **kwargs):
    set_job(job_id=task.name, flow="celery-workers")

@signals.task_postrun.connect
def on_task_end(**kwargs):
    clear_job()
```

---

## DataHub URN 형식

수집된 lineage는 아래 형식으로 DataHub에 등록됩니다.

**DB 테이블 (SQLAlchemy)**
```
urn:li:dataset:(urn:li:dataPlatform:{platform},{schema}.{table},{env})

# 예시
urn:li:dataset:(urn:li:dataPlatform:postgres,public.orders,PROD)
urn:li:dataset:(urn:li:dataPlatform:mysql,shop.users,PROD)
```

**Athena 테이블 (Glue Data Catalog)**
```
urn:li:dataset:(urn:li:dataPlatform:glue,{database}.{table},{env})

# 예시
urn:li:dataset:(urn:li:dataPlatform:glue,sales_db.orders,PROD)
urn:li:dataset:(urn:li:dataPlatform:glue,sales_db.order_summary,PROD)
```

**S3 (boto3 직접 호출 / Athena UNLOAD)**
```
urn:li:dataset:(urn:li:dataPlatform:s3,{bucket}/{prefix},{env})

# 예시
urn:li:dataset:(urn:li:dataPlatform:s3,raw-data/events/2024/01,PROD)
urn:li:dataset:(urn:li:dataPlatform:s3,data-lake/exports/orders,PROD)
```

---

## 테스트 실행

```bash
# 전체 테스트
pytest

# 특정 모듈
pytest tests/test_ms_usage.py -v   # MS 사용 패턴 통합 테스트
pytest tests/parsers/              # SQL 파서 단위 테스트
pytest tests/hooks/                # 훅 단위 테스트
```

---

## 제약 사항 및 주의점

- **SQLAlchemy 훅**은 `install_all_hooks()` 호출 순서와 무관하게 이후 생성되는 모든 Engine에 적용됩니다.
- **boto3 `resource` API**는 내부적으로 `client`를 사용하므로 동일하게 감지됩니다.
- **상위 함수에 데코레이터 적용 시**, 새 스레드를 생성하면 해당 스레드는 job context를 상속하지 않습니다. 새 스레드가 필요한 경우 `get_job()`으로 context를 명시적으로 전달하세요.
- **Athena fire-and-forget 패턴**에서는 `GetQueryExecution`을 호출하지 않으면 lineage가 emit되지 않습니다. 폴링 루프 또는 Airflow 태스크에서 상태를 확인하는 구조가 필요합니다.
- **복잡한 CTE / 중첩 서브쿼리**는 SQL 파서의 한계로 일부 테이블이 누락될 수 있습니다. 이 경우 `get_job().inputs` / `get_job().outputs`에 직접 URN을 추가하는 방식을 병행하세요.
- **`DATAHUB_SILENT_FAIL=true` (기본값)** 상태에서는 DataHub GMS가 내려가 있어도 MS 서비스에 영향을 주지 않습니다.
