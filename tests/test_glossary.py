"""
E-commerce 파이프라인 Glossary Term 등록 테스트.

수행 내용:
  1. GlossaryNode  : Ecommerce 도메인 노드 생성
  2. GlossaryTerms : 컬럼별 비즈니스 용어 생성
  3. SchemaMetadata: 각 Dataset 스키마(컬럼) 등록
  4. 컬럼 ↔ GlossaryTerm 연결

등록 대상 Dataset:
  raw.orders / raw.products / raw.customers
  staging.orders
  dw.order_items
  analytics.customer_orders / analytics.daily_summary / analytics.tmp_top_customers

실행 방법:
  pytest tests/test_glossary.py -v -s
"""
from __future__ import annotations

import hashlib
import os
import time

import pytest

# ── 환경 설정 ─────────────────────────────────────────────────────────────────

DATAHUB_GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
DATAHUB_ENV     = os.environ.get("DATAHUB_ENV", "PROD")
PLATFORM        = "glue"
AUDIT_ACTOR     = "urn:li:corpuser:datahub"


def _datahub_reachable() -> bool:
    try:
        import requests
        return requests.get(f"{DATAHUB_GMS_URL}/config", timeout=3).status_code == 200
    except Exception:
        return False


requires_datahub = pytest.mark.skipif(
    not _datahub_reachable(),
    reason=f"DataHub GMS({DATAHUB_GMS_URL})에 연결할 수 없습니다.",
)


# ── URN 헬퍼 ──────────────────────────────────────────────────────────────────

def _dataset_urn(db: str, table: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{PLATFORM},{db}.{table},{DATAHUB_ENV})"


def _term_urn(term: str) -> str:
    return f"urn:li:glossaryTerm:{term}"


def _node_urn(node: str) -> str:
    return f"urn:li:glossaryNode:{node}"


def _audit() -> "AuditStampClass":
    from datahub.metadata.schema_classes import AuditStampClass
    return AuditStampClass(time=int(time.time() * 1000), actor=AUDIT_ACTOR)


def _schema_hash(fields: list[tuple]) -> str:
    raw = ",".join(f"{n}:{t}" for n, t, *_ in fields)
    return hashlib.md5(raw.encode()).hexdigest()


# ── Glossary 정의 ─────────────────────────────────────────────────────────────

GLOSSARY_NODE = "Ecommerce"

GLOSSARY_TERMS: dict[str, str] = {
    "OrderId":         "주문 고유 식별자",
    "CustomerId":      "고객 고유 식별자",
    "ProductId":       "상품 고유 식별자",
    "OrderAmount":     "주문 금액 (원화)",
    "CreatedAt":       "레코드 생성 일시",
    "OrderStatus":     "주문 상태 (active / cancelled 등)",
    "ProductName":     "상품명",
    "ProductCategory": "상품 카테고리",
    "CustomerName":    "고객 이름",
    "CustomerEmail":   "고객 이메일 주소",
    "OrderCount":      "고객 또는 기간별 주문 건수",
    "TotalAmount":     "고객별 총 주문 금액",
    "OrderDate":       "주문 일자 (DATE)",
    "Revenue":         "기간·카테고리별 매출액",
    "CategoryRevenue": "카테고리별 매출액",
}


# ── Dataset 스키마 정의 (컬럼명, 타입, GlossaryTerm) ─────────────────────────
#   형식: (field_name, type_class, term_key | None)

def _str():
    from datahub.metadata.schema_classes import SchemaFieldDataTypeClass, StringTypeClass
    return SchemaFieldDataTypeClass(type=StringTypeClass())

def _num():
    from datahub.metadata.schema_classes import NumberTypeClass, SchemaFieldDataTypeClass
    return SchemaFieldDataTypeClass(type=NumberTypeClass())

def _date():
    from datahub.metadata.schema_classes import DateTypeClass, SchemaFieldDataTypeClass
    return SchemaFieldDataTypeClass(type=DateTypeClass())

def _time():
    from datahub.metadata.schema_classes import SchemaFieldDataTypeClass, TimeTypeClass
    return SchemaFieldDataTypeClass(type=TimeTypeClass())


DATASET_SCHEMAS: dict[str, list[tuple]] = {
    # (dataset_urn, [(field_name, type_fn, term_key)])
    "raw.orders": [
        ("order_id",    _str,  "OrderId"),
        ("customer_id", _str,  "CustomerId"),
        ("product_id",  _str,  "ProductId"),
        ("amount",      _num,  "OrderAmount"),
        ("created_at",  _time, "CreatedAt"),
        ("status",      _str,  "OrderStatus"),
    ],
    "raw.products": [
        ("product_id", _str, "ProductId"),
        ("name",       _str, "ProductName"),
        ("category",   _str, "ProductCategory"),
    ],
    "raw.customers": [
        ("customer_id", _str, "CustomerId"),
        ("name",        _str, "CustomerName"),
        ("email",       _str, "CustomerEmail"),
    ],
    "staging.orders": [
        ("order_id",    _str,  "OrderId"),
        ("customer_id", _str,  "CustomerId"),
        ("product_id",  _str,  "ProductId"),
        ("amount",      _num,  "OrderAmount"),
        ("created_at",  _time, "CreatedAt"),
    ],
    "dw.order_items": [
        ("order_id",      _str,  "OrderId"),
        ("customer_id",   _str,  "CustomerId"),
        ("product_name",  _str,  "ProductName"),
        ("category",      _str,  "ProductCategory"),
        ("amount",        _num,  "OrderAmount"),
        ("created_at",    _time, "CreatedAt"),
    ],
    "analytics.customer_orders": [
        ("customer_id",  _str, "CustomerId"),
        ("name",         _str, "CustomerName"),
        ("email",        _str, "CustomerEmail"),
        ("order_count",  _num, "OrderCount"),
        ("total_amount", _num, "TotalAmount"),
    ],
    "analytics.daily_summary": [
        ("order_date",  _date, "OrderDate"),
        ("category",    _str,  "ProductCategory"),
        ("order_count", _num,  "OrderCount"),
        ("revenue",     _num,  "Revenue"),
    ],
    "analytics.tmp_top_customers": [
        ("customer_id",      _str, "CustomerId"),
        ("name",             _str, "CustomerName"),
        ("total_amount",     _num, "TotalAmount"),
        ("category",         _str, "ProductCategory"),
        ("category_revenue", _num, "CategoryRevenue"),
    ],
}


# ── emit 헬퍼 ─────────────────────────────────────────────────────────────────

def _emit_glossary_node(emitter) -> None:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import GlossaryNodeInfoClass

    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=_node_urn(GLOSSARY_NODE),
        aspect=GlossaryNodeInfoClass(
            definition=f"{GLOSSARY_NODE} 도메인 비즈니스 용어 모음",
            name=GLOSSARY_NODE,
        ),
    ))
    print(f"  [node] {GLOSSARY_NODE}")


def _emit_glossary_terms(emitter) -> None:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import GlossaryTermInfoClass

    for term, definition in GLOSSARY_TERMS.items():
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=_term_urn(term),
            aspect=GlossaryTermInfoClass(
                definition=definition,
                name=term,
                termSource="INTERNAL",
                parentNode=_node_urn(GLOSSARY_NODE),
            ),
        ))
        print(f"  [term] {term}: {definition}")


def _emit_dataset_schemas(emitter) -> None:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        GlossaryTermAssociationClass,
        GlossaryTermsClass,
        OtherSchemaClass,
        SchemaFieldClass,
        SchemaMetadataClass,
    )

    audit = _audit()

    for dataset_key, fields in DATASET_SCHEMAS.items():
        db, table = dataset_key.split(".", 1)
        dataset_urn = _dataset_urn(db, table)

        schema_fields = []
        for field_name, type_fn, term_key in fields:
            glossary_terms = None
            if term_key:
                glossary_terms = GlossaryTermsClass(
                    terms=[GlossaryTermAssociationClass(urn=_term_urn(term_key))],
                    auditStamp=audit,
                )
            schema_fields.append(SchemaFieldClass(
                fieldPath=field_name,
                type=type_fn(),
                nativeDataType=type_fn().__class__.__name__.replace("SchemaFieldDataTypeClass", ""),
                glossaryTerms=glossary_terms,
            ))

        schema = SchemaMetadataClass(
            schemaName=f"{db}.{table}",
            platform=f"urn:li:dataPlatform:{PLATFORM}",
            version=0,
            hash=_schema_hash(fields),
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=schema_fields,
            created=audit,
            lastModified=audit,
        )

        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema,
        ))

        term_summary = ", ".join(
            f"{f[0]}→{f[2]}" for f in fields if f[2]
        )
        print(f"  [schema] {dataset_key}  ({term_summary})")


# ── 테스트 ────────────────────────────────────────────────────────────────────

@requires_datahub
class TestGlossaryRegistration:

    def test_register_glossary_node_and_terms(self):
        """GlossaryNode + GlossaryTerm 15개 등록

        DataHub UI 확인:
          Govern → Glossary → Ecommerce
        """
        from datahub.emitter.rest_emitter import DatahubRestEmitter

        emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS_URL)

        print(f"\n[glossary] node + terms 등록 시작")
        _emit_glossary_node(emitter)
        _emit_glossary_terms(emitter)
        print(f"[glossary] 완료: {len(GLOSSARY_TERMS)}개 term 등록")

    def test_register_dataset_schemas_with_terms(self):
        """Dataset 스키마 등록 + 컬럼별 GlossaryTerm 연결

        DataHub UI 확인:
          Dataset → Schema 탭 → 각 컬럼 옆 tag에 term 표시
        """
        from datahub.emitter.rest_emitter import DatahubRestEmitter

        emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS_URL)

        print(f"\n[schema] dataset schema + glossary term 연결 시작")
        _emit_dataset_schemas(emitter)
        print(f"[schema] 완료: {len(DATASET_SCHEMAS)}개 dataset 등록")

    def test_full_glossary_pipeline(self):
        """전체 glossary 등록 순서대로 실행

        DataHub UI 확인 순서:
          1. Govern → Glossary → Ecommerce  (node + terms)
          2. Search → 각 dataset → Schema 탭  (컬럼 + term 연결)
        """
        from datahub.emitter.rest_emitter import DatahubRestEmitter

        emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS_URL)

        print(f"\n[full-glossary] DataHub UI: {DATAHUB_GMS_URL}")
        print("[full-glossary] 1. GlossaryNode + Terms 등록")
        _emit_glossary_node(emitter)
        _emit_glossary_terms(emitter)

        print("[full-glossary] 2. Dataset Schema + GlossaryTerm 연결")
        _emit_dataset_schemas(emitter)

        print(f"\n[full-glossary] 완료")
        print(f"  - GlossaryNode : 1개 ({GLOSSARY_NODE})")
        print(f"  - GlossaryTerms: {len(GLOSSARY_TERMS)}개")
        print(f"  - Datasets     : {len(DATASET_SCHEMAS)}개")
        total_columns = sum(len(f) for f in DATASET_SCHEMAS.values())
        print(f"  - Columns      : {total_columns}개")
