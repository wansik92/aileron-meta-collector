from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from .context import JobContext

logger = logging.getLogger(__name__)


def inject_to_airflow(job: "JobContext", airflow_context: Dict[str, Any]) -> None:
    """aileron이 수집한 lineage를 Airflow task의 inlets/outlets에 자동 주입합니다.

    DataHub Airflow plugin이 task 완료 시 task._inlets/_outlets를 읽어
    DataJobInputOutput aspect로 emit합니다.

    Args:
        job:              aileron JobContext (inputs/outputs 포함)
        airflow_context:  Airflow task context dict (**context 또는 provide_context=True)

    사용 예::

        def my_etl(**context):
            with datahub_job("my-etl", flow="pipeline", airflow_context=context) as job:
                ...
            # 자동으로 task._inlets/task._outlets에 주입됨
    """
    if not airflow_context:
        return

    task = airflow_context.get("task")
    if task is None:
        logger.debug("[aileron] airflow_context에 task가 없어 lineage 주입 skip")
        return

    if not job.inputs and not job.outputs:
        logger.debug("[aileron] 주입할 inputs/outputs 없음 — skip")
        return

    try:
        from datahub_airflow_plugin.entities import Urn

        input_urns  = [Urn(u) for u in job.inputs]
        output_urns = [Urn(u) for u in job.outputs]

        # DataHub Airflow plugin은 _inlets/_outlets(private)를 먼저 읽음
        if hasattr(task, "_inlets"):
            task._inlets.extend(input_urns)
            task._outlets.extend(output_urns)
        else:
            task.inlets.extend(input_urns)
            task.outlets.extend(output_urns)

        logger.info(
            "[aileron] Airflow task lineage 주입 완료 | task=%s  inputs=%d  outputs=%d",
            getattr(task, "task_id", "?"), len(input_urns), len(output_urns),
        )

    except ImportError:
        logger.warning(
            "[aileron] datahub_airflow_plugin 미설치 — Airflow lineage 주입 skip. "
            "pip install 'apache-airflow-providers-datahub'"
        )
    except Exception as e:
        logger.warning("[aileron] Airflow lineage 주입 실패 (서비스 영향 없음): %s", e)
