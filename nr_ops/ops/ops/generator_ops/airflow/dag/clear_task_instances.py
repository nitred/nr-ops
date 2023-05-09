import logging
import time
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple

import pandas as pd
from pydantic import StrictBool, StrictStr, conlist, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.http import HTTPConnOp

logger = logging.getLogger(__name__)


class AirflowDagClearTaskInstancesOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    dag_id: StrictStr
    dag_run_id: StrictStr
    task_ids: conlist(StrictStr, min_items=1)
    only_failed: StrictBool
    only_running: StrictBool
    reset_dag_runs: StrictBool
    include_etl_metadata: StrictBool = False
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None
    requests_kwargs: Optional[Dict[str, Any]] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowDagClearTaskInstancesOpMetadataModel(BaseOpMetadataModel):
    etl_request_start_ts: StrictStr
    etl_response_end_ts: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowDagClearTaskInstancesOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowDagClearTaskInstancesOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.airflow.dag.clear_task_instances"
    OP_CONFIG_MODEL = AirflowDagClearTaskInstancesOpConfigModel
    OP_METADATA_MODEL = AirflowDagClearTaskInstancesOpMetadataModel
    OP_AUDIT_MODEL = AirflowDagClearTaskInstancesOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        dag_id: str,
        dag_run_id: str,
        task_ids: List[str],
        only_failed: bool,
        only_running: bool,
        reset_dag_runs: bool,
        include_etl_metadata: bool = False,
        accepted_status_codes: Optional[List[int]] = None,
        requests_kwargs: Optional[Dict[str, any]] = None,
        **kwargs,
    ):
        """."""

        self.http_conn_id = http_conn_id
        self.dag_id = dag_id
        self.dag_run_id = dag_run_id
        self.task_ids = task_ids
        self.only_failed = only_failed
        self.only_running = only_running
        self.reset_dag_runs = reset_dag_runs
        self.accepted_status_codes = accepted_status_codes
        self.include_etl_metadata = include_etl_metadata
        self.requests_kwargs = requests_kwargs if requests_kwargs is not None else {}

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.get_connector(op_id=self.http_conn_id)

    def clear(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """."""
        url = f"{self.http_conn.base_url}/api/v1/dags/{self.dag_id}/clearTaskInstances"

        logger.info(
            f"AirflowDagClearTaskInstancesOp.dagrun_trigger: Clearing DAG Run with "
            f"{self.dag_id=} & {self.dag_run_id=} & {self.task_ids} with {url=}"
        )

        etl_request_start_ts = pd.Timestamp.now(tz="UTC").isoformat()

        payload = {
            "dag_run_id": self.dag_run_id,
            "task_ids": self.task_ids,
            "only_failed": self.only_failed,
            "only_running": self.only_running,
            "reset_dag_runs": self.reset_dag_runs,
            "dry_run": False,
        }

        status_code, response_json = self.http_conn.call(
            method="post",
            url=url,
            requests_kwargs={
                "headers": {
                    "Content-Type": "application/json",
                },
                "json": payload,
                **self.requests_kwargs,
            },
            accepted_status_codes=self.accepted_status_codes,
            return_type="json",
        )
        etl_response_end_ts = pd.Timestamp.now(tz="UTC").isoformat()

        etl_metadata_json = {
            "etl_request_start_ts": etl_request_start_ts,
            "etl_response_end_ts": etl_response_end_ts,
        }

        output_json = {
            "status_code": status_code,
            "response": response_json,
        }

        return etl_metadata_json, output_json

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"AirflowDagClearTaskInstancesOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="AirflowDagClearTaskInstancesOp.run:",
        )

        logger.info(f"AirflowDagClearTaskInstancesOp.run: Yielding results for page")

        etl_metadata_json, output_json = self.clear()

        if self.include_etl_metadata:
            output_json["etl_metadata"] = etl_metadata_json

        yield OpMsg(
            data=output_json,
            metadata=AirflowDagClearTaskInstancesOpMetadataModel(
                **etl_metadata_json,
            ),
            audit=AirflowDagClearTaskInstancesOpAuditModel(),
        )
