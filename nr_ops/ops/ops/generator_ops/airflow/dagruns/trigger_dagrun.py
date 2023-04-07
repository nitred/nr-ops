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


class AirflowDagRunTriggerDagRunOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    dag_id: StrictStr
    logical_date: Optional[StrictStr] = None
    dag_run_id: Optional[StrictStr] = None
    conf: Optional[Dict[str, Any]] = None
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None
    include_etl_metadata: StrictBool = False
    requests_kwargs: Optional[Dict[str, Any]] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    # # Validate that both dag_run_id and logical_date are not None
    # @root_validator(pre=False)
    # def validate_dag_run_id_and_logical_date(
    #     cls, values: Dict[str, Any]
    # ) -> Dict[str, Any]:
    #     dag_run_id = values.get("dag_run_id")
    #     logical_date = values.get("logical_date")
    #     if dag_run_id is None and logical_date is None:
    #         raise ValueError(
    #             f"Both {dag_run_id=} and {logical_date=} cannot be None. "
    #             f"Please provide one of them."
    #         )
    #     return values


class AirflowDagRunTriggerDagRunOpMetadataModel(BaseOpMetadataModel):
    etl_request_start_ts: StrictStr
    etl_response_end_ts: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowDagRunTriggerDagRunOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowDagRunTriggerDagRunOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.airflow.dagruns.trigger_dagrun"
    OP_CONFIG_MODEL = AirflowDagRunTriggerDagRunOpConfigModel
    OP_METADATA_MODEL = AirflowDagRunTriggerDagRunOpMetadataModel
    OP_AUDIT_MODEL = AirflowDagRunTriggerDagRunOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        dag_id: str,
        logical_date: Optional[str] = None,
        conf: Optional[Dict[str, Any]] = None,
        dag_run_id: Optional[str] = None,
        accepted_status_codes: Optional[List[int]] = None,
        include_etl_metadata: bool = False,
        requests_kwargs: Optional[Dict[str, any]] = None,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.dag_id = dag_id
        self.logical_date = logical_date
        self.conf = conf if conf is not None else {}
        self.dag_run_id = dag_run_id
        self.accepted_status_codes = accepted_status_codes
        self.include_etl_metadata = include_etl_metadata
        self.requests_kwargs = requests_kwargs if requests_kwargs is not None else {}

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.get_connector(op_id=self.http_conn_id)

    def dagrun_trigger(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """."""
        url = f"{self.http_conn.base_url}/api/v1/dags/{self.dag_id}/dagRuns"
        logger.info(
            f"AirflowDagRunTriggerDagRunOp.dagrun_trigger: Triggering DAG Run with "
            f"{self.dag_id=} with {url=}"
        )

        etl_request_start_ts = pd.Timestamp.now(tz="UTC").isoformat()

        payload = {"conf": self.conf}
        if self.dag_run_id is not None:
            payload["dag_run_id"] = self.dag_run_id
        if self.logical_date is not None:
            payload["logical_date"] = self.logical_date

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
        logger.info(f"AirflowDagRunTriggerDagRunOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="AirflowDagRunTriggerDagRunOp.run:"
        )

        logger.info(f"AirflowDagRunTriggerDagRunOp.run: Yielding results for page")

        etl_metadata_json, output_json = self.dagrun_trigger()

        if self.include_etl_metadata:
            output_json["etl_metadata"] = etl_metadata_json

        yield OpMsg(
            data=output_json,
            metadata=AirflowDagRunTriggerDagRunOpMetadataModel(
                **etl_metadata_json,
            ),
            audit=AirflowDagRunTriggerDagRunOpAuditModel(),
        )
