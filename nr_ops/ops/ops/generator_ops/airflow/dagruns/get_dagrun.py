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


class AirflowDagRunGetDagRunOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    dag_id: StrictStr
    dag_run_id: StrictStr
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None
    include_etl_metadata: StrictBool = False
    requests_kwargs: Optional[Dict[str, Any]] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowDagRunGetDagRunOpMetadataModel(BaseOpMetadataModel):
    etl_request_start_ts: StrictStr
    etl_response_end_ts: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowDagRunGetDagRunOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowDagRunGetDagRunOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.airflow.dagruns.get_dagrun"
    OP_CONFIG_MODEL = AirflowDagRunGetDagRunOpConfigModel
    OP_METADATA_MODEL = AirflowDagRunGetDagRunOpMetadataModel
    OP_AUDIT_MODEL = AirflowDagRunGetDagRunOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        dag_id: str,
        dag_run_id: str,
        accepted_status_codes: Optional[List[int]] = None,
        include_etl_metadata: bool = False,
        requests_kwargs: Optional[Dict[str, any]] = None,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.dag_id = dag_id
        self.dag_run_id = dag_run_id
        self.accepted_status_codes = accepted_status_codes
        self.include_etl_metadata = include_etl_metadata
        self.requests_kwargs = requests_kwargs if requests_kwargs is not None else {}

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.get_connector(op_id=self.http_conn_id)

    def get_dagrun(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """."""
        url = (
            f"{self.http_conn.base_url}/api/v1/"
            f"dags/{self.dag_id}/"
            f"dagRuns/{self.dag_run_id}"
        )
        logger.info(
            f"AirflowDagRunGetDagRunOp.get_dagrun: Getting DAGRun with "
            f"{self.dag_id=} and {self.dag_run_id=} with {url=}"
        )

        etl_request_start_ts = pd.Timestamp.now(tz="UTC").isoformat()

        status_code, response_json = self.http_conn.call(
            method="get",
            url=url,
            requests_kwargs={
                "headers": {
                    "Content-Type": "application/json",
                },
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
        logger.info(f"AirflowDagRunGetDagRunOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="AirflowDagRunGetDagRunOp.run:"
        )

        logger.info(f"AirflowDagRunGetDagRunOp.run: Yielding results for page")

        etl_metadata_json, output_json = self.get_dagrun()

        if self.include_etl_metadata:
            output_json["etl_metadata"] = etl_metadata_json

        yield OpMsg(
            data=output_json,
            metadata=AirflowDagRunGetDagRunOpMetadataModel(
                **etl_metadata_json,
            ),
            audit=AirflowDagRunGetDagRunOpAuditModel(),
        )
