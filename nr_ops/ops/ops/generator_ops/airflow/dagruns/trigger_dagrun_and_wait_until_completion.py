import itertools
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
from nr_ops.ops.ops.generator_ops.airflow.dagruns.get_dagrun import (
    AirflowDagRunGetDagRunOp,
)
from nr_ops.ops.ops.generator_ops.airflow.dagruns.trigger_dagrun import (
    AirflowDagRunTriggerDagRunOp,
    AirflowDagRunTriggerDagRunOpConfigModel,
)

logger = logging.getLogger(__name__)


class AirflowDagRunTriggerDagAndWaitUntilCompletionOpConfigModel(BaseOpConfigModel):
    trigger_dagrun_config: AirflowDagRunTriggerDagRunOpConfigModel
    poll_interval: float

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowDagRunTriggerDagAndWaitUntilCompletionOpMetadataModel(BaseOpMetadataModel):
    etl_request_start_ts: StrictStr
    etl_response_end_ts: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowDagRunTriggerDagAndWaitUntilCompletionOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowDagRunTriggerDagAndWaitUntilCompletionOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.airflow.dagruns.trigger_dagrun_and_wait_until_completion"
    OP_CONFIG_MODEL = AirflowDagRunTriggerDagAndWaitUntilCompletionOpConfigModel
    OP_METADATA_MODEL = AirflowDagRunTriggerDagAndWaitUntilCompletionOpMetadataModel
    OP_AUDIT_MODEL = AirflowDagRunTriggerDagAndWaitUntilCompletionOpAuditModel

    templated_fields = None

    def __init__(
        self,
        trigger_dagrun_config: Dict[str, Any],
        poll_interval: float,
        **kwargs,
    ):
        """."""
        self.trigger_dagrun_config = trigger_dagrun_config
        self.poll_interval = poll_interval

        self.templated_fields = kwargs.get("templated_fields", [])

        self.trigger_dagrun_config_model = AirflowDagRunTriggerDagRunOpConfigModel(
            **trigger_dagrun_config
        )

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.get_connector(
            op_id=self.trigger_dagrun_config_model.http_conn_id
        )

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"AirflowDagRunTriggerDagAndWaitUntilCompletionOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="AirflowDagRunTriggerDagAndWaitUntilCompletionOp.run:",
        )

        logger.info(
            f"AirflowDagRunTriggerDagAndWaitUntilCompletionOp.run: Initializing and "
            f"running AirflowDagRunTriggerDagRunOp."
        )

        trigger_dagrun_op = AirflowDagRunTriggerDagRunOp(**self.trigger_dagrun_config)
        trigger_dagrun_op_msg: OpMsg = list(trigger_dagrun_op.run(time_step=time_step))[
            0
        ]

        logger.info(
            f"AirflowDagRunTriggerDagAndWaitUntilCompletionOp.run: Done running "
            f"AirflowDagRunTriggerDagRunOp, "
            f"returned {trigger_dagrun_op_msg.data['status_code']=}."
        )

        if trigger_dagrun_op_msg.data["status_code"] != 200:
            raise Exception(
                f"Trigger dagrun did not return 200. "
                f"{trigger_dagrun_op_msg.data['status_code']=} and "
                f"{trigger_dagrun_op_msg.data=}"
            )

        logger.info(
            f"AirflowDagRunTriggerDagAndWaitUntilCompletionOp.run: Initializing "
            f"AirflowDagRunGetDagRunOp."
        )

        get_dagrun_op = AirflowDagRunGetDagRunOp(
            http_conn_id=self.trigger_dagrun_config_model.http_conn_id,
            dag_id=self.trigger_dagrun_config_model.dag_id,
            dag_run_id=trigger_dagrun_op_msg.data["response"]["dag_run_id"],
            # Accept any status code since status codes are handled within this op.
            accepted_status_codes=None,
        )

        logger.info(
            f"AirflowDagRunTriggerDagAndWaitUntilCompletionOp.run: Successfully "
            f"initialized AirflowDagRunGetDagRunOp."
        )

        for wait_iteration in itertools.count(1):
            logger.info(
                f"AirflowDagRunTriggerDagAndWaitUntilCompletionOp.run: Running "
                f"{wait_iteration=} (1-indexed). Starting off with waiting for "
                f"{self.poll_interval=} seconds."
            )

            time.sleep(self.poll_interval)

            logger.info(
                f"AirflowDagRunTriggerDagAndWaitUntilCompletionOp.run: Running "
                f"AirflowDagRunGetDagRunOp for {wait_iteration=} (1-indexed)."
            )

            get_dagrun_op_msg: OpMsg = list(get_dagrun_op.run(time_step=time_step))[0]

            if get_dagrun_op_msg.data["status_code"] != 200:
                raise Exception(
                    f"Get dagrun did not return 200. "
                    f"{get_dagrun_op_msg.data['status_code']=} and "
                    f"{get_dagrun_op_msg.data=}"
                )

            if get_dagrun_op_msg.data["response"]["state"] in ["queued", "running"]:
                logger.info(
                    f"AirflowDagRunTriggerDagAndWaitUntilCompletionOp.run: "
                    f"AirflowDagRunGetDagRunOp for {wait_iteration=} (1-indexed) "
                    f"returned {get_dagrun_op_msg.data['response']['state']=}. "
                )
                continue

            elif get_dagrun_op_msg.data["response"]["state"] in ["success", "failed"]:
                logger.info(
                    f"AirflowDagRunTriggerDagAndWaitUntilCompletionOp.run: "
                    f"AirflowDagRunGetDagRunOp for {wait_iteration=} (1-indexed) "
                    f"returned {get_dagrun_op_msg.data['response']['state']=}. "
                    f"DAGRun has completed. Yielding get_dagrun_op_msg."
                )
                yield get_dagrun_op_msg
                # Must return otherwise the generator will continue to infinity.
                return

            else:
                raise NotImplementedError(
                    f"Unrecognized state {get_dagrun_op_msg.data['response']['state']=}"
                )
