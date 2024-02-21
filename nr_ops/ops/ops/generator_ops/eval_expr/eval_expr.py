import logging
import time
from io import StringIO
from typing import Any, Dict, Generator, Optional

import pandas as pd
from pydantic import StrictBool, StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.utils.eval.eval_globals import EVAL_GLOBALS

logger = logging.getLogger(__name__)


class EvalExprOpConfigModel(BaseOpConfigModel):
    expr: StrictStr
    metadata_expr: Optional[StrictStr] = None
    msg_var_name: Optional[StrictStr] = None
    time_step_var_name: Optional[StrictStr] = None
    op_manager_var_name: Optional[StrictStr] = None
    log_output: StrictBool = False
    iterate_over_output: StrictBool = False
    msg_data_var_name: Optional[StrictStr] = None
    msg_metadata_var_name: Optional[StrictStr] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class EvalExprOpMetadataModel(BaseOpMetadataModel):
    output_metadata: Optional[Dict[str, Any]]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class EvalExprOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class EvalExprOp(BaseGeneratorOp):
    OP_TYPE = "generator.eval_expr"
    OP_CONFIG_MODEL = EvalExprOpConfigModel
    OP_METADATA_MODEL = EvalExprOpMetadataModel
    OP_AUDIT_MODEL = EvalExprOpAuditModel

    templated_fields = None

    def __init__(
        self,
        expr: str,
        metadata_expr: Optional[str] = None,
        msg_var_name: Optional[str] = None,
        time_step_var_name: Optional[str] = None,
        op_manager_var_name: Optional[str] = None,
        log_output: StrictBool = False,
        iterate_over_output: StrictBool = False,
        msg_data_var_name: Optional[str] = None,
        msg_metadata_var_name: Optional[str] = None,
        **kwargs,
    ):
        self.expr = expr
        self.metadata_expr = metadata_expr
        self.msg_var_name = "msg" if msg_var_name is None else msg_var_name
        self.time_step_var_name = (
            "time_step" if time_step_var_name is None else time_step_var_name
        )
        self.op_manager_var_name = (
            "op_manager" if op_manager_var_name is None else op_manager_var_name
        )
        self.log_output = log_output
        self.iterate_over_output = iterate_over_output
        self.msg_data_var_name = msg_data_var_name
        self.msg_metadata_var_name = msg_metadata_var_name

        self.templated_fields = kwargs.get("templated_fields", [])

        self.op_manager = get_global_op_manager()

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"EvalExprOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=time_step, msg=msg, log_prefix="EvalExprOp.run:")

        logger.info(f"EvalExprOp.run: Evaluating expression: {self.expr=}")

        start = time.time()

        params = {
            **EVAL_GLOBALS,
            self.msg_var_name: msg,
            self.time_step_var_name: time_step,
            self.op_manager_var_name: self.op_manager,
        }

        if self.msg_data_var_name:
            params[self.msg_data_var_name] = msg.data

        if self.msg_metadata_var_name:
            params[self.msg_metadata_var_name] = msg.metadata

        output = eval(self.expr, params)
        end = time.time()

        logger.info(
            f"EvalExprOp.run: Evaluated expression: {type(output)=}. "
            f"Time taken: {end - start:0.3f} seconds"
        )

        # Special logging for pandas DataFrames
        if isinstance(output, pd.DataFrame):
            buf = StringIO()
            output.info(buf=buf)
            buf.seek(0)
            logger.info(f"EvalExprOp.run: DataFrame info:\n{buf.read()}")

        if self.log_output:
            logger.info(f"EvalExprOp.run: Logging output:\n{output=}")

        if self.metadata_expr:
            output_metadata = eval(
                self.metadata_expr,
                {
                    **EVAL_GLOBALS,
                    self.msg_var_name: msg,
                    self.time_step_var_name: time_step,
                    self.op_manager_var_name: self.op_manager,
                },
            )
            logger.info(
                f"EvalExprOp.run: Evaluated metadata expression: "
                f"{type(output_metadata)=}"
            )
            if not isinstance(output_metadata, dict):
                raise ValueError(
                    f"EvalExprOp.run: Expected metadata_expr to evaluate to a dict, "
                    f"but got {type(output_metadata)=}"
                )
        else:
            output_metadata = None

        if self.iterate_over_output:
            logger.info(f"EvalExprOp.run: Iterating over output of {type(output)=}")

            for item_i, item in enumerate(output):
                logger.info(
                    f"EvalExprOp.run: Iterating over output of {type(output)=}, "
                    f"yielding {item_i=} of {type(item)=}"
                )
                yield OpMsg(
                    data=item,
                    metadata=EvalExprOpMetadataModel(output_metadata=output_metadata),
                    audit=EvalExprOpAuditModel(),
                )
        else:
            logger.info(f"EvalExprOp.run: Yielding output of {type(output)=}")
            yield OpMsg(
                data=output,
                metadata=EvalExprOpMetadataModel(output_metadata=output_metadata),
                audit=EvalExprOpAuditModel(),
            )
