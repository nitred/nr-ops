import logging
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


class EvalExprAsMetadataOpConfigModel(BaseOpConfigModel):
    expr: StrictStr
    msg_var_name: Optional[StrictStr] = None
    time_step_var_name: Optional[StrictStr] = None
    op_manager_var_name: Optional[StrictStr] = None
    log_output: StrictBool = False

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class EvalExprAsMetadataOpMetadataModel(BaseOpMetadataModel):
    output_metadata: Any

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class EvalExprAsMetadataOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class EvalExprAsMetadataOp(BaseGeneratorOp):
    """Store output of eval_expr as metadata. Return input data as is."""

    OP_TYPE = "generator.eval_expr_as_metadata"
    OP_CONFIG_MODEL = EvalExprAsMetadataOpConfigModel
    OP_METADATA_MODEL = EvalExprAsMetadataOpMetadataModel
    OP_AUDIT_MODEL = EvalExprAsMetadataOpAuditModel

    templated_fields = None

    def __init__(
        self,
        expr: str,
        msg_var_name: Optional[str] = None,
        time_step_var_name: Optional[str] = None,
        op_manager_var_name: Optional[str] = None,
        log_output: StrictBool = False,
        **kwargs,
    ):
        self.expr = expr
        self.msg_var_name = "msg" if msg_var_name is None else msg_var_name
        self.time_step_var_name = (
            "time_step" if time_step_var_name is None else time_step_var_name
        )
        self.op_manager_var_name = (
            "op_manager" if op_manager_var_name is None else op_manager_var_name
        )
        self.log_output = log_output

        self.templated_fields = kwargs.get("templated_fields", [])

        self.op_manager = get_global_op_manager()

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"EvalExprAsMetadataOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="EvalExprAsMetadataOp.run:"
        )

        output = eval(
            self.expr,
            {
                **EVAL_GLOBALS,
                self.msg_var_name: msg,
                self.time_step_var_name: time_step,
                self.op_manager_var_name: self.op_manager,
            },
        )

        logger.info(f"EvalExprAsMetadataOp.run: Evaluated expression: {type(output)=}")

        # Special logging for pandas DataFrames
        if isinstance(output, pd.DataFrame):
            buf = StringIO()
            output.info(buf=buf)
            buf.seek(0)
            logger.info(f"EvalExprAsMetadataOp.run: DataFrame info:\n{buf.read()}")

        if self.log_output:
            logger.info(f"EvalExprAsMetadataOp.run: Logging output:\n{output=}")

        logger.info(
            f"EvalExprAsMetadataOp.run: STORING output of {type(output)=} as metadata "
            f"of this op. Yielding input data of this op as output."
        )

        # Yield input data as output.
        # Store output as metadata.
        yield OpMsg(
            data=msg.data,
            metadata=EvalExprAsMetadataOpMetadataModel(output_metadata=output),
            audit=EvalExprAsMetadataOpAuditModel(),
        )
