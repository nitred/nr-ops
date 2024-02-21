import logging
from io import StringIO
from typing import Any, Generator, Literal, Optional

import numpy as np
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


class EvalExprConditionalOpConfigModel(BaseOpConfigModel):
    conditional_expr: StrictStr
    on_true_behavior: Literal["yield_input", "yield_output", "consume"]
    on_false_behavior: Literal["yield_input", "yield_output", "consume"]
    expr: StrictStr
    msg_var_name: Optional[StrictStr] = None
    time_step_var_name: Optional[StrictStr] = None
    op_manager_var_name: Optional[StrictStr] = None
    log_output: StrictBool = False
    iterate_over_output: StrictBool = False

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class EvalExprConditionalOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class EvalExprConditionalOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class EvalExprConditionalOp(BaseGeneratorOp):
    """Store output of eval_expr as metadata. Return input data as is."""

    OP_TYPE = "generator.eval_expr_conditional"
    OP_CONFIG_MODEL = EvalExprConditionalOpConfigModel
    OP_METADATA_MODEL = EvalExprConditionalOpMetadataModel
    OP_AUDIT_MODEL = EvalExprConditionalOpAuditModel

    templated_fields = None

    def __init__(
        self,
        conditional_expr: str,
        on_true_behavior: str,
        on_false_behavior: str,
        expr: str,
        msg_var_name: Optional[str] = None,
        time_step_var_name: Optional[str] = None,
        op_manager_var_name: Optional[str] = None,
        log_output: bool = False,
        iterate_over_output: bool = False,
        **kwargs,
    ):
        """."""
        self.conditional_expr = conditional_expr
        self.on_true_behavior = on_true_behavior
        self.on_false_behavior = on_false_behavior
        self.expr = expr
        self.msg_var_name = "msg" if msg_var_name is None else msg_var_name
        self.time_step_var_name = (
            "time_step" if time_step_var_name is None else time_step_var_name
        )
        self.op_manager_var_name = (
            "op_manager" if op_manager_var_name is None else op_manager_var_name
        )
        self.log_output = log_output
        self.iterate_over_output = iterate_over_output

        self.templated_fields = kwargs.get("templated_fields", [])

        self.op_manager = get_global_op_manager()

    def eval_output(self, time_step: TimeStep, msg: OpMsg):
        """."""
        output = eval(
            self.expr,
            {
                **EVAL_GLOBALS,
                self.msg_var_name: msg,
                self.time_step_var_name: time_step,
                self.op_manager_var_name: self.op_manager,
            },
        )

        logger.info(
            f"EvalExprConditionalOp.run: Evaluated output expression: "
            f"{type(output)=}"
        )

        # Special logging for pandas DataFrames
        if isinstance(output, pd.DataFrame):
            buf = StringIO()
            output.info(buf=buf)
            buf.seek(0)
            logger.info(f"EvalExprConditionalOp.run: DataFrame info:\n{buf.read()}")

        if self.log_output:
            logger.info(f"EvalExprConditionalOp.run: Logging output:\n{output=}")

        return output

    def behavior_consume(self):
        """."""
        logger.info(
            f"EvalExprConditionalOp.run: "
            f"Consuming and returning i.e. stopping the generator."
        )
        return

    def behavior_yield_input(self, input_msg: OpMsg):
        """."""
        logger.info(f"EvalExprConditionalOp.run: Yielding input msg")
        yield OpMsg(
            data=input_msg.data,
            metadata=EvalExprConditionalOpMetadataModel(),
            audit=EvalExprConditionalOpAuditModel(),
        )

    def behavior_yield_output(self, output: Any):
        """."""
        if self.iterate_over_output:
            logger.info(
                f"EvalExprConditionalOp.run: Iterating over output of "
                f"{type(output)=} and yielding items"
            )

            for item_i, item in enumerate(output):
                logger.info(
                    f"EvalExprConditionalOp.run: "
                    f"Iterating over output of {type(output)=}, "
                    f"yielding {item_i=} of {type(item)=}"
                )
                yield OpMsg(
                    data=item,
                    metadata=EvalExprConditionalOpMetadataModel(),
                    audit=EvalExprConditionalOpAuditModel(),
                )
        else:
            logger.info(
                f"EvalExprConditionalOp.run: Yielding output of {type(output)=}"
            )
            yield OpMsg(
                data=output,
                metadata=EvalExprConditionalOpMetadataModel(),
                audit=EvalExprConditionalOpAuditModel(),
            )

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"EvalExprConditionalOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="EvalExprConditionalOp.run:"
        )

        logger.info(
            f"EvalExprOp.run: Evaluating conditional expression: "
            f"{self.conditional_expr=}"
        )

        conditional_output = eval(
            self.conditional_expr,
            {
                **EVAL_GLOBALS,
                self.msg_var_name: msg,
                self.time_step_var_name: time_step,
                self.op_manager_var_name: self.op_manager,
            },
        )

        logger.info(
            f"EvalExprConditionalOp.run: Evaluated conditional expression: "
            f"{type(conditional_output)=}"
        )

        if not isinstance(conditional_output, (bool, np.bool_)):
            raise TypeError(
                f"EvalExprConditionalOp.run: Conditional expression must evaluate to "
                f"bool or np.bool_. Got {type(conditional_output)=}"
            )

        if isinstance(conditional_output, np.bool_):
            conditional_output = bool(conditional_output)

        logger.info(
            f"EvalExprConditionalOp.run: Evaluated conditional expression: "
            f"{conditional_output=}"
        )

        if conditional_output is True:
            logger.info(
                f"EvalExprConditionalOp.run: {conditional_output=}. "
                f"Executing {self.on_true_behavior=}."
            )
            if self.on_true_behavior == "consume":
                return self.behavior_consume()
            elif self.on_true_behavior == "yield_input":
                yield from self.behavior_yield_input(msg)
            elif self.on_true_behavior == "yield_output":
                output = self.eval_output(time_step, msg)
                yield from self.behavior_yield_output(output)
            else:
                raise NotImplementedError()
        else:
            logger.info(
                f"EvalExprConditionalOp.run: {conditional_output=}. "
                f"Executing {self.on_false_behavior=}."
            )
            if self.on_false_behavior == "consume":
                return self.behavior_consume()
            elif self.on_false_behavior == "yield_input":
                yield from self.behavior_yield_input(msg)
            elif self.on_false_behavior == "yield_output":
                output = self.eval_output(time_step, msg)
                yield from self.behavior_yield_output(output)
            else:
                raise NotImplementedError()
