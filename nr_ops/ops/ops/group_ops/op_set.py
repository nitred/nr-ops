import logging
from typing import Generator, List, Optional

from pydantic import conlist

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGroupOp, BaseOp, BaseOpConfigModel
from nr_ops.ops.op import Op, OpModel

logger = logging.getLogger(__name__)


class OpSetGroupOpConfigModel(BaseOpConfigModel):
    ops: conlist(OpModel, min_items=1)

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    def get_self_and_child_op_ids(self) -> Generator[str, None, None]:
        """."""
        for op in self.ops:
            yield from op.get_self_and_child_op_ids()


class OpSetGroupOpMetadataModel(BaseOpMetadataModel):
    output_metadata: BaseOpMetadataModel

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OpSetGroupOpAuditModel(BaseOpAuditModel):
    output_audit: BaseOpAuditModel

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OpSetGroupOp(BaseGroupOp):
    OP_TYPE = "group.op_set"
    OP_CONFIG_MODEL = OpSetGroupOpConfigModel
    OP_METADATA_MODEL = OpSetGroupOpMetadataModel
    OP_AUDIT_MODEL = OpSetGroupOpAuditModel

    templated_fields = None

    def __init__(self, ops: List[BaseOp], **kwargs):
        self.ops = [Op(**op) for op in ops]

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[Optional[OpMsg], None, None]:
        """."""
        logger.info(f"OpSetGroupOp.run: Running")

        n_ops = len(self.ops)
        for op_i, op in enumerate(self.ops):
            # Exhaust each op generator.
            logger.info(
                f"OpSetGroupOp.run: Iterating over Ops, current Op "
                f"{op_i + 1}/{n_ops} (1-index) | {op.op_type=} | {op.op_id=}"
            )
            for output_msg_i, output_msg in enumerate(op.run(time_step, msg)):
                logger.info(
                    f"OpSetGroupOp.run: Consuming (not yielding) output_msg from Op "
                    f"{op_i + 1}/{n_ops} (1-index) | {op.op_type=} | {op.op_id=} | "
                    f"{output_msg_i} | {type(output_msg.data)=}"
                )
                pass

        # Yield original message to keep the chain going.
        logger.info(f"OpSetGroupOp.run: Yielding original input message {type(msg)=}.")
        yield msg
