import logging
from typing import Generator, List, Optional

from pydantic import conlist

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_depth import BaseOpDepthModel
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
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OpSetGroupOpAuditModel(BaseOpAuditModel):
    pass

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
        self, depth: BaseOpDepthModel, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[Optional[OpMsg], None, None]:
        """."""
        logger.info(f"OpSetGroupOp.run: Running")

        for op in self.ops:
            # Exhaust each op generator.
            for _ in op.run(depth, time_step, msg):
                pass

        # Yield original message to keep the chain going.
        yield msg
