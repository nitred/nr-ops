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


class OpFanInGroupOpConfigModel(BaseOpConfigModel):
    ops: conlist(OpModel, min_items=1)

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    def get_self_and_child_op_ids(self) -> Generator[str, None, None]:
        """."""
        for op in self.ops:
            yield from op.get_self_and_child_op_ids()


class OpFanInGroupOpMetadataModel(BaseOpMetadataModel):
    output_metadata: BaseOpMetadataModel

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OpFanInGroupOpAuditModel(BaseOpAuditModel):
    output_audit: BaseOpAuditModel

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OpFanInGroupOp(BaseGroupOp):
    """.

    This is a group op that yields the outputs of each op in the group as its output.
    - Think of it as a group of independent sources.
    - The input of this group op is passed to each op in the group.
    - Each op in the group is run to completion before the next op is run.
    - The output of each op is yielded out as the output of the group op AND not chained
      to other ops.
    """

    # Alternate names:
    # - group.group_of_generators
    # - group.group_of_sources

    OP_TYPE = "group.op_fan_in"
    OP_CONFIG_MODEL = OpFanInGroupOpConfigModel
    OP_METADATA_MODEL = OpFanInGroupOpMetadataModel
    OP_AUDIT_MODEL = OpFanInGroupOpAuditModel

    templated_fields = None

    def __init__(self, ops: List[BaseOp], **kwargs):
        self.ops = [Op(**op) for op in ops]

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[Optional[OpMsg], None, None]:
        """."""
        logger.info(f"OpFanInGroupOp.run: Running")

        n_ops = len(self.ops)
        for op_i, op in enumerate(self.ops):
            op_i += 1
            # Exhaust each op generator.
            # Pass the input message of this group op to each op.
            for output_msg_i, output_msg in enumerate(op.run(time_step, msg)):
                logger.info(
                    f"OpFanInGroupOp.run: Yielding output_msg from Op"
                    f"{op_i=}/{n_ops=} (1-index) | {op.op_type=} | {op.op_id=} | "
                    f"{output_msg_i} | {type(output_msg.data)=}"
                )

                # INHERIT metadata and audit from the input message.
                yield OpMsg(
                    data=output_msg.data,
                    metadata=output_msg.metadata,
                    audit=output_msg.audit,
                )
