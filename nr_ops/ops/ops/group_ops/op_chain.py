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


class OpChainGroupOpConfigModel(BaseOpConfigModel):
    ops: conlist(OpModel, min_items=1)

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    def get_self_and_child_op_ids(self) -> Generator[str, None, None]:
        """."""
        for op in self.ops:
            yield from op.get_self_and_child_op_ids()


class OpChainGroupOpMetadataModel(BaseOpMetadataModel):
    output_metadata: BaseOpMetadataModel

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OpChainGroupOpAuditModel(BaseOpAuditModel):
    output_audit: BaseOpAuditModel

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OpChainGroupOp(BaseGroupOp):
    OP_TYPE = "group.op_chain"
    OP_CONFIG_MODEL = OpChainGroupOpConfigModel
    OP_METADATA_MODEL = OpChainGroupOpMetadataModel
    OP_AUDIT_MODEL = OpChainGroupOpAuditModel

    templated_fields = None

    def __init__(self, ops: List[BaseOp], **kwargs):
        self.ops = [Op(**op) for op in ops]

    @staticmethod
    def get_output_msgs_from_op(
        time_step: TimeStep,
        msgs: Generator[Optional[OpMsg], None, None],
        op: Op,
    ) -> Generator[Optional[OpMsg], None, None]:
        """A generator of output messages for each input messages.

        - All output messages of all input messages are yielded in order in one
          single generator.
        """
        for input_msg in msgs:
            for output_msg in op.run(time_step=time_step, msg=input_msg):
                yield output_msg

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[Optional[OpMsg], None, None]:
        """."""
        logger.info(f"OpChainGroupOp.run: Running")
        # Convert a single message into a generator of messages.
        # This is just to treat all messages as if they came from a generator.
        msgs = (_msg for _msg in [msg])  # type: Generator[Optional[OpMsg], None, None]

        for op in self.ops:
            # Create a chain of generators.
            # Each output generator is the input to the next generator.
            msgs = self.get_output_msgs_from_op(time_step=time_step, msgs=msgs, op=op)

        # The final generator is the generator of this op_group.
        final_msgs = msgs

        for output_msg_i, output_msg in enumerate(final_msgs):
            logger.info(
                f"OpChainGroupOp.run: Yielding output_msg from op_chain | "
                f"{output_msg_i=} | {type(output_msg.data)=}"
            )
            yield OpMsg(
                data=output_msg.data,
                metadata=output_msg.metadata,
                audit=output_msg.audit,
            )
