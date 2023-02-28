import logging
import time

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class MockConsumerOpConfigModel(BaseOpConfigModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MockConsumerOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MockConsumerOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MockConsumerOp(BaseConsumerOp):
    OP_TYPE = "consumer.mock"
    OP_CONFIG_MODEL = MockConsumerOpConfigModel
    OP_METADATA_MODEL = MockConsumerOpMetadataModel
    OP_AUDIT_MODEL = MockConsumerOpAuditModel

    templated_fields = None

    def __init__(self, **kwargs):
        self.templated_fields = kwargs.get("templated_fields", [])

    def run(self, time_step: TimeStep, msg: OpMsg) -> OpMsg:
        """."""
        logger.info(f"MockConsumerOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="MockConsumerOp.run:"
        )

        return OpMsg(
            data=None,
            metadata=MockConsumerOpMetadataModel(),
            audit=MockConsumerOpAuditModel(),
        )
