import logging
import time
from typing import Literal, Union

from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.queue import QueueConnOp

logger = logging.getLogger(__name__)


class QueuePutConsumerOpConfigModel(BaseOpConfigModel):
    queue_conn_id: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class QueuePutConsumerOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class QueuePutConsumerOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class QueuePutConsumerOp(BaseConsumerOp):
    OP_TYPE = "consumer.queue.put"
    OP_CONFIG_MODEL = QueuePutConsumerOpConfigModel
    OP_METADATA_MODEL = QueuePutConsumerOpMetadataModel
    OP_AUDIT_MODEL = QueuePutConsumerOpAuditModel

    templated_fields = None

    def __init__(self, queue_conn_id: str, **kwargs):
        self.queue_conn_id = queue_conn_id

        op_manager = get_global_op_manager()

        self.queue_conn: Union[QueueConnOp] = op_manager.get_connector(
            op_id=self.queue_conn_id
        )

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(self, time_step: TimeStep, msg: OpMsg) -> OpMsg:
        """."""
        logger.info(f"QueuePutConsumerOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="QueuePutConsumerOp.run:"
        )

        fifo = self.queue_conn.get_reference()

        logger.info(
            f"QueuePutConsumerOp.run: Putting item of {type(msg.data)=} to queue"
        )
        fifo.put(msg.data)

        return OpMsg(
            data=None,
            metadata=QueuePutConsumerOpMetadataModel(),
            audit=QueuePutConsumerOpAuditModel(),
        )
