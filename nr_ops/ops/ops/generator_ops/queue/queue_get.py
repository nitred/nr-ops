import logging
import queue
from typing import Generator, Optional, Union

from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.queue import QueueConnOp

logger = logging.getLogger(__name__)


class QueueGetGeneratorOpConfigModel(BaseOpConfigModel):
    queue_conn_id: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class QueueGetGeneratorOpMetadataModel(BaseOpMetadataModel):
    item_i: int

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class QueueGetGeneratorOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class QueueGetGeneratorOp(BaseGeneratorOp):
    OP_TYPE = "generator.queue.get"
    OP_CONFIG_MODEL = QueueGetGeneratorOpConfigModel
    OP_METADATA_MODEL = QueueGetGeneratorOpMetadataModel
    OP_AUDIT_MODEL = QueueGetGeneratorOpAuditModel

    templated_fields = None

    def __init__(
        self,
        queue_conn_id: str,
        **kwargs,
    ):
        self.queue_conn_id = queue_conn_id
        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.queue_conn: Union[QueueConnOp] = op_manager.get_connector(
            op_id=self.queue_conn_id
        )

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"QueueGetGeneratorOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="QueueGetGeneratorOp.run:"
        )

        fifo: queue.Queue = self.queue_conn.get_reference()

        for item_i, item in enumerate(fifo.queue):
            logger.info(
                f"QueueGetGeneratorOp.run: Yielding {item_i=} (0-index) from queue of "
                f"type {type(item)=}"
            )
            yield OpMsg(
                data=item,
                metadata=QueueGetGeneratorOpMetadataModel(item_i=item_i),
                audit=QueueGetGeneratorOpAuditModel(),
            )
