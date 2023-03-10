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
from nr_ops.ops.ops.connector_ops.interfaces.list import ListConnOp

logger = logging.getLogger(__name__)


class PutListConsumerOpConfigModel(BaseOpConfigModel):
    list_conn_id: StrictStr
    put_type: Literal["put_append"]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PutListConsumerOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PutListConsumerOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PutListConsumerOp(BaseConsumerOp):
    OP_TYPE = "consumer.put_list"
    OP_CONFIG_MODEL = PutListConsumerOpConfigModel
    OP_METADATA_MODEL = PutListConsumerOpMetadataModel
    OP_AUDIT_MODEL = PutListConsumerOpAuditModel

    templated_fields = None

    def __init__(self, list_conn_id: str, put_type: str, **kwargs):
        self.list_conn_id = list_conn_id
        self.put_type = put_type

        op_manager = get_global_op_manager()

        self.list_conn: Union[ListConnOp] = op_manager.get_connector(
            op_id=self.list_conn_id
        )

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(self, time_step: TimeStep, msg: OpMsg) -> OpMsg:
        """."""
        logger.info(f"PutListConsumerOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="PutListConsumerOp.run:"
        )

        if self.put_type == "put_append":
            self.list_conn.put_append(item=msg.data)
        else:
            raise NotImplementedError()

        return OpMsg(
            data=None,
            metadata=PutListConsumerOpMetadataModel(),
            audit=PutListConsumerOpAuditModel(),
        )
