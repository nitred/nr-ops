import logging
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


class ListPutConsumerOpConfigModel(BaseOpConfigModel):
    list_conn_id: StrictStr
    put_type: Literal["append", "extend"]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class ListPutConsumerOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class ListPutConsumerOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class ListPutConsumerOp(BaseConsumerOp):
    OP_TYPE = "consumer.list.put"
    OP_CONFIG_MODEL = ListPutConsumerOpConfigModel
    OP_METADATA_MODEL = ListPutConsumerOpMetadataModel
    OP_AUDIT_MODEL = ListPutConsumerOpAuditModel

    templated_fields = None

    def __init__(
        self, list_conn_id: str, put_type: Literal["append", "extend"], **kwargs
    ):
        self.list_conn_id = list_conn_id
        self.put_type = put_type

        op_manager = get_global_op_manager()

        self.list_conn: Union[ListConnOp] = op_manager.get_connector(
            op_id=self.list_conn_id
        )

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(self, time_step: TimeStep, msg: OpMsg) -> OpMsg:
        """."""
        logger.info(f"ListPutConsumerOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="ListPutConsumerOp.run:"
        )

        _list = self.list_conn.get_reference()
        logger.info(
            f"ListPutConsumerOp.run: Putting item of {type(msg.data)=} to list "
            f"with {self.put_type=}. Current {len(_list)=}"
        )

        if self.put_type == "append":
            _list.append(msg.data)
        elif self.put_type == "extend":
            _list.extend(msg.data)
        else:
            raise NotImplementedError()

        logger.info(
            f"ListPutConsumerOp.run: Done Putting item of {type(msg.data)=} to list "
            f"with {self.put_type=}. New {len(_list)=}"
        )

        return OpMsg(
            data=None,
            metadata=ListPutConsumerOpMetadataModel(),
            audit=ListPutConsumerOpAuditModel(),
        )
