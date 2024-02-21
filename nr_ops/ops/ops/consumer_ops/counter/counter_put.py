import logging
from typing import Literal, Union

from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.counter import CounterConnOp

logger = logging.getLogger(__name__)


class CounterPutConsumerOpConfigModel(BaseOpConfigModel):
    counter_conn_id: StrictStr
    put_type: Literal["update_dict", "update_list_of_dicts"]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class CounterPutConsumerOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class CounterPutConsumerOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class CounterPutConsumerOp(BaseConsumerOp):
    OP_TYPE = "consumer.counter.put"
    OP_CONFIG_MODEL = CounterPutConsumerOpConfigModel
    OP_METADATA_MODEL = CounterPutConsumerOpMetadataModel
    OP_AUDIT_MODEL = CounterPutConsumerOpAuditModel

    templated_fields = None

    def __init__(
        self,
        counter_conn_id: str,
        put_type: Literal["update_dict", "update_list_of_dicts"],
        **kwargs,
    ):
        self.counter_conn_id = counter_conn_id
        self.put_type = put_type

        op_manager = get_global_op_manager()

        self.counter_conn: Union[CounterConnOp] = op_manager.get_connector(
            op_id=self.counter_conn_id
        )

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(self, time_step: TimeStep, msg: OpMsg) -> OpMsg:
        """."""
        logger.info(f"CounterPutConsumerOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="CounterPutConsumerOp.run:"
        )

        logger.info(
            f"CounterPutConsumerOp.run: Putting item of {type(msg.data)=} "
            f"to counter with {self.put_type=}."
        )

        if self.put_type == "update_dict":
            self.counter_conn.put_update_dict(item=msg.data)
        elif self.put_type == "update_list_of_dicts":
            self.counter_conn.put_update_list_of_dicts(msg.data)
        else:
            raise NotImplementedError()

        logger.info(
            f"CounterPutConsumerOp.run: Done Putting item of {type(msg.data)=} "
            f"to counter with {self.put_type=}."
        )

        return OpMsg(
            data=None,
            metadata=CounterPutConsumerOpMetadataModel(),
            audit=CounterPutConsumerOpAuditModel(),
        )
