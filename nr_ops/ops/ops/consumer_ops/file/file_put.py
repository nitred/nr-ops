import logging
from typing import Literal, Union

from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.file import FileConnOp

logger = logging.getLogger(__name__)


class FilePutConsumerOpConfigModel(BaseOpConfigModel):
    file_conn_id: StrictStr
    put_type: Literal["write", "write_and_flush"]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class FilePutConsumerOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class FilePutConsumerOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class FilePutConsumerOp(BaseConsumerOp):
    OP_TYPE = "consumer.file.put"
    OP_CONFIG_MODEL = FilePutConsumerOpConfigModel
    OP_METADATA_MODEL = FilePutConsumerOpMetadataModel
    OP_AUDIT_MODEL = FilePutConsumerOpAuditModel

    templated_fields = None

    def __init__(
        self, file_conn_id: str, put_type: Literal["write", "write_and_flush"], **kwargs
    ):
        self.file_conn_id = file_conn_id
        self.put_type = put_type

        op_manager = get_global_op_manager()

        self.file_conn: Union[FileConnOp] = op_manager.get_connector(
            op_id=self.file_conn_id
        )

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(self, time_step: TimeStep, msg: OpMsg) -> OpMsg:
        """."""
        logger.info(f"FilePutConsumerOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="FilePutConsumerOp.run:"
        )

        logger.info(
            f"FilePutConsumerOp.run: Putting item of {type(msg.data)=} to file "
            f"with {self.put_type=}."
        )

        if self.put_type == "write":
            self.file_conn.put_write(msg.data)
        if self.put_type == "write_and_flush":
            self.file_conn.put_write_and_flush(msg.data)
        else:
            raise NotImplementedError()

        logger.info(
            f"FilePutConsumerOp.run: Done Putting item of {type(msg.data)=} to file "
            f"with {self.put_type=}."
        )

        return OpMsg(
            data=None,
            metadata=FilePutConsumerOpMetadataModel(),
            audit=FilePutConsumerOpAuditModel(),
        )
