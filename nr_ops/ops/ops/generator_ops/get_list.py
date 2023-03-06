import logging
from typing import Generator, Literal, Optional, Union

from pydantic import StrictBool, StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.list import ListConnOp

logger = logging.getLogger(__name__)


class GetListGeneratorOpConfigModel(BaseOpConfigModel):
    list_conn_id: StrictStr
    get_type: Literal["get_reference", "get_deepcopy", "get_reference_and_reinit"]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GetListGeneratorOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GetListGeneratorOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GetListGeneratorOp(BaseGeneratorOp):
    OP_TYPE = "generator.get_list"
    OP_CONFIG_MODEL = GetListGeneratorOpConfigModel
    OP_METADATA_MODEL = GetListGeneratorOpMetadataModel
    OP_AUDIT_MODEL = GetListGeneratorOpAuditModel

    templated_fields = None

    def __init__(
        self,
        list_conn_id: str,
        get_type: str,
        **kwargs,
    ):
        self.list_conn_id = list_conn_id
        self.get_type = get_type
        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.list_conn: Union[ListConnOp] = op_manager.connector.get_connector(
            op_id=self.list_conn_id
        )

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"GetListGeneratorOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="GetListGeneratorOp.run:"
        )

        if self.get_type == "get_reference":
            output = self.list_conn.get_reference()
        elif self.get_type == "get_deepcopy":
            output = self.list_conn.get_deepcopy()
        elif self.get_type == "get_reference_and_reinit":
            output = self.list_conn.get_reference_and_reinit()
        else:
            raise NotImplementedError()

        yield OpMsg(
            data=output,
            metadata=GetListGeneratorOpMetadataModel(),
            audit=GetListGeneratorOpAuditModel(),
        )
