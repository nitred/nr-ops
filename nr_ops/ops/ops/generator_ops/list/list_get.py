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


class ListGetGeneratorOpConfigModel(BaseOpConfigModel):
    list_conn_id: StrictStr
    get_type: Literal["iterate_over_list", "get_reference"]
    reinitialize_list: StrictBool

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class ListGetGeneratorOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class ListGetGeneratorOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class ListGetGeneratorOp(BaseGeneratorOp):
    OP_TYPE = "generator.list.get"
    OP_CONFIG_MODEL = ListGetGeneratorOpConfigModel
    OP_METADATA_MODEL = ListGetGeneratorOpMetadataModel
    OP_AUDIT_MODEL = ListGetGeneratorOpAuditModel

    templated_fields = None

    def __init__(
        self,
        list_conn_id: str,
        get_type: str,
        reinitialize_list: bool,
        **kwargs,
    ):
        self.list_conn_id = list_conn_id
        self.get_type = get_type
        self.reinitialize_list = reinitialize_list
        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.list_conn: Union[ListConnOp] = op_manager.get_connector(
            op_id=self.list_conn_id
        )

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"ListGetGeneratorOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="ListGetGeneratorOp.run:"
        )

        if self.reinitialize_list:
            _list = self.list_conn.get_reference_and_reinit()
        else:
            _list = self.list_conn.get_reference()

        if self.get_type == "get_reference":
            logger.info(
                f"ListGetGeneratorOp.run: Yielding reference to list. {len(_list)=}"
            )
            yield OpMsg(
                data=_list,
                metadata=ListGetGeneratorOpMetadataModel(),
                audit=ListGetGeneratorOpAuditModel(),
            )

        elif self.get_type == "iterate_over_list":
            logger.info(
                f"ListGetGeneratorOp.run: Iterating over list and yielding items. "
                f"{len(_list)=}"
            )
            for item_i, item in enumerate(_list):
                logger.info(
                    f"ListGetGeneratorOp.run: Yielding {item_i=} (0-index) from list. "
                    f"{type(item)=}"
                )
                yield OpMsg(
                    data=item,
                    metadata=ListGetGeneratorOpMetadataModel(),
                    audit=ListGetGeneratorOpAuditModel(),
                )
        else:
            raise NotImplementedError()

        if self.reinitialize_list:
            logger.info(
                f"ListGetGeneratorOp.run: Done yielding. {self.reinitialize_list=}. "
                f"List has been reinitialized to an empty list []."
            )
        else:
            logger.info(
                f"ListGetGeneratorOp.run: Done yielding. {self.reinitialize_list=}. "
                f"List has not been reinitialized."
            )
