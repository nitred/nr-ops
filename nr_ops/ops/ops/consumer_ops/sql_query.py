import logging
from typing import Any, Dict, List, Union

from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.postgres import PostgresConnOp

logger = logging.getLogger(__name__)


class SQLQueryConsumerOpConfigModel(BaseOpConfigModel):
    sql_conn_id: StrictStr
    sql_query: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SQLQueryConsumerOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SQLQueryConsumerOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SQLQueryConsumerOp(BaseConsumerOp):
    OP_TYPE = "consumer.sql_query"
    OP_CONFIG_MODEL = SQLQueryConsumerOpConfigModel
    OP_METADATA_MODEL = SQLQueryConsumerOpMetadataModel
    OP_AUDIT_MODEL = SQLQueryConsumerOpAuditModel

    templated_fields = None

    def __init__(self, sql_conn_id: str, sql_query: str, **kwargs):
        self.sql_conn_id = sql_conn_id
        self.sql_query = sql_query
        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.db: Union[PostgresConnOp] = op_manager.get_connector(
            op_id=self.sql_conn_id
        )

    def run(self, time_step: TimeStep, msg: OpMsg) -> OpMsg:
        """."""
        logger.info(f"SQLQueryConsumerOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="SQLQueryConsumerOp.run:"
        )

        with self.db.get_engine().connect() as conn:
            conn.execute(self.sql_query)

        return OpMsg(
            data=None,
            metadata=SQLQueryConsumerOpMetadataModel(),
            audit=SQLQueryConsumerOpAuditModel(),
        )
