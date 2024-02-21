import logging
from typing import Any, Dict, List, Union

from pydantic import StrictBool, StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.postgres import PostgresConnOp

logger = logging.getLogger(__name__)


class SQLQueryGeneratorOpConfigModel(BaseOpConfigModel):
    sql_conn_id: StrictStr
    sql_query: StrictStr
    iterate_over_rows: StrictBool = False

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SQLQueryGeneratorOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SQLQueryGeneratorOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SQLQueryGeneratorOp(BaseGeneratorOp):
    OP_TYPE = "generator.sql_query"
    OP_CONFIG_MODEL = SQLQueryGeneratorOpConfigModel
    OP_METADATA_MODEL = SQLQueryGeneratorOpMetadataModel
    OP_AUDIT_MODEL = SQLQueryGeneratorOpAuditModel

    templated_fields = None

    def __init__(
        self,
        sql_conn_id: str,
        sql_query: str,
        iterate_over_rows: bool = False,
        **kwargs,
    ):
        self.sql_conn_id = sql_conn_id
        self.sql_query = sql_query
        self.iterate_over_rows = iterate_over_rows
        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.db: Union[PostgresConnOp] = op_manager.get_connector(
            op_id=self.sql_conn_id
        )

    def run(self, time_step: TimeStep, msg: OpMsg) -> OpMsg:
        """."""
        logger.info(f"SQLQueryGeneratorOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="SQLQueryGeneratorOp.run:"
        )

        with self.db.get_engine().connect() as conn:
            result = conn.execute(self.sql_query)

        if self.iterate_over_rows:
            for row in result:
                yield OpMsg(
                    data=row,
                    metadata=SQLQueryGeneratorOpMetadataModel(),
                    audit=SQLQueryGeneratorOpAuditModel(),
                )
        else:
            yield OpMsg(
                data=result,
                metadata=SQLQueryGeneratorOpMetadataModel(),
                audit=SQLQueryGeneratorOpAuditModel(),
            )
