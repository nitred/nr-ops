import logging
from typing import Any, Dict, Literal, Optional

import pangres as pg
from pydantic import BaseModel, StrictBool, StrictInt, StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.postgres import PostgresConnOp
from nr_ops.utils.sqlalchemy.dtype_lookup import lookup_sqlalchemy_dtype

logger = logging.getLogger(__name__)


class UpsertConfigModel(BaseModel):
    # Reference: https://github.com/ThibTrip/pangres/wiki/Upsert
    table_name: StrictStr
    schema_: StrictStr
    if_row_exists: Literal["ignore", "update"]
    dtype: Optional[Dict[StrictStr, StrictStr]] = None
    create_schema: StrictBool = False
    create_table: StrictBool = False
    add_new_columns: StrictBool = False
    chunksize: Optional[StrictInt] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False
        fields = {"schema_": "schema"}

    # Source: https://github.com/pydantic/pydantic/issues/153#issuecomment-379695371
    def dict(self, *args, **kwargs):
        d = super().dict(*args, **kwargs)
        d["schema"] = d.pop("schema_")
        return d


class PangresDFToSQLDBOpOpConfigModel(BaseOpConfigModel):
    postgres_conn_id: StrictStr
    upsert_config: UpsertConfigModel

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PangresDFToSQLDBOpOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PangresDFToSQLDBOpOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PangresDFToSQLDBOp(BaseConsumerOp):
    OP_TYPE = "consumer.pangres.df_to_sql_db"
    OP_CONFIG_MODEL = PangresDFToSQLDBOpOpConfigModel
    OP_METADATA_MODEL = PangresDFToSQLDBOpOpMetadataModel
    OP_AUDIT_MODEL = PangresDFToSQLDBOpOpAuditModel

    templated_fields = None

    def __init__(self, postgres_conn_id: str, upsert_config: Dict[str, Any], **kwargs):
        """."""
        self.postgres_conn_id = postgres_conn_id
        self.upsert_config = upsert_config
        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.postgres: PostgresConnOp = op_manager.get_connector(
            op_id=self.postgres_conn_id
        )

    def run(self, time_step: TimeStep, msg: OpMsg) -> OpMsg:
        """."""
        logger.info("PangresDFToSQLDBOpOp: Running.")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="PangresDFToSQLDBOpOp.run:"
        )

        if self.upsert_config is None:
            col_to_dtype_obj_map = None
        else:
            col_to_dtype_obj_map = {
                column_name: lookup_sqlalchemy_dtype(dtype)
                for column_name, dtype in self.upsert_config.get("dtype", {}).items()
            }

        pg.upsert(
            con=self.postgres.get_engine(),
            df=msg.data,
            create_schema=self.upsert_config.get("create_schema", False),
            create_table=self.upsert_config.get("create_table", False),
            add_new_columns=self.upsert_config.get("add_new_columns", False),
            adapt_dtype_of_empty_db_columns=False,
            chunksize=self.upsert_config.get("chunksize", None),
            yield_chunks=False,
            table_name=self.upsert_config.get("table_name"),
            schema=self.upsert_config.get("schema"),
            if_row_exists=self.upsert_config.get("if_row_exists"),
            dtype=col_to_dtype_obj_map,
        )

        return OpMsg(
            data=None,
            metadata=PangresDFToSQLDBOpOpMetadataModel(),
            audit=PangresDFToSQLDBOpOpAuditModel(),
        )
