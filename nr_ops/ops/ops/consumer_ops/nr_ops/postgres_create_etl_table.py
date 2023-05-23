import logging
from typing import Any, Dict, List, Union

import pandas as pd
from pydantic import BaseModel, Field, StrictBool, StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.postgres import PostgresConnOp

logger = logging.getLogger(__name__)


class IndexConfigModel(BaseModel):
    is_unique: StrictBool

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class NROpsPGCreateETLTableConsumerOpConfigModel(BaseOpConfigModel):
    postgres_conn_id: StrictStr
    schema_: StrictStr
    table: StrictStr
    # dedup_uuid is primary key
    # Force user to provide {} if they want no indexes
    indexes: Dict[StrictStr, IndexConfigModel]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False
        fields = {"schema_": "schema"}

    # Source: https://github.com/pydantic/pydantic/issues/153#issuecomment-379695371
    def dict(self, *args, **kwargs):
        d = super().dict(*args, **kwargs)
        d["schema"] = d.pop("schema_")
        return d


class NROpsPGCreateETLTableConsumerOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class NROpsPGCreateETLTableConsumerOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class NROpsPostgresCreateETLTableConsumerOp(BaseConsumerOp):
    OP_TYPE = "consumer.nr_ops.postgres_create_etl_table"
    OP_CONFIG_MODEL = NROpsPGCreateETLTableConsumerOpConfigModel
    OP_METADATA_MODEL = NROpsPGCreateETLTableConsumerOpMetadataModel
    OP_AUDIT_MODEL = NROpsPGCreateETLTableConsumerOpAuditModel

    templated_fields = None

    def __init__(
        self,
        postgres_conn_id: str,
        schema: str,
        table: str,
        indexes: Dict[str, Any],
        **kwargs,
    ):
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.table = table
        self.indexes = indexes
        self.indexes_with_models = {
            key: IndexConfigModel(**value) for key, value in self.indexes.items()
        }
        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.db: Union[PostgresConnOp] = op_manager.get_connector(
            op_id=self.postgres_conn_id
        )

    def run(self, time_step: TimeStep, msg: OpMsg) -> OpMsg:
        """."""
        logger.info(f"NROpsPGCreateETLTableConsumerOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="NROpsPGCreateETLTableConsumerOp.run:",
        )

        logger.info(
            f"NROpsPGCreateETLTableConsumerOp.run: Checking to see if "
            f"'{self.schema}'.'{self.table}' already exist in the database."
        )
        # Make sure that the table does not exist!
        df = pd.read_sql(
            sql=f"""
            SELECT
                *
            FROM
                pg_catalog.pg_tables 
            WHERE 
                schemaname = '{self.schema}' 
                AND tablename = '{self.table}'
          """,
            con=self.db.get_engine(),
        )

        if len(df) == 0:
            logger.info(
                f"NROpsPGCreateETLTableConsumerOp.run: '{self.schema}'.'{self.table}' "
                f"does not exist in the database. Attempting to create the table..."
            )
        else:
            logger.info(
                f"NROpsPGCreateETLTableConsumerOp.run: '{self.schema}'.'{self.table}' "
                f"already exists in the database. Will not attempt to create table "
                f"or its indexes or triggers. Consuming..."
            )
            return OpMsg(
                data=None,
                metadata=NROpsPGCreateETLTableConsumerOpMetadataModel(),
                audit=NROpsPGCreateETLTableConsumerOpAuditModel(),
            )

        indexes = [
            f"""create """
            f"""{'unique' if index_model.is_unique else ''} """
            f"""index """
            f"""if not exists """
            f""" "{self.schema.lower().replace(' ', '_')}_{self.table.lower().replace(' ', '_')}_{index_col.lower().replace(' ', '_')}" """
            f"""on "{self.schema}"."{self.table}" ({index_col});"""
            for index_col, index_model in self.indexes_with_models.items()
        ]
        indexes_str = "\n".join(indexes)

        create_sql = f"""
            create table if not exists "{self.schema}"."{self.table}"
            (
                etl_logical_ts                       timestamp with time zone,
                etl_start_ts                         timestamp with time zone,
                etl_end_ts                           timestamp with time zone,
                etl_created_at                       timestamp with time zone default current_timestamp,
                etl_updated_at                       timestamp with time zone default current_timestamp,
                dedup_uuid                           uuid,
                data                                 JSONB,
                etl_metadata                         JSONB,
                PRIMARY KEY (dedup_uuid)
            );
    
            {indexes_str}
    
            CREATE OR REPLACE FUNCTION
                update_etl_updated_at_{self.schema.lower().replace(' ', '_')}_{self.table.lower().replace(' ', '_')}()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.etl_updated_at = now();
                RETURN NEW;
            END;
            $$ language 'plpgsql';
    
    
            DO $$ BEGIN
            DROP TRIGGER IF EXISTS
                trigger_etl_updated_at_{self.schema.lower().replace(' ', '_')}_{self.table.lower().replace(' ', '_')}
            ON
                "{self.schema}"."{self.table}";
    
            CREATE TRIGGER
                trigger_etl_updated_at_{self.schema.lower().replace(' ', '_')}_{self.table.lower().replace(' ', '_')}
            BEFORE INSERT OR UPDATE ON
                "{self.schema}"."{self.table}"
            FOR EACH ROW EXECUTE PROCEDURE
                update_etl_updated_at_{self.schema.lower().replace(' ', '_')}_{self.table.lower().replace(' ', '_')}();
            END $$;
        """

        logger.info(
            f"NROpsPGCreateETLTableConsumerOp.run: Applying the following "
            f"create statements!\n{create_sql}"
        )

        # Create table and indexes and triggers
        with self.db.get_engine().connect() as conn:
            conn.execute(create_sql)

        logger.info(
            f"NROpsPGCreateETLTableConsumerOp.run: Done creating etl table "
            f"'{self.schema}'.'{self.table}'"
        )

        return OpMsg(
            data=None,
            metadata=NROpsPGCreateETLTableConsumerOpMetadataModel(),
            audit=NROpsPGCreateETLTableConsumerOpAuditModel(),
        )
