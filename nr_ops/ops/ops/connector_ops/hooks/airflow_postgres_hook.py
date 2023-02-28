"""."""
import logging

from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class AirflowPostgresHookConnOpConfigModel(BaseOpConfigModel):
    airflow_conn_id: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowPostgresHookConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowPostgresHookConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowPostgresHookConnOp(BaseConnectorOp):
    OP_TYPE = "connector.hooks.airflow_postgres_hook"
    OP_CONFIG_MODEL = AirflowPostgresHookConnOpConfigModel
    OP_METADATA_MODEL = AirflowPostgresHookConnOpMetadataModel
    OP_AUDIT_MODEL = AirflowPostgresHookConnOpAuditModel

    templated_fields = None

    def __init__(self, airflow_conn_id: str, **kwargs):
        self.airflow_conn_id = airflow_conn_id
        self.templated_fields = kwargs.get("templated_fields", [])

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        self.hook = PostgresHook(postgres_conn_id=self.airflow_conn_id)

    def run(self) -> OpMsg:
        """.

        # ------------------------------------------------------------------------------
        # Get engine
        # ------------------------------------------------------------------------------
        self.hook.get_sqlalchemy_engine()

        # ------------------------------------------------------------------------------
        # Get connection
        # ------------------------------------------------------------------------------
        self.hook.get_conn()
        """
        logger.info(f"AirflowPostgresHookConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="AirflowPostgresHookConnOp.run:"
        )

        return OpMsg(
            data=self.hook,
            metadata=AirflowPostgresHookConnOpMetadataModel(),
            audit=AirflowPostgresHookConnOpAuditModel(),
        )
