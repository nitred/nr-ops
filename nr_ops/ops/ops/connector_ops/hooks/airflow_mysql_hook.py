"""."""
import logging

from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class AirflowMysqlHookConnOpConfigModel(BaseOpConfigModel):
    airflow_conn_id: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowMysqlHookConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowMysqlHookConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowMysqlHookConnOp(BaseConnectorOp):
    OP_TYPE = "connector.hooks.airflow_mysql_hook"
    OP_CONFIG_MODEL = AirflowMysqlHookConnOpConfigModel
    OP_METADATA_MODEL = AirflowMysqlHookConnOpMetadataModel
    OP_AUDIT_MODEL = AirflowMysqlHookConnOpAuditModel

    templated_fields = None

    def __init__(self, airflow_conn_id: str, **kwargs):
        self.airflow_conn_id = airflow_conn_id
        self.templated_fields = kwargs.get("templated_fields", [])

        from airflow.providers.mysql.hooks.mysql import MySqlHook

        self.hook = MySqlHook(mysql_conn_id=self.airflow_conn_id)

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
        logger.info(f"AirflowMysqlHookConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="AirflowMysqlHookConnOp.run:"
        )

        return OpMsg(
            data=self.hook,
            metadata=AirflowMysqlHookConnOpMetadataModel(),
            audit=AirflowMysqlHookConnOpAuditModel(),
        )
