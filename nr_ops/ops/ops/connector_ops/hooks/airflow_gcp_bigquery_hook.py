"""."""
import logging

from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class AirflowGCPBigQueryHookConnOpConfigModel(BaseOpConfigModel):
    airflow_conn_id: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowGCPBigQueryHookConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowGCPBigQueryHookConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowGCPBigQueryHookConnOp(BaseConnectorOp):
    OP_TYPE = "connector.hooks.airflow_gcp_bigquery_hook"
    OP_CONFIG_MODEL = AirflowGCPBigQueryHookConnOpConfigModel
    OP_METADATA_MODEL = AirflowGCPBigQueryHookConnOpMetadataModel
    OP_AUDIT_MODEL = AirflowGCPBigQueryHookConnOpAuditModel

    templated_fields = None

    def __init__(self, airflow_conn_id: str, **kwargs):
        self.airflow_conn_id = airflow_conn_id
        self.templated_fields = kwargs.get("templated_fields", [])

        self.hook = None

    def run(self) -> OpMsg:
        """."""
        logger.info(f"AirflowGCPBigQueryHookConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="AirflowGCPBigQueryHookConnOp.run:"
        )

        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        self.hook = BigQueryHook(gcp_conn_id=self.airflow_conn_id)

        logger.info(
            f"AirflowGCPBigQueryHookConnOp.run: Initialized BigQueryHook "
            f"with {self.airflow_conn_id=}"
        )

        return OpMsg(
            data=self.hook,
            metadata=AirflowGCPBigQueryHookConnOpMetadataModel(),
            audit=AirflowGCPBigQueryHookConnOpAuditModel(),
        )
