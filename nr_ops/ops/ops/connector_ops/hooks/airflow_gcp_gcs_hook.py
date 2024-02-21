"""."""
import logging

from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class AirflowGCPGCSHookConnOpConfigModel(BaseOpConfigModel):
    airflow_conn_id: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowGCPGCSHookConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowGCPGCSHookConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowGCPGCSHookConnOp(BaseConnectorOp):
    OP_TYPE = "connector.hooks.airflow_gcp_gcs_hook"
    OP_CONFIG_MODEL = AirflowGCPGCSHookConnOpConfigModel
    OP_METADATA_MODEL = AirflowGCPGCSHookConnOpMetadataModel
    OP_AUDIT_MODEL = AirflowGCPGCSHookConnOpAuditModel

    templated_fields = None

    def __init__(self, airflow_conn_id: str, **kwargs):
        self.airflow_conn_id = airflow_conn_id
        self.templated_fields = kwargs.get("templated_fields", [])

        self.hook = None

    def run(self) -> OpMsg:
        """."""
        logger.info(f"AirflowGCPGCSHookConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="AirflowGCPGCSHookConnOp.run:"
        )

        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        self.hook = GCSHook(gcp_conn_id=self.airflow_conn_id)

        logger.info(
            f"AirflowGCPGCSHookConnOp.run: Initialized GCSHook "
            f"with {self.airflow_conn_id=}"
        )

        return OpMsg(
            data=self.hook,
            metadata=AirflowGCPGCSHookConnOpMetadataModel(),
            audit=AirflowGCPGCSHookConnOpAuditModel(),
        )
