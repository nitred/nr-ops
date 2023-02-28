"""."""
import logging

from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class AirflowS3HookConnOpConfigModel(BaseOpConfigModel):
    airflow_conn_id: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowS3HookConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowS3HookConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowS3HookConnOp(BaseConnectorOp):
    OP_TYPE = "connector.hooks.airflow_s3_hook"
    OP_CONFIG_MODEL = AirflowS3HookConnOpConfigModel
    OP_METADATA_MODEL = AirflowS3HookConnOpMetadataModel
    OP_AUDIT_MODEL = AirflowS3HookConnOpAuditModel

    templated_fields = None

    def __init__(self, airflow_conn_id: str, **kwargs):
        self.airflow_conn_id = airflow_conn_id
        self.templated_fields = kwargs.get("templated_fields", [])

        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        self.hook = S3Hook(aws_conn_id=self.airflow_conn_id)

    def run(self) -> OpMsg:
        """."""
        logger.info(f"AirflowS3HookConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="AirflowS3HookConnOp.run:"
        )

        return OpMsg(
            data=self.hook,
            metadata=AirflowS3HookConnOpMetadataModel(),
            audit=AirflowS3HookConnOpAuditModel(),
        )
