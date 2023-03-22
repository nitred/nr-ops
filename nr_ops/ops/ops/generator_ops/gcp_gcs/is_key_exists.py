import logging
from typing import Generator, Optional

from pydantic import StrictStr, validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.gcp_gcs import GCPGCSConnOp

logger = logging.getLogger(__name__)


class GCPGCSIsKeyExistsOpConfigModel(BaseOpConfigModel):
    gcs_conn_id: StrictStr
    bucket: StrictStr
    key: StrictStr

    @validator("key", pre=False)
    def validate_key(cls, key: str):
        if key.endswith("/"):
            raise ValueError(f"{key=} must not end with a '/'")

        if key.startswith("/"):
            raise ValueError(f"{key=} must not start with a '/'")

        return key

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPGCSIsKeyExistsOpMetadataModel(BaseOpMetadataModel):
    bucket: StrictStr
    key: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPGCSIsKeyExistsOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPGCSIsKeyExistsOp(BaseGeneratorOp):
    OP_TYPE = "generator.gcp.gcs.is_key_exists"
    OP_CONFIG_MODEL = GCPGCSIsKeyExistsOpConfigModel
    OP_METADATA_MODEL = GCPGCSIsKeyExistsOpMetadataModel
    OP_AUDIT_MODEL = GCPGCSIsKeyExistsOpAuditModel

    templated_fields = None

    def __init__(
        self,
        gcs_conn_id: str,
        bucket: str,
        key: str,
        **kwargs,
    ):
        """."""
        self.gcs_conn_id = gcs_conn_id
        self.bucket = bucket
        self.key = key
        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.gcs_conn: GCPGCSConnOp = op_manager.get_connector(op_id=self.gcs_conn_id)

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"GCPGCSIsKeyExistsOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="GCPGCSIsKeyExistsOp.run:"
        )

        logger.info(
            f"GCPGCSIsKeyExistsOp.run: Checking if key exist. {self.bucket=} | "
            f"{self.key=}"
        )

        is_key_exists = self.gcs_conn.is_key_exists(bucket=self.bucket, key=self.key)

        logger.info(
            f"GCPGCSIsKeyExistsOp.run: {is_key_exists=} | {self.bucket=} | {self.key=}"
        )
        yield OpMsg(
            data=is_key_exists,
            metadata=GCPGCSIsKeyExistsOpMetadataModel(
                bucket=self.bucket,
                key=self.key,
            ),
            audit=GCPGCSIsKeyExistsOpAuditModel(),
        )
