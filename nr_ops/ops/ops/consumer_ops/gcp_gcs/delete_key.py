import logging
from typing import Optional

from pydantic import StrictBool, StrictStr, validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.gcp_gcs import GCPGCSConnOp

logger = logging.getLogger(__name__)


class GCPGCSDeleteKeyOpConfigModel(BaseOpConfigModel):
    gcs_conn_id: StrictStr
    bucket: StrictStr
    key: StrictStr
    check_if_exists: StrictBool

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


class GCPGCSDeleteKeyOpMetadataModel(BaseOpMetadataModel):
    bucket: StrictStr
    key: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPGCSDeleteKeyOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPGCSDeleteKeyOp(BaseConsumerOp):
    OP_TYPE = "consumer.gcp.gcs.delete_key"
    OP_CONFIG_MODEL = GCPGCSDeleteKeyOpConfigModel
    OP_METADATA_MODEL = GCPGCSDeleteKeyOpMetadataModel
    OP_AUDIT_MODEL = GCPGCSDeleteKeyOpAuditModel

    templated_fields = None

    def __init__(
        self,
        gcs_conn_id: str,
        bucket: str,
        key: str,
        check_if_exists: bool,
        **kwargs,
    ):
        """."""
        self.gcs_conn_id = gcs_conn_id
        self.bucket = bucket
        self.key = key
        self.check_if_exists = check_if_exists

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.gcs_conn: GCPGCSConnOp = op_manager.get_connector(op_id=self.gcs_conn_id)

    def run(self, time_step: TimeStep, msg: Optional[OpMsg] = None) -> OpMsg:
        """."""
        logger.info(f"GCPGCSDeleteKeyOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="GCPGCSDeleteKeyOp.run:"
        )

        # First assume, that the object exists. This means we will delete the key.
        # Later, if we find out that the key does not exist, we will set this to
        # False. If it is False, then we will not attempt to delete.
        is_key_exists = True

        if self.check_if_exists:
            logger.info(
                f"GCPGCSDeleteKeyOp.run: {self.check_if_exists=}. Checking to see "
                f"if key exists."
            )
            is_key_exists = self.gcs_conn.is_key_exists(
                bucket=self.bucket, key=self.key
            )
            logger.info(
                f"GCPGCSDeleteKeyOp.run: Done checking to see if key exists. "
                f"{is_key_exists=}."
            )

        if is_key_exists:
            logger.info(
                f"GCPGCSDeleteKeyOp.run: {is_key_exists=}. Deleting key "
                f"{self.bucket} | {self.key=}"
            )
            self.gcs_conn.delete_key(bucket=self.bucket, key=self.key)
            logger.info(
                f"GCPGCSDeleteKeyOp.run: Done deleting key {self.bucket} | {self.key=}"
            )
        else:
            logger.info(
                f"GCPGCSDeleteKeyOp.run: {is_key_exists=}. Since key does not exist "
                f"we will not attempt to delete it."
            )

        return OpMsg(
            data=None,
            metadata=GCPGCSDeleteKeyOpMetadataModel(
                bucket=self.bucket,
                key=self.key,
            ),
            audit=GCPGCSDeleteKeyOpAuditModel(),
        )
