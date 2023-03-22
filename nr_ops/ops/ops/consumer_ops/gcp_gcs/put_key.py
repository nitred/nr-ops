import logging
from typing import Literal, Optional

from pydantic import StrictBool, StrictStr, validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.gcp_gcs import GCPGCSConnOp

logger = logging.getLogger(__name__)


class GCPGCSPutKeyOpConfigModel(BaseOpConfigModel):
    gcs_conn_id: StrictStr
    bucket: StrictStr
    key: StrictStr
    replace: StrictBool
    input_type: Literal["bytes", "bytesio"]

    @validator("key", pre=False)
    def validate_key(cls, key: str):
        if key.endswith("/"):
            raise ValueError(f"{key=} must not end with a '/'")

        if key.startswith("/"):
            raise ValueError(f"{key=} must not start with a '/'")

        return key

    @validator("replace", pre=False)
    def validate_replace(cls, replace: bool):
        if replace is True:
            raise ValueError(
                f"{replace=} is not implemented. Must be False. If you require "
                f"replace functionality, then use `gcs.is_key_exists` and "
                f"`gcs.delete_key` operators to implement your own."
            )

        return replace

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPGCSPutKeyOpMetadataModel(BaseOpMetadataModel):
    bucket: StrictStr
    key: StrictStr
    input_type: Literal["bytes", "bytesio"]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPGCSPutKeyOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPGCSPutKeyOp(BaseConsumerOp):
    OP_TYPE = "consumer.gcp.gcs.put_key"
    OP_CONFIG_MODEL = GCPGCSPutKeyOpConfigModel
    OP_METADATA_MODEL = GCPGCSPutKeyOpMetadataModel
    OP_AUDIT_MODEL = GCPGCSPutKeyOpAuditModel

    templated_fields = None

    def __init__(
        self,
        gcs_conn_id: str,
        bucket: str,
        key: str,
        replace: bool,
        input_type: Literal["bytes", "bytesio"],
        **kwargs,
    ):
        """."""
        self.gcs_conn_id = gcs_conn_id
        self.bucket = bucket
        self.key = key
        self.replace = replace
        self.input_type = input_type

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.gcs_conn: GCPGCSConnOp = op_manager.get_connector(op_id=self.gcs_conn_id)

    def run(self, time_step: TimeStep, msg: Optional[OpMsg] = None) -> OpMsg:
        """."""
        logger.info(f"GCPGCSPutKeyOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="GCPGCSPutKeyOp.run:"
        )

        logger.info(
            f"GCPGCSPutKeyOp.run: Uploading object to GCS "
            f"{self.input_type=} | {type(msg.data)=} | to {self.bucket} | {self.key=}"
        )

        self.gcs_conn.put_key(
            bucket=self.bucket,
            key=self.key,
            data=msg.data,
            input_type=self.input_type,
            replace=self.replace,
        )

        logger.info(
            f"GCPGCSPutKeyOp.run: Done uploading to GCS with {self.input_type=} for "
            f"{self.key=}"
        )
        return OpMsg(
            data=None,
            metadata=GCPGCSPutKeyOpMetadataModel(
                bucket=self.bucket,
                key=self.key,
                input_type=self.input_type,
            ),
            audit=GCPGCSPutKeyOpAuditModel(),
        )
