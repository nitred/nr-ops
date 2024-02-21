import logging
from typing import Any, Dict, List, Literal, Optional

from pydantic import StrictBool, StrictStr, validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.s3 import S3ConnOp

logger = logging.getLogger(__name__)


class S3PutKeyOpConfigModel(BaseOpConfigModel):
    s3_conn_id: StrictStr
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

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class S3PutKeyOpMetadataModel(BaseOpMetadataModel):
    bucket: StrictStr
    key: StrictStr
    input_type: Literal["bytes", "bytesio"]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class S3PutKeyOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class S3PutKeyOp(BaseConsumerOp):
    OP_TYPE = "consumer.s3.put_key"
    OP_CONFIG_MODEL = S3PutKeyOpConfigModel
    OP_METADATA_MODEL = S3PutKeyOpMetadataModel
    OP_AUDIT_MODEL = S3PutKeyOpAuditModel

    templated_fields = None

    def __init__(
        self,
        s3_conn_id: str,
        bucket: str,
        key: str,
        replace: bool,
        input_type: Literal["bytes", "bytesio"],
        templated_fields: List[Dict[str, Any]] = None,
        **kwargs,
    ):
        """."""
        self.s3_conn_id = s3_conn_id
        self.bucket = bucket
        self.key = key
        self.replace = replace
        self.input_type = input_type
        self.templated_fields = [] if templated_fields is None else templated_fields

        op_manager = get_global_op_manager()

        self.s3_conn: S3ConnOp = op_manager.get_connector(op_id=self.s3_conn_id)

    def run(self, time_step: TimeStep, msg: Optional[OpMsg] = None) -> OpMsg:
        """."""
        logger.info(f"S3PutKeyOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=time_step, msg=None, log_prefix="S3PutKeyOp.run:")

        logger.info(
            f"S3PutKeyOp.run: Putting key {self.key=} with {self.input_type=}. "
            f"Input {type(msg.data)=}"
        )

        if self.input_type == "bytes":
            logger.info(
                f"S3PutKeyOp.run: Uploading with {len(msg.data):,} bytes "
                f"i.e. approx {len(msg.data)//1000//1000:,} MiB"
            )

        self.s3_conn.put_key(
            bucket=self.bucket,
            key=self.key,
            input_type=self.input_type,
            replace=self.replace,
            data=msg.data,
        )

        logger.info(
            f"S3PutKeyOp.run: Uploading object to s3 "
            f"{self.input_type=} | {type(msg.data)=} | to {self.bucket} | {self.key=}"
        )

        return OpMsg(
            data=None,
            metadata=S3PutKeyOpMetadataModel(
                bucket=self.bucket,
                key=self.key,
                input_type=self.input_type,
            ),
            audit=S3PutKeyOpAuditModel(),
        )
