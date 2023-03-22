import logging
from typing import Generator, Literal, Optional

from pydantic import StrictStr, validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.gcp_gcs import GCPGCSConnOp

logger = logging.getLogger(__name__)


class GCPGCSGetKeyOpConfigModel(BaseOpConfigModel):
    gcs_conn_id: StrictStr
    bucket: StrictStr
    key: StrictStr
    output_type: Literal["bytes", "bytesio"]

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


class GCPGCSGetKeyOpMetadataModel(BaseOpMetadataModel):
    bucket: StrictStr
    key: StrictStr
    output_type: Literal["bytes", "bytesio"]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPGCSGetKeyOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPGCSGetKeyOp(BaseGeneratorOp):
    OP_TYPE = "generator.gcp.gcs.get_key"
    OP_CONFIG_MODEL = GCPGCSGetKeyOpConfigModel
    OP_METADATA_MODEL = GCPGCSGetKeyOpMetadataModel
    OP_AUDIT_MODEL = GCPGCSGetKeyOpAuditModel

    templated_fields = None

    def __init__(
        self,
        gcs_conn_id: str,
        bucket: str,
        key: str,
        output_type: Literal["bytes", "bytesio"],
        **kwargs,
    ):
        """."""
        self.gcs_conn_id = gcs_conn_id
        self.bucket = bucket
        self.key = key
        self.output_type = output_type
        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.gcs_conn: GCPGCSConnOp = op_manager.get_connector(op_id=self.gcs_conn_id)

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"GCPGCSGetKeyOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="GCPGCSGetKeyOp.run:"
        )

        logger.info(
            f"GCPGCSGetKeyOp.run: Getting key {self.key=} with {self.output_type=}"
        )

        output = self.gcs_conn.get_key(
            bucket=self.bucket, key=self.key, output_type=self.output_type
        )

        if self.output_type == "bytes":
            logger.info(
                f"GCPGCSGetKeyOp.run: Download with {len(output):,} bytes "
                f"i.e. approx {len(output)//1000//1000:,} MiB"
            )

        logger.info(
            f"GCPGCSGetKeyOp.run: Return data with {self.output_type=} for {self.key=}"
        )
        yield OpMsg(
            data=output,
            metadata=GCPGCSGetKeyOpMetadataModel(
                bucket=self.bucket,
                key=self.key,
                output_type=self.output_type,
            ),
            audit=GCPGCSGetKeyOpAuditModel(),
        )
