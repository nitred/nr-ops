import logging
import re
from typing import Generator, List, Optional

from pydantic import StrictStr, validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.s3 import S3ConnOp

logger = logging.getLogger(__name__)


class S3ListKeysOpConfigModel(BaseOpConfigModel):
    s3_conn_id: StrictStr
    bucket: StrictStr
    prefix: StrictStr
    regex: Optional[StrictStr] = None
    iterate_over_keys: bool = True

    @validator("prefix", pre=False)
    def validate_prefix(cls, prefix: str):
        if prefix and not prefix.endswith("/"):
            raise ValueError(f"{prefix=} must end with a '/' if not empty")

        if prefix.startswith("/"):
            raise ValueError(f"{prefix=} must not start with a '/'")

        return prefix

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class S3ListKeysOpMetadataModel(BaseOpMetadataModel):
    bucket: StrictStr
    prefix: StrictStr
    key: Optional[StrictStr] = None
    keys: Optional[List[StrictStr]] = None
    filename: Optional[StrictStr] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class S3ListKeysOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class S3ListKeysOp(BaseGeneratorOp):
    OP_TYPE = "generator.s3.list_keys"
    OP_CONFIG_MODEL = S3ListKeysOpConfigModel
    OP_METADATA_MODEL = S3ListKeysOpMetadataModel
    OP_AUDIT_MODEL = S3ListKeysOpAuditModel

    templated_fields = None

    def __init__(
        self,
        s3_conn_id: str,
        bucket: str,
        prefix: str,
        regex: Optional[str] = None,
        iterate_over_keys: bool = True,
        **kwargs,
    ):
        """."""
        self.s3_conn_id = s3_conn_id
        self.bucket = bucket
        self.prefix = prefix
        self.regex = regex
        self.iterate_over_keys = iterate_over_keys

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.s3_conn: S3ConnOp = op_manager.get_connector(op_id=self.s3_conn_id)

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"S3ListKeysOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=time_step, msg=msg, log_prefix="S3ListKeysOp.run:")

        keys = self.s3_conn.list_keys(bucket=self.bucket, prefix=self.prefix)
        logger.info(
            f"S3ListKeysOp.run: {len(keys)=} found in {self.bucket=} with "
            f"{self.prefix=}. Sample keys: \n{keys[:5]}"
        )

        if self.regex is not None:
            compiled_regex = re.compile(self.regex)
            keys = [key for key in keys if compiled_regex.match(key)]
            logger.info(
                f"S3ListKeysOp.run: Applied regex matching on keys. "
                f" {len(keys)=} matched with {self.regex=}. Sample keys: \n{keys[:5]}"
            )

        if self.iterate_over_keys:
            for key in keys:
                filename = key[len(self.prefix) :]
                logger.info(f"S3ListKeysOp.run: Yielding {key=}, {filename=}")
                yield OpMsg(
                    data=key,
                    metadata=S3ListKeysOpMetadataModel(
                        bucket=self.bucket,
                        prefix=self.prefix,
                        keys=None,
                        key=key,
                        filename=filename,
                    ),
                    audit=S3ListKeysOpAuditModel(),
                )
        else:
            logger.info(
                f"S3ListKeysOp.run: Yielding all keys of {len(keys)=} as output."
            )
            yield OpMsg(
                data=keys,
                metadata=S3ListKeysOpMetadataModel(
                    bucket=self.bucket,
                    prefix=self.prefix,
                    keys=keys,
                    key=None,
                    filename=None,
                ),
                audit=S3ListKeysOpAuditModel(),
            )
