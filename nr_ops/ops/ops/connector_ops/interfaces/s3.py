import logging
from io import BytesIO
from typing import Any, Dict, Generator, List, Literal, Optional

from pydantic import BaseModel, StrictStr, conlist, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel
from nr_ops.ops.ops.connector_ops.interfaces.base import validate_hook_type_and_config

logger = logging.getLogger(__name__)


class S3ConnOpConfigModel(BaseOpConfigModel):
    hook_type: Literal[
        "connector.hooks.airflow_s3_hook",
    ]
    hook_config: Dict[StrictStr, Any]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    _validate_hook_type_and_config = root_validator(allow_reuse=True)(
        validate_hook_type_and_config
    )


class S3ConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class S3ConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class S3ConnOp(BaseConnectorOp):
    OP_TYPE = "connector.s3"
    OP_CONFIG_MODEL = S3ConnOpConfigModel
    OP_METADATA_MODEL = S3ConnOpMetadataModel
    OP_AUDIT_MODEL = S3ConnOpAuditModel

    templated_fields = None

    def __init__(self, hook_type: str, hook_config: Dict[str, Any], **kwargs):
        self.hook_type = hook_type
        self.hook_config = hook_config
        self.templated_fields = kwargs.get("templated_fields", [])

        from nr_ops.ops.op_collection import OP_COLLECTION

        self.hook_op = OP_COLLECTION[self.hook_type](**self.hook_config)
        self.hook = self.hook_op.run().data

        if self.hook_type == "connector.hooks.airflow_s3_hook":
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            self.hook: S3Hook
        else:
            raise NotImplementedError()

    def list_keys(self, bucket: str, prefix: str, delimiter: str = "/"):
        """."""
        if self.hook_type == "connector.hooks.airflow_s3_hook":
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            self.hook: S3Hook
            keys = self.hook.list_keys(
                bucket_name=bucket,
                prefix=prefix,
                delimiter=delimiter,
            )
            return keys
        else:
            raise NotImplementedError()

    def get_key(self, bucket: str, key: str, output_type: str):
        """."""
        if self.hook_type == "connector.hooks.airflow_s3_hook":
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            self.hook: S3Hook
            transfer_obj = self.hook.get_key(bucket_name=bucket, key=key)
            if output_type == "bytes":
                return transfer_obj.get()["Body"].read()
            elif output_type == "bytesio":
                return transfer_obj.get()["Body"]
            else:
                raise NotImplementedError()
        else:
            raise NotImplementedError()

    def put_key(
        self,
        bucket: str,
        key: str,
        input_type: str,
        replace: bool,
        data: Any,
    ):
        if self.hook_type == "connector.hooks.airflow_s3_hook":
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            self.hook: S3Hook
            if input_type == "bytes":
                self.hook.load_bytes(
                    bytes_data=data, bucket_name=bucket, key=key, replace=replace
                )
            elif input_type == "bytesio":
                self.hook.load_file_obj(
                    file_obj=data, bucket_name=bucket, key=key, replace=replace
                )
            else:
                raise NotImplementedError()
        else:
            raise NotImplementedError()

    def run(self) -> OpMsg:
        """."""
        logger.info(f"S3ConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=None, msg=None, log_prefix="S3ConnOp.run:")

        if self.hook_type == "connector.hooks.airflow_s3_hook":
            pass
        else:
            raise NotImplementedError()

        return OpMsg(
            data=self,
            metadata=S3ConnOpMetadataModel(),
            audit=S3ConnOpAuditModel(),
        )
