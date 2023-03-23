import io
import logging
from typing import Any, Dict, List, Literal

from pydantic import StrictStr, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel
from nr_ops.ops.ops.connector_ops.interfaces.base import validate_hook_type_and_config

logger = logging.getLogger(__name__)


class GCPGCSConnOpConfigModel(BaseOpConfigModel):
    hook_type: Literal[
        "connector.hooks.airflow_gcp_gcs_hook",
    ]
    hook_config: Dict[StrictStr, Any]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    _validate_hook_type_and_config = root_validator(allow_reuse=True)(
        validate_hook_type_and_config
    )


class GCPGCSConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPGCSConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPGCSConnOp(BaseConnectorOp):
    OP_TYPE = "connector.gcp.gcs"
    OP_CONFIG_MODEL = GCPGCSConnOpConfigModel
    OP_METADATA_MODEL = GCPGCSConnOpMetadataModel
    OP_AUDIT_MODEL = GCPGCSConnOpAuditModel

    templated_fields = None

    def __init__(self, hook_type: str, hook_config: Dict[str, Any], **kwargs):
        self.hook_type = hook_type
        self.hook_config = hook_config
        self.templated_fields = kwargs.get("templated_fields", [])

        from nr_ops.ops.op_collection import OP_COLLECTION

        self.hook_op = OP_COLLECTION[self.hook_type](**self.hook_config)
        self.hook = self.hook_op.run().data

        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        # Will be populated in run()
        self.gcs_hook: GCSHook = None

    def list_keys(self, bucket: str, prefix: str, delimiter: str = "/") -> List[str]:
        """."""
        keys = self.gcs_hook.list(
            bucket_name=bucket,
            prefix=prefix,
            delimiter=delimiter,
        )
        return keys

    def get_key(self, bucket: str, key: str, output_type: str):
        """."""
        bytes_obj = self.gcs_hook.download(bucket_name=bucket, object_name=key)
        if output_type == "bytes":
            return bytes_obj
        elif output_type == "bytesio":
            bytesio_obj = io.BytesIO(bytes_obj)
            bytesio_obj.seek(0)
            return bytesio_obj
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
        if replace is True:
            raise NotImplementedError(f"{replace=} is not implemented yet.")

        if input_type == "bytes":
            self.gcs_hook.upload(
                bucket_name=bucket,
                object_name=key,
                data=data,
                # TODO: Make this configurable after figuring out what effect
                #  it has.
                encoding="utf-8",
            )
        elif input_type == "bytesio":
            self.gcs_hook.upload(
                bucket_name=bucket,
                object_name=key,
                # NOTE: There is no file_obj based upload method in GCSHook.
                # Therefore, we have to read the data as bytes.
                data=data.read(),
                # TODO: Make this configurable after figuring out what effect it has
                encoding="utf-8",
            )
        else:
            raise NotImplementedError()

    def delete_key(
        self,
        bucket: str,
        key: str,
    ):
        self.gcs_hook.delete(
            bucket_name=bucket,
            object_name=key,
        )
        self.gcs_hook

    def is_key_exists(self, bucket: str, key: str) -> bool:
        """."""
        is_exists = self.gcs_hook.exists(bucket_name=bucket, object_name=key)
        return is_exists

    def get_blob(self, bucket: str, key: str) -> bool:
        """."""
        client = self.gcs_hook.get_conn()
        bucket = client.bucket(bucket)
        blob = bucket.blob(blob_name=key)
        return blob

    def run(self) -> OpMsg:
        """."""
        logger.info(f"S3ConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=None, msg=None, log_prefix="S3ConnOp.run:")

        if self.hook_type == "connector.hooks.airflow_gcp_gcs_hook":
            from airflow.providers.google.cloud.hooks.gcs import GCSHook

            self.gcs_hook: GCSHook = self.hook

        else:
            raise NotImplementedError()

        return OpMsg(
            data=self,
            metadata=GCPGCSConnOpMetadataModel(),
            audit=GCPGCSConnOpAuditModel(),
        )
