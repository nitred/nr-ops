import logging
from typing import Any, Dict, Literal

from pydantic import StrictStr, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel
from nr_ops.ops.ops.connector_ops.hooks.python_file import PythonFileHookConnOp
from nr_ops.ops.ops.connector_ops.interfaces.base import validate_hook_type_and_config

logger = logging.getLogger(__name__)


class FileConnOpConfigModel(BaseOpConfigModel):
    hook_type: Literal[
        "connector.hooks.python.file",
    ]
    hook_config: Dict[StrictStr, Any]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    _validate_hook_type_and_config = root_validator(allow_reuse=True)(
        validate_hook_type_and_config
    )


class FileConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class FileConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class FileConnOp(BaseConnectorOp):
    OP_TYPE = "connector.file"
    OP_CONFIG_MODEL = FileConnOpConfigModel
    OP_METADATA_MODEL = FileConnOpMetadataModel
    OP_AUDIT_MODEL = FileConnOpAuditModel

    templated_fields = None

    def __init__(self, hook_type: str, hook_config: Dict[str, Any], **kwargs):
        self.hook_type = hook_type
        self.hook_config = hook_config
        self.templated_fields = kwargs.get("templated_fields", [])

        from nr_ops.ops.op_collection import OP_COLLECTION

        self.hook_op = OP_COLLECTION[self.hook_type](**self.hook_config)
        self.hook = self.hook_op.run().data

        if self.hook_type == "connector.hooks.python.file":
            self.hook: PythonFileHookConnOp
        else:
            raise NotImplementedError()

    def get_reference(self):
        """."""
        if self.hook_type == "connector.hooks.python.file":
            return self.hook.get_reference()
        else:
            raise NotImplementedError()

    def get_read(self):
        """."""
        if self.hook_type == "connector.hooks.python.file":
            return self.hook.get_read()
        else:
            raise NotImplementedError()

    def put_write(self, item: Any):
        """."""
        if self.hook_type == "connector.hooks.python.file":
            self.hook.put_write(item=item)
        else:
            raise NotImplementedError()

    def put_write_line(self, item: Any):
        """."""
        if self.hook_type == "connector.hooks.python.file":
            self.hook.put_write_line(item=item)
        else:
            raise NotImplementedError()

    def put_write_and_flush(self, item: Any):
        """."""
        if self.hook_type == "connector.hooks.python.file":
            self.hook.put_write_and_flush(item=item)
        else:
            raise NotImplementedError()

    def put_write_line_and_flush(self, item: Any):
        """."""
        if self.hook_type == "connector.hooks.python.file":
            self.hook.put_write_line_and_flush(item=item)
        else:
            raise NotImplementedError()

    def run(self) -> OpMsg:
        """."""
        logger.info(f"FileConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=None, msg=None, log_prefix="FileConnOp.run:")

        if self.hook_type == "connector.hooks.python.file":
            pass
        else:
            raise NotImplementedError()

        return OpMsg(
            data=self,
            metadata=FileConnOpMetadataModel(),
            audit=FileConnOpAuditModel(),
        )
