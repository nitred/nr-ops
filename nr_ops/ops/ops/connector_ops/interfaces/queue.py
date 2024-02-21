import logging
from typing import Any, Dict, Literal

from pydantic import StrictStr, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel
from nr_ops.ops.ops.connector_ops.hooks.python_queue import PythonQueueHookConnOp
from nr_ops.ops.ops.connector_ops.interfaces.base import validate_hook_type_and_config

logger = logging.getLogger(__name__)


class QueueConnOpConfigModel(BaseOpConfigModel):
    hook_type: Literal[
        "connector.hooks.python.queue",
    ]
    hook_config: Dict[StrictStr, Any]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    _validate_hook_type_and_config = root_validator(allow_reuse=True)(
        validate_hook_type_and_config
    )


class QueueConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class QueueConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class QueueConnOp(BaseConnectorOp):
    OP_TYPE = "connector.queue"
    OP_CONFIG_MODEL = QueueConnOpConfigModel
    OP_METADATA_MODEL = QueueConnOpMetadataModel
    OP_AUDIT_MODEL = QueueConnOpAuditModel

    templated_fields = None

    def __init__(self, hook_type: str, hook_config: Dict[str, Any], **kwargs):
        self.hook_type = hook_type
        self.hook_config = hook_config
        self.templated_fields = kwargs.get("templated_fields", [])

        from nr_ops.ops.op_collection import OP_COLLECTION

        self.hook_op = OP_COLLECTION[self.hook_type](**self.hook_config)
        self.hook = self.hook_op.run().data

        if self.hook_type == "connector.hooks.python.queue":
            self.hook: PythonQueueHookConnOp
        else:
            raise NotImplementedError()

    def get_reference(self):
        """."""
        if self.hook_type == "connector.hooks.python.queue":
            return self.hook.get_reference()
        else:
            raise NotImplementedError()

    def put(self, item: Any):
        """."""
        if self.hook_type == "connector.hooks.python.queue":
            self.hook.append(item=item)
        else:
            raise NotImplementedError()

    def get(self, item: Any):
        """."""
        if self.hook_type == "connector.hooks.python.queue":
            self.hook.append(item=item)
        else:
            raise NotImplementedError()

    def run(self) -> OpMsg:
        """."""
        logger.info(f"QueueConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=None, msg=None, log_prefix="QueueConnOp.run:")

        if self.hook_type == "connector.hooks.python.queue":
            pass
        else:
            raise NotImplementedError()

        return OpMsg(
            data=self,
            metadata=QueueConnOpMetadataModel(),
            audit=QueueConnOpAuditModel(),
        )
