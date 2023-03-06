""".
https://googleapis.github.io/google-api-python-client/docs/dyn/analyticsreporting_v4.html
"""
import logging
from io import BytesIO
from typing import Any, Dict, Generator, List, Literal, Optional

from pydantic import BaseModel, StrictStr, conlist, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel
from nr_ops.ops.ops.connector_ops.hooks.python_list import PythonListHookConnOp
from nr_ops.ops.ops.connector_ops.interfaces.base import validate_hook_type_and_config

logger = logging.getLogger(__name__)


class ListConnOpConfigModel(BaseOpConfigModel):
    hook_type: Literal[
        "connector.hooks.python.list",
    ]
    hook_config: Dict[StrictStr, Any]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    _validate_hook_type_and_config = root_validator(allow_reuse=True)(
        validate_hook_type_and_config
    )


class ListConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class ListConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class ListConnOp(BaseConnectorOp):
    OP_TYPE = "connector.list"
    OP_CONFIG_MODEL = ListConnOpConfigModel
    OP_METADATA_MODEL = ListConnOpMetadataModel
    OP_AUDIT_MODEL = ListConnOpAuditModel

    templated_fields = None

    def __init__(self, hook_type: str, hook_config: Dict[str, Any], **kwargs):
        self.hook_type = hook_type
        self.hook_config = hook_config
        self.templated_fields = kwargs.get("templated_fields", [])

        from nr_ops.ops.op_collection import OP_COLLECTION

        self.hook_op = OP_COLLECTION[self.hook_type](**self.hook_config)
        self.hook = self.hook_op.run().data

        if self.hook_type == "connector.hooks.python.list":
            self.hook: PythonListHookConnOp
        else:
            raise NotImplementedError()

    def get_reference(self):
        """."""
        if self.hook_type == "connector.hooks.python.list":
            return self.hook.get_reference()
        else:
            raise NotImplementedError()

    def get_reference_and_reinit(self):
        """."""
        if self.hook_type == "connector.hooks.python.list":
            return self.hook.get_reference_and_reinit()
        else:
            raise NotImplementedError()

    def get_deepcopy(self):
        """."""
        if self.hook_type == "connector.hooks.python.list":
            return self.hook.get_deepcopy()
        else:
            raise NotImplementedError()

    def put_append(self, item: Any):
        """."""
        if self.hook_type == "connector.hooks.python.list":
            self.hook.put_append(item=item)
        else:
            raise NotImplementedError()

    def run(self) -> OpMsg:
        """."""
        logger.info(f"ListConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=None, msg=None, log_prefix="ListConnOp.run:")

        if self.hook_type == "connector.hooks.python.list":
            pass
        else:
            raise NotImplementedError()

        return OpMsg(
            data=self,
            metadata=ListConnOpMetadataModel(),
            audit=ListConnOpAuditModel(),
        )
