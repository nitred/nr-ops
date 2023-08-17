from __future__ import annotations

import logging
from io import BytesIO
from typing import Any, Dict, Generator, List, Literal, Optional, Union

from pydantic import BaseModel, StrictStr, conlist, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel
from nr_ops.ops.ops.connector_ops.hooks.python_counter import PythonCounterHookConnOp
from nr_ops.ops.ops.connector_ops.interfaces.base import validate_hook_type_and_config

logger = logging.getLogger(__name__)


class CounterConnOpConfigModel(BaseOpConfigModel):
    hook_type: Literal[
        "connector.hooks.python.counter",
    ]
    hook_config: Dict[StrictStr, Any]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    _validate_hook_type_and_config = root_validator(allow_reuse=True)(
        validate_hook_type_and_config
    )


class CounterConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class CounterConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class CounterConnOp(BaseConnectorOp):
    OP_TYPE = "connector.counter"
    OP_CONFIG_MODEL = CounterConnOpConfigModel
    OP_METADATA_MODEL = CounterConnOpMetadataModel
    OP_AUDIT_MODEL = CounterConnOpAuditModel

    templated_fields = None

    def __init__(self, hook_type: str, hook_config: Dict[str, Any], **kwargs):
        self.hook_type = hook_type
        self.hook_config = hook_config
        self.templated_fields = kwargs.get("templated_fields", [])

        from nr_ops.ops.op_collection import OP_COLLECTION

        self.hook_op = OP_COLLECTION[self.hook_type](**self.hook_config)
        self.hook = self.hook_op.run().data

        if self.hook_type == "connector.hooks.python.counter":
            self.hook: PythonCounterHookConnOp
        else:
            raise NotImplementedError()

    # NOTE: Returns Union[PythonCounterHookConnOp] but the interface is CounterConnOp
    def get_reference(self) -> CounterConnOp:
        """."""
        if self.hook_type == "connector.hooks.python.counter":
            return self.hook.get_reference()
        else:
            raise NotImplementedError()

    # NOTE: Returns Union[PythonCounterHookConnOp] but the interface is CounterConnOp
    def get_reference_and_reinit(self) -> CounterConnOp:
        """."""
        if self.hook_type == "connector.hooks.python.counter":
            return self.hook.get_reference_and_reinit()
        else:
            raise NotImplementedError()

    def put_update_dict(self, item: Any):
        """."""
        if self.hook_type == "connector.hooks.python.counter":
            self.hook.put_update_dict(item=item)
        else:
            raise NotImplementedError()

    def put_update_list_of_dicts(self, item: Any):
        """."""
        if self.hook_type == "connector.hooks.python.counter":
            self.hook.put_update_list_of_dicts(item=item)
        else:
            raise NotImplementedError()

    def run(self) -> OpMsg:
        """."""
        logger.info(f"CounterConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=None, msg=None, log_prefix="CounterConnOp.run:")

        if self.hook_type == "connector.hooks.python.counter":
            pass
        else:
            raise NotImplementedError()

        return OpMsg(
            data=self,
            metadata=CounterConnOpMetadataModel(),
            audit=CounterConnOpAuditModel(),
        )
