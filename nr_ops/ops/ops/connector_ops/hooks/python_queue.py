"""."""
import json
import logging
import os
import queue
from typing import Literal, Optional

from numpy.ma import copy
from pydantic import BaseModel, StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class PythonQueueHookConnOpConfigModel(BaseOpConfigModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PythonQueueHookConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PythonQueueHookConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PythonQueueHookConnOp(BaseConnectorOp):
    OP_TYPE = "connector.hooks.python.queue"
    OP_CONFIG_MODEL = PythonQueueHookConnOpConfigModel
    OP_METADATA_MODEL = PythonQueueHookConnOpMetadataModel
    OP_AUDIT_MODEL = PythonQueueHookConnOpAuditModel

    templated_fields = None

    def __init__(self, **kwargs):
        self.templated_fields = kwargs.get("templated_fields", [])

        # NOTE: This is the list :)
        self._queue = queue.Queue()

    def run(self) -> OpMsg:
        """."""
        logger.info(f"PythonQueueHookConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="PythonQueueHookConnOp.run:"
        )

        return OpMsg(
            data=self,
            metadata=PythonQueueHookConnOpMetadataModel(),
            audit=PythonQueueHookConnOpAuditModel(),
        )

    def get_reference(self):
        """."""
        return self._queue

    def get_reference_and_reinit(self):
        """."""
        _queue = self._queue
        self._queue = queue.Queue()
        return _queue

    def put(self, item):
        """."""
        self._queue.put(item, block=True)

    def get(self):
        """."""
        self._queue.get(block=True)
