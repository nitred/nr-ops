"""."""
import json
import logging
import os
from typing import Literal, Optional

from numpy.ma import copy
from pydantic import BaseModel, StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class PythonListHookConnOpConfigModel(BaseOpConfigModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PythonListHookConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PythonListHookConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PythonListHookConnOp(BaseConnectorOp):
    OP_TYPE = "connector.hooks.python.list"
    OP_CONFIG_MODEL = PythonListHookConnOpConfigModel
    OP_METADATA_MODEL = PythonListHookConnOpMetadataModel
    OP_AUDIT_MODEL = PythonListHookConnOpAuditModel

    templated_fields = None

    def __init__(self, **kwargs):
        self.templated_fields = kwargs.get("templated_fields", [])

        # NOTE: This is the list :)
        self._list = []

    def run(self) -> OpMsg:
        """."""
        logger.info(f"PythonListHookConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="PythonListHookConnOp.run:"
        )

        return OpMsg(
            data=self,
            metadata=PythonListHookConnOpMetadataModel(),
            audit=PythonListHookConnOpAuditModel(),
        )

    def get_reference(self):
        """."""
        return self._list

    def get_deepcopy(self):
        """."""
        return copy.deepcopy(self._list)

    def get_reference_and_reinit(self):
        """."""
        _list = self._list
        self._list = []
        return _list

    def put_append(self, item):
        """."""
        self._list.append(item)

    def put_extend(self, item):
        """."""
        self._list.extend(item)

    def size(self):
        """."""
        return len(self._list)
