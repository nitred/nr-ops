"""."""
import json
import logging
import os
from collections import Counter
from typing import Dict, List, Literal, Optional

from numpy.ma import copy
from pydantic import BaseModel, StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class PythonCounterHookConnOpConfigModel(BaseOpConfigModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PythonCounterHookConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PythonCounterHookConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PythonCounterHookConnOp(BaseConnectorOp):
    OP_TYPE = "connector.hooks.python.counter"
    OP_CONFIG_MODEL = PythonCounterHookConnOpConfigModel
    OP_METADATA_MODEL = PythonCounterHookConnOpMetadataModel
    OP_AUDIT_MODEL = PythonCounterHookConnOpAuditModel

    templated_fields = None

    def __init__(self, **kwargs):
        self.templated_fields = kwargs.get("templated_fields", [])

        # NOTE: This is the counter :)
        self._counter = Counter()

    def run(self) -> OpMsg:
        """."""
        logger.info(f"PythonCounterHookConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="PythonCounterHookConnOp.run:"
        )

        return OpMsg(
            data=self,
            metadata=PythonCounterHookConnOpMetadataModel(),
            audit=PythonCounterHookConnOpAuditModel(),
        )

    def get_reference(self):
        """."""
        return self._counter

    def get_reference_and_reinit(self):
        """."""
        _counter = self._counter
        self._counter = Counter()
        return _counter

    def put_update_dict(self, item: Dict[str, int]):
        """."""
        self._counter.update(item)
        logger.info(
            f"PythonCounterHookConnOp.put_update_dict: Counter updated. "
            f"New counter value: {json.dumps(self._counter, indent=2)}"
        )

    def put_update_list_of_dicts(self, item: List[Dict[str, int]]):
        """."""
        for _item in item:
            self._counter.update(_item)
        logger.info(
            f"PythonCounterHookConnOp.put_update_list_of_dicts: Counter updated. "
            f"New counter value: {json.dumps(self._counter, indent=2)}"
        )
