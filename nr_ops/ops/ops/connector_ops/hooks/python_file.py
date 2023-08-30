"""."""
import logging
from typing import Literal

from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class PythonFileHookConnOpConfigModel(BaseOpConfigModel):
    path: StrictStr
    mode: Literal[
        "r",
        "r+",
        "w",
        "w+",
        "a",
        "a+",
        "x",
        "rb",
        "rb+",
        "wb",
        "wb+",
        "ab",
        "ab+",
        "xb",
    ]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PythonFileHookConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PythonFileHookConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PythonFileHookConnOp(BaseConnectorOp):
    OP_TYPE = "connector.hooks.python.file"
    OP_CONFIG_MODEL = PythonFileHookConnOpConfigModel
    OP_METADATA_MODEL = PythonFileHookConnOpMetadataModel
    OP_AUDIT_MODEL = PythonFileHookConnOpAuditModel

    templated_fields = None

    def __init__(
        self,
        path: str,
        mode: Literal[
            "r",
            "r+",
            "w",
            "w+",
            "a",
            "a+",
            "x",
            "rb",
            "rb+",
            "wb",
            "wb+",
            "ab",
            "ab+",
            "xb",
        ],
        **kwargs,
    ):
        self.path = path
        self.mode = mode
        self.templated_fields = kwargs.get("templated_fields", [])

        self._file = open(self.path, self.mode)

    def run(self) -> OpMsg:
        """."""
        logger.info(f"PythonFileHookConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="PythonFileHookConnOp.run:"
        )

        return OpMsg(
            data=self,
            metadata=PythonFileHookConnOpMetadataModel(),
            audit=PythonFileHookConnOpAuditModel(),
        )

    def get_reference(self):
        """."""
        return self._file

    def get_read(self):
        """."""
        self._file.read()

    def put_write(self, item):
        """."""
        self._file.write(item)

    def put_write_line(self, item):
        """."""
        self._file.write(item)
        self._file.write("\n")

    def put_write_and_flush(self, item):
        """."""
        self._file.write(item)
        self._file.flush()

    def put_write_line_and_flush(self, item):
        """."""
        self._file.write(item)
        self._file.write("\n")
        self._file.flush()

    # TODO: Seek
