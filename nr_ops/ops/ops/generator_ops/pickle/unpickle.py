import io
import logging
import pickle
import time
from typing import Generator, Literal, Optional

from pydantic import StrictBool, conint

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class UnPickleGeneratorOpConfigModel(BaseOpConfigModel):
    input_type: Literal["bytes"]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class UnPickleGeneratorOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class UnPickleGeneratorOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class UnPickleGeneratorOp(BaseGeneratorOp):
    OP_TYPE = "generator.unpickle"
    OP_CONFIG_MODEL = UnPickleGeneratorOpConfigModel
    OP_METADATA_MODEL = UnPickleGeneratorOpMetadataModel
    OP_AUDIT_MODEL = UnPickleGeneratorOpAuditModel

    templated_fields = None

    def __init__(
        self,
        input_type: Literal["bytes", "bytesio"],
        fix_imports: Optional[StrictBool] = None,
        **kwargs,
    ):
        self.input_type = input_type
        self.fix_imports = fix_imports

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"UnPickleGeneratorOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=None, log_prefix="UnPickleGeneratorOp.run:"
        )

        logger.info(f"UnPickleGeneratorOp.run: ")

        pickle_kwargs = {}
        if self.fix_imports is not None:
            pickle_kwargs["fix_imports"] = self.fix_imports

        output = pickle.loads(msg.data, **pickle_kwargs)

        logger.info(
            f"UnPickleGeneratorOp.run: Yielding unpickled input of type "
            f"{self.input_type=}, {type(output)=}"
        )

        yield OpMsg(
            data=output,
            metadata=UnPickleGeneratorOpMetadataModel(),
            audit=UnPickleGeneratorOpAuditModel(),
        )
