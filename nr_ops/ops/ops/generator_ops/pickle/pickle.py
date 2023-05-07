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


class PickleGeneratorOpConfigModel(BaseOpConfigModel):
    output_type: Literal["bytes", "bytesio"]
    protocol: conint(ge=0, le=5) = 4
    fix_imports: Optional[StrictBool] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PickleGeneratorOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PickleGeneratorOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PickleGeneratorOp(BaseGeneratorOp):
    OP_TYPE = "generator.pickle"
    OP_CONFIG_MODEL = PickleGeneratorOpConfigModel
    OP_METADATA_MODEL = PickleGeneratorOpMetadataModel
    OP_AUDIT_MODEL = PickleGeneratorOpAuditModel

    templated_fields = None

    def __init__(
        self,
        output_type: Literal["bytes", "bytesio"],
        protocol: int = 4,
        fix_imports: Optional[StrictBool] = None,
        **kwargs,
    ):
        self.output_type = output_type
        self.protocol = protocol
        self.fix_imports = fix_imports

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"PickleGeneratorOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="PickleGeneratorOp.run:"
        )

        logger.info(f"PickleGeneratorOp.run: ")

        pickle_io = io.BytesIO()

        pickle_kwargs = {"protocol": self.protocol}
        if self.fix_imports is not None:
            pickle_kwargs["fix_imports"] = self.fix_imports

        pickle.dump(msg.data, pickle_io, **pickle_kwargs)
        pickle_io.seek(0)

        if self.output_type == "bytes":
            output = pickle_io.read()
            f"PickleGeneratorOp.run: pickled bytes {len(output)=:,} bytes."
        elif self.output_type == "bytesio":
            output = pickle_io
        else:
            raise NotImplementedError()

        logger.info(
            f"PickleGeneratorOp.run: Yielding pickled out of type {self.output_type=}."
        )

        yield OpMsg(
            data=output,
            metadata=PickleGeneratorOpMetadataModel(),
            audit=PickleGeneratorOpAuditModel(),
        )
