import logging
import time
from typing import Generator, Optional

from pydantic import StrictBool, conint

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class SkipEmptyInputGeneratorOpConfigModel(BaseOpConfigModel):
    skip_zero_length: StrictBool = True
    skip_none: StrictBool = True

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SkipEmptyInputGeneratorOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SkipEmptyInputGeneratorOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SkipEmptyInputGeneratorOp(BaseGeneratorOp):
    OP_TYPE = "generator.skip_empty_input"
    OP_CONFIG_MODEL = SkipEmptyInputGeneratorOpConfigModel
    OP_METADATA_MODEL = SkipEmptyInputGeneratorOpMetadataModel
    OP_AUDIT_MODEL = SkipEmptyInputGeneratorOpAuditModel

    templated_fields = None

    def __init__(self, skip_zero_length: bool = True, skip_none: bool = True, **kwargs):
        self.skip_zero_length = skip_zero_length
        self.skip_none = skip_none

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"SkipEmptyInputGeneratorOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="SkipEmptyInputGeneratorOp.run:"
        )

        if self.skip_none and msg.data is None:
            logger.info(
                f"SkipEmptyInputGeneratorOp.run: Skipping input OpMsg because "
                f"{self.skip_none=} and {type(msg.data)=}"
            )
            return

        if self.skip_zero_length and len(msg.data) == 0:
            logger.info(
                f"SkipEmptyInputGeneratorOp.run: Skipping input OpMsg because "
                f"{self.skip_zero_length=} and len({msg.data})=0 for {type(msg.data)=}"
            )
            return

        logger.info(
            f"SkipEmptyInputGeneratorOp.run: Done checking input for {self.skip_zero_length=}"
            f"and {self.skip_none=}. msg.data is not empty. Yielding OpMsg."
        )

        yield OpMsg(
            data=msg.data,
            metadata=SkipEmptyInputGeneratorOpMetadataModel(),
            audit=SkipEmptyInputGeneratorOpAuditModel(),
        )
