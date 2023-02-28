import logging
import time
from typing import Generator, Optional

from pydantic import conint

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class SleepGeneratorOpConfigModel(BaseOpConfigModel):
    seconds: conint(ge=0) = 0

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SleepGeneratorOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SleepGeneratorOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SleepGeneratorOp(BaseGeneratorOp):
    OP_TYPE = "generator.sleep"
    OP_CONFIG_MODEL = SleepGeneratorOpConfigModel
    OP_METADATA_MODEL = SleepGeneratorOpMetadataModel
    OP_AUDIT_MODEL = SleepGeneratorOpAuditModel

    templated_fields = None

    def __init__(self, seconds: int, **kwargs):
        self.seconds = seconds
        self.templated_fields = kwargs.get("templated_fields", [])

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"SleepGeneratorOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=None, log_prefix="SleepGeneratorOp.run:"
        )

        logger.info(f"SleepGeneratorOp.run: Sleeping for {self.seconds=}")
        time.sleep(self.seconds)
        logger.info(
            f"SleepGeneratorOp.run: Done sleeping for {self.seconds=}. "
            f"Yielding input OpMsg as is."
        )

        yield OpMsg(
            data=msg.data,
            metadata=SleepGeneratorOpMetadataModel(),
            audit=SleepGeneratorOpAuditModel(),
        )
