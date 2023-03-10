import logging
from typing import Any, Dict, Generator, List, Optional

import pandas as pd
from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpTimeStepMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseOpConfigModel, BaseTimeStepOp

logger = logging.getLogger(__name__)


class SimpleTimeStepOpConfigModel(BaseOpConfigModel):
    start: StrictStr
    end: StrictStr
    tz: StrictStr
    metadata: Optional[Dict[str, Any]] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SimpleTimeStepOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SimpleTimeStepOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SimpleTimeStepOp(BaseTimeStepOp):
    OP_TYPE = "ts.simple"
    OP_CONFIG_MODEL = SimpleTimeStepOpConfigModel
    OP_METADATA_MODEL = SimpleTimeStepOpMetadataModel
    OP_AUDIT_MODEL = SimpleTimeStepOpAuditModel

    templated_fields = None

    def __init__(
        self,
        start: str,
        end: str,
        tz: str,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self.start = start
        self.end = end
        self.tz = tz
        self.metadata = metadata

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(self) -> Generator[OpTimeStepMsg, None, None]:
        """."""
        logger.info(f"SimpleTimeStepOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=None, msg=None, log_prefix="SimpleTimeStepOp.run:")

        time_step = TimeStep(
            start=self.start,
            end=self.end,
            tz=self.tz,
            metadata=self.metadata,
        )

        logger.info(f"SimpleTimeStepOp.run: {time_step=}")

        yield OpTimeStepMsg(
            data=time_step,
            metadata=SimpleTimeStepOpMetadataModel(),
            audit=SimpleTimeStepOpAuditModel(),
        )
