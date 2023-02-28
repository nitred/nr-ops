import logging
from typing import Any, Dict, Generator, List, Optional

import pandas as pd
from pydantic import StrictStr, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpTimeStepMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseOpConfigModel, BaseTimeStepOp

logger = logging.getLogger(__name__)


class SimpleStartOffsetTimeStepOpConfigModel(BaseOpConfigModel):
    start: StrictStr
    end_offset: StrictStr
    tz: StrictStr
    metadata: Optional[Dict[str, Any]] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SimpleStartOffsetTimeStepOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SimpleStartOffsetTimeStepOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class SimpleStartOffsetTimeStepOp(BaseTimeStepOp):
    OP_TYPE = "ts.simple_start_offset"
    OP_CONFIG_MODEL = SimpleStartOffsetTimeStepOpConfigModel
    OP_METADATA_MODEL = SimpleStartOffsetTimeStepOpMetadataModel
    OP_AUDIT_MODEL = SimpleStartOffsetTimeStepOpAuditModel

    templated_fields = None

    def __init__(
        self,
        start: str,
        end_offset: str,
        tz: str,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self.start = start
        self.end_offset = end_offset
        self.tz = tz
        self.metadata = metadata

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(self) -> Generator[OpTimeStepMsg, None, None]:
        """."""
        logger.info(f"SimpleStartOffsetTimeStepOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="SimpleStartOffsetTimeStepOp.run:"
        )

        end = pd.Timestamp(self.start, tz=self.tz) + pd.Timedelta(self.end_offset)

        time_step = TimeStep(
            start=self.start, end=end, tz=self.tz, metadata=self.metadata
        )

        logger.info(f"SimpleStartOffsetTimeStepOp.run: {time_step=}")

        yield OpTimeStepMsg(
            data=time_step,
            metadata=SimpleStartOffsetTimeStepOpMetadataModel(),
            audit=SimpleStartOffsetTimeStepOpAuditModel(),
        )
