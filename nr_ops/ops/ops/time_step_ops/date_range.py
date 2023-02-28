import logging
from typing import Any, Dict, Generator, List, Literal, Optional

import pandas as pd
from pydantic import StrictBool, StrictStr, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpTimeStepMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseOpConfigModel, BaseTimeStepOp

logger = logging.getLogger(__name__)


class DateRangeTimeStepOpConfigModel(BaseOpConfigModel):
    start: StrictStr
    end: StrictStr
    tz: StrictStr
    freq: StrictStr
    inclusive: Literal["both", "neither", "left", "right"]
    metadata: Optional[Dict[str, Any]] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class DateRangeTimeStepOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class DateRangeTimeStepOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class DateRangeTimeStepOp(BaseTimeStepOp):
    OP_TYPE = "ts.date_range"
    OP_CONFIG_MODEL = DateRangeTimeStepOpConfigModel
    OP_METADATA_MODEL = DateRangeTimeStepOpMetadataModel
    OP_AUDIT_MODEL = DateRangeTimeStepOpAuditModel

    templated_fields = None

    def __init__(
        self,
        start: str,
        end: str,
        freq: str,
        tz: str,
        inclusive: str,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self.start = start
        self.end = end
        self.freq = freq
        self.tz = tz
        self.inclusive = inclusive
        self.metadata = metadata

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(self) -> Generator[OpTimeStepMsg, None, None]:
        """."""
        logger.info(f"DateRangeTimeStepOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="DateRangeTimeStepOp.run:"
        )

        date_range = pd.date_range(
            start=self.start,
            end=self.end,
            freq=self.freq,
            tz=self.tz,
            inclusive=self.inclusive,
        )

        for start_date in date_range:
            time_step = TimeStep(
                start=start_date,
                end=start_date + pd.Timedelta(self.freq),
                tz=self.tz,
                metadata=self.metadata,
            )
            yield OpTimeStepMsg(
                data=time_step,
                metadata=DateRangeTimeStepOpMetadataModel(),
                audit=DateRangeTimeStepOpAuditModel(),
            )
