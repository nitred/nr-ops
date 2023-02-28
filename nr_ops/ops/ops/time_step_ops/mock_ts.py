import logging
from typing import Generator

import pandas as pd

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpTimeStepMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseOpConfigModel, BaseTimeStepOp

logger = logging.getLogger(__name__)


class MockTimeStepOpConfigModel(BaseOpConfigModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MockTimeStepOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MockTimeStepOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MockTimeStepOp(BaseTimeStepOp):
    OP_TYPE = "ts.mock"
    OP_CONFIG_MODEL = MockTimeStepOpConfigModel
    OP_METADATA_MODEL = MockTimeStepOpMetadataModel
    OP_AUDIT_MODEL = MockTimeStepOpAuditModel

    templated_fields = None

    def __init__(self, **kwargs):
        self.templated_fields = kwargs.get("templated_fields", [])

    def run(self) -> Generator[OpTimeStepMsg, None, None]:
        """."""
        logger.info(f"MockTimeStepOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=None, msg=None, log_prefix="MockTimeStepOp.run:")

        time_step = TimeStep(
            start=pd.Timestamp.now(tz="UTC"),
            end=pd.Timestamp.now(tz="UTC") + pd.Timedelta(days=1),
            tz="UTC",
        )
        yield OpTimeStepMsg(
            data=time_step,
            metadata=MockTimeStepOpMetadataModel(),
            audit=MockTimeStepOpAuditModel(),
        )
