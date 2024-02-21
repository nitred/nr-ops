import logging
from io import StringIO
from typing import Any, Dict, Generator, Literal, Optional

import pandas as pd
from pydantic import confloat

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.utils.pandas.log_df_info import log_df_info

logger = logging.getLogger(__name__)


class PandasTrainTestSplitConfigModel(BaseOpConfigModel):
    test_size: confloat(ge=0, le=1)
    random_state: Optional[int] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PandasTrainTestSplitMetadataModel(BaseOpMetadataModel):
    test_size: float
    random_state: Optional[int]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PandasTrainTestSplitAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PandasTrainTestSplit(BaseGeneratorOp):
    OP_TYPE = "generator.pandas.train_and_test_split"
    OP_CONFIG_MODEL = PandasTrainTestSplitConfigModel
    OP_METADATA_MODEL = PandasTrainTestSplitMetadataModel
    OP_AUDIT_MODEL = PandasTrainTestSplitAuditModel

    templated_fields = None

    def __init__(
        self,
        test_size: float,
        random_state: Optional[int],
        **kwargs,
    ):
        self.test_size = test_size
        self.random_state = random_state

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"PandasTrainTestSplit.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="PandasTrainTestSplit.run:"
        )

        test_df = msg.data.sample(frac=self.test_size, random_state=self.random_state)
        train_df = msg.data.drop(index=test_df.index)

        log_df_info(test_df, log_prefix="PandasTrainTestSplit.run: test_df: ")
        log_df_info(train_df, log_prefix="PandasTrainTestSplit.run: train_df: ")

        yield OpMsg(
            data={
                "train_df": train_df,
                "test_df": test_df,
            },
            metadata=PandasTrainTestSplitMetadataModel(
                test_size=self.test_size,
                random_state=self.random_state,
            ),
            audit=PandasTrainTestSplitAuditModel(),
        )
