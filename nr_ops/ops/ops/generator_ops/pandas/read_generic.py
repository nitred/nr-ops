import logging
from io import StringIO
from typing import Any, Dict, Generator, Literal, Optional

import pandas as pd

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class PandasReadGenericOpConfigModel(BaseOpConfigModel):
    input_as_argument: bool = True
    read_type: Literal[
        "read_csv",
        "read_excel",
        "read_json",
        "read_parquet",
        "read_sql",
    ]
    read_kwargs: Dict[str, Any]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PandasReadGenericOpMetadataModel(BaseOpMetadataModel):
    read_kwargs: Dict[str, Any]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PandasReadGenericOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PandasReadGenericOp(BaseGeneratorOp):
    OP_TYPE = "generator.pandas.read_generic"
    OP_CONFIG_MODEL = PandasReadGenericOpConfigModel
    OP_METADATA_MODEL = PandasReadGenericOpMetadataModel
    OP_AUDIT_MODEL = PandasReadGenericOpAuditModel

    templated_fields = None

    def __init__(
        self,
        read_type: str,
        read_kwargs: Dict[str, Any],
        input_as_argument: bool = True,
        **kwargs,
    ):
        self.read_type = read_type
        self.read_kwargs = read_kwargs
        self.input_as_argument = input_as_argument

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"PandasReadGenericOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=None, log_prefix="PandasReadGenericOp.run:"
        )

        read_func = getattr(pd, self.read_type)
        if self.input_as_argument:
            output = read_func(msg.data, **self.read_kwargs)
        else:
            output = read_func(**self.read_kwargs)

        # Special logging for pandas DataFrames
        buf = StringIO()
        output.info(buf=buf)
        buf.seek(0)
        logger.info(f"PandasReadGenericOp.run: DataFrame info:\n{buf.read()}")

        yield OpMsg(
            data=output,
            metadata=PandasReadGenericOpMetadataModel(read_kwargs=self.read_kwargs),
            audit=PandasReadGenericOpAuditModel(),
        )
