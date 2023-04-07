import itertools
import logging
from typing import Generator, Literal, Optional, Union

from pydantic import StrictBool, StrictInt, StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseGeneratorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class BatchGeneratorOpConfigModel(BaseOpConfigModel):
    batch_size: StrictInt

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BatchGeneratorOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BatchGeneratorOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BatchGeneratorOp(BaseGeneratorOp):
    OP_TYPE = "generator.batch"
    OP_CONFIG_MODEL = BatchGeneratorOpConfigModel
    OP_METADATA_MODEL = BatchGeneratorOpMetadataModel
    OP_AUDIT_MODEL = BatchGeneratorOpAuditModel

    templated_fields = None

    def __init__(
        self,
        batch_size: int,
        **kwargs,
    ):
        """."""
        self.batch_size = batch_size

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"BatchGeneratorOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="BatchGeneratorOp.run:"
        )

        # ref: https://stackoverflow.com/a/24527424
        def batcher(iterable, batch_size=10):
            iterator = iter(iterable)
            for first in iterator:
                yield itertools.chain(
                    [first], itertools.islice(iterator, batch_size - 1)
                )

        for batch_i, batch in enumerate(batcher(msg.data, batch_size=self.batch_size)):
            logger.info(
                f"BatchGeneratorOp: Yielding batch {batch_i + 1} (1-index) "
                f"of size {self.batch_size=}."
            )
            yield OpMsg(
                data=batch,
                metadata=BatchGeneratorOpMetadataModel(),
                audit=BatchGeneratorOpAuditModel(),
            )
