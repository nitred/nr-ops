import bz2
import gzip
import itertools
import json
import logging
import zlib
from typing import Any, Dict, Generator, List, Literal, Optional

import zstandard as zstd
from pydantic import StrictStr, StrictInt

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class DecompressNewlineSeparatedStreamsWithZlibOpConfigModel(BaseOpConfigModel):
    wbits: StrictInt

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class DecompressNewlineSeparatedStreamsWithZlibOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class DecompressNewlineSeparatedStreamsWithZlibOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class DecompressNewlineSeparatedStreamsWithZlibOp(BaseGeneratorOp):
    OP_TYPE = "generator.decompress.decompress_newline_separated_streams_with_zlib"
    OP_CONFIG_MODEL = DecompressNewlineSeparatedStreamsWithZlibOpConfigModel
    OP_METADATA_MODEL = DecompressNewlineSeparatedStreamsWithZlibOpMetadataModel
    OP_AUDIT_MODEL = DecompressNewlineSeparatedStreamsWithZlibOpAuditModel

    templated_fields = None

    def __init__(
        self,
        wbits: int,
        **kwargs,
    ):
        self.wbits = wbits
        self.templated_fields = kwargs.get("templated_fields", [])

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"DecompressNewlineSeparatedStreamsWithZlibOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="DecompressNewlineSeparatedStreamsWithZlibOp.run:",
        )

        current_data = msg.data

        for iter_i in itertools.count(0):
            try:
                z = zlib.decompressobj(wbits=47)
                output = z.decompress(current_data)
                unused_data = z.unused_data
                logger.info(
                    f"DecompressNewlineSeparatedStreamsWithZlibOp.run: "
                    f"Done decompressing {iter_i=}. "
                    f"{len(output)=:,} bytes. "
                    f"{len(unused_data)=:,} bytes. Yielding uncompressed output."
                )
                yield OpMsg(
                    data=output,
                    metadata=DecompressNewlineSeparatedStreamsWithZlibOpMetadataModel(),
                    audit=DecompressNewlineSeparatedStreamsWithZlibOpAuditModel(),
                )

            except Exception:
                logger.error(
                    f"DecompressNewlineSeparatedStreamsWithZlibOp.run: We do not expect "
                    f"any errors in this operation. Please investigate and make sure "
                    f"the input data provided fits the use case that this op tries to "
                    f"solve."
                )
                raise

            if unused_data.startswith(b"\n"):
                unused_data = unused_data[1:]

            if len(unused_data) == 0:
                break

            current_data = unused_data

        logger.info(
            f"DecompressNewlineSeparatedStreamsWithZlibOp.run: "
            f"No more unused_data to process. Returning. "
        )
