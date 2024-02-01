import bz2
import gzip
import logging
import zlib
from typing import Any, Dict, Generator, List, Literal, Optional

import zstandard as zstd
from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class CompressOpConfigModel(BaseOpConfigModel):
    compress_type: Literal["bz2", "gzip", "zlib", "zstd"]
    compress_kwargs: Optional[Dict[StrictStr, Any]] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class CompressOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class CompressOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class CompressOp(BaseGeneratorOp):
    OP_TYPE = "generator.compress"
    OP_CONFIG_MODEL = CompressOpConfigModel
    OP_METADATA_MODEL = CompressOpMetadataModel
    OP_AUDIT_MODEL = CompressOpAuditModel

    templated_fields = None

    def __init__(
        self,
        compress_type: str,
        compress_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self.compress_type = compress_type
        self.compress_kwargs = {} if compress_kwargs is None else compress_kwargs
        self.templated_fields = kwargs.get("templated_fields", [])

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"CompressOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=time_step, msg=msg, log_prefix="CompressOp.run:")

        if self.compress_type == "bz2":
            compress = bz2.compress
        elif self.compress_type == "gzip":
            compress = gzip.compress
        elif self.compress_type == "zlib":
            compress = zlib.compress
        elif self.compress_type == "zstd":
            compress = zstd.compress
        else:
            raise NotImplementedError()

        if not isinstance(msg.data, bytes):
            raise ValueError(
                f"CompressOp.run: msg.data must be bytes, not {type(msg.data)}"
            )

        output = compress(msg.data, **self.compress_kwargs)

        logger.info(
            f"CompressOp.run: Compressed {len(msg.data):,} bytes "
            f"to {len(output):,} bytes. "
            f"Compression ratio: {len(output) / len(msg.data):.2f}"
        )

        yield OpMsg(
            data=output,
            metadata=CompressOpMetadataModel(),
            audit=CompressOpAuditModel(),
        )
