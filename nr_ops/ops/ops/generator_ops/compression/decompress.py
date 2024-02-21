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


class DecompressOpConfigModel(BaseOpConfigModel):
    decompress_type: Literal["bz2", "gzip", "zlib", "zstd"]
    decompress_kwargs: Optional[Dict[StrictStr, Any]] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class DecompressOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class DecompressOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class DecompressOp(BaseGeneratorOp):
    OP_TYPE = "generator.decompress"
    OP_CONFIG_MODEL = DecompressOpConfigModel
    OP_METADATA_MODEL = DecompressOpMetadataModel
    OP_AUDIT_MODEL = DecompressOpAuditModel

    templated_fields = None

    def __init__(
        self,
        decompress_type: str,
        decompress_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self.decompress_type = decompress_type
        self.decompress_kwargs = {} if decompress_kwargs is None else decompress_kwargs
        self.templated_fields = kwargs.get("templated_fields", [])

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"DecompressOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=time_step, msg=msg, log_prefix="DecompressOp.run:")

        if self.decompress_type == "bz2":
            decompress = bz2.decompress
        elif self.decompress_type == "gzip":
            decompress = gzip.decompress
        elif self.decompress_type == "zlib":
            decompress = zlib.decompress
        elif self.decompress_type == "zstd":
            decompress = zstd.decompress
        else:
            raise NotImplementedError()

        if not isinstance(msg.data, bytes):
            raise ValueError(
                f"DecompressOp.run: msg.data must be bytes, not {type(msg.data)}"
            )

        output = decompress(msg.data, **self.decompress_kwargs)

        logger.info(
            f"DecompressOp.run: Decompressed {len(msg.data):,} bytes "
            f"to {len(output):,} bytes. "
            f"Decompression ratio: {len(output) / len(msg.data):.2f}"
        )

        yield OpMsg(
            data=output,
            metadata=DecompressOpMetadataModel(),
            audit=DecompressOpAuditModel(),
        )
