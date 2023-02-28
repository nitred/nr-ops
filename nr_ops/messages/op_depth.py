from __future__ import annotations

import logging

from pydantic import BaseModel, StrictStr

logger = logging.getLogger(__name__)


class BaseOpDepthModel(BaseModel):
    op_type_depth: StrictStr
    op_id_depth: StrictStr

    def init_new_depth(self, op_id: str, op_type: str) -> BaseOpDepthModel:
        """Add op id and type to an existing depth."""
        new_type_depth = f"{self.op_type_depth} -> {op_type}"
        new_id_depth = f"{self.op_id_depth} -> {op_id}"
        logger.info(f"{'-'* 80}")
        logger.info(f"Op.run: op_type_depth: {new_type_depth}")
        logger.info(f"Op.run: op_id_depth  : {new_id_depth}")
        return BaseOpDepthModel(op_type_depth=new_type_depth, op_id_depth=new_id_depth)
