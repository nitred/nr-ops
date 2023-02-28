from typing import Any

from pydantic import BaseModel

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.time_step import TimeStep


class OpMsg(BaseModel):
    data: Any
    metadata: BaseOpMetadataModel
    audit: BaseOpAuditModel

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OpTimeStepMsg(OpMsg):
    data: TimeStep
