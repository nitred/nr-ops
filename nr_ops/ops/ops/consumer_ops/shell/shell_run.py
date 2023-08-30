import logging
import os
import subprocess
import time
from typing import List, Optional

from pydantic import StrictBool, StrictInt, StrictStr, conlist

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class ShellRunConsumerOpConfigModel(BaseOpConfigModel):
    cmd: StrictStr
    accepted_exit_codes: Optional[conlist(StrictInt, min_items=1)] = None
    log_cmd: Optional[StrictBool] = False
    log_stdout: Optional[StrictBool] = False

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class ShellRunConsumerOpMetadataModel(BaseOpMetadataModel):
    exit_code: int

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class ShellRunConsumerOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class ShellRunConsumerOp(BaseConsumerOp):
    OP_TYPE = "consumer.shell.run"
    OP_CONFIG_MODEL = ShellRunConsumerOpConfigModel
    OP_METADATA_MODEL = ShellRunConsumerOpMetadataModel
    OP_AUDIT_MODEL = ShellRunConsumerOpAuditModel

    templated_fields = None

    def __init__(
        self,
        cmd: str,
        accepted_exit_codes: Optional[List[int]] = None,
        log_cmd: bool = False,
        log_stdout: bool = False,
        **kwargs,
    ):
        self.cmd = cmd
        self.accepted_exit_codes = (
            [0] if accepted_exit_codes is None else accepted_exit_codes
        )
        self.log_cmd = log_cmd
        self.log_stdout = log_stdout

        self.templated_fields = kwargs.get("templated_fields", [])

    def run(self, time_step: TimeStep, msg: Optional[OpMsg] = None) -> OpMsg:
        """."""
        logger.info(f"ShellRunConsumerOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="ShellRunConsumerOp.run:"
        )

        if self.log_cmd:
            logger.info(f"ShellRunConsumerOp.run: Running command | {self.cmd=}")
        else:
            logger.info(f"ShellRunConsumerOp.run: Running command | not logging cmd")

        start = time.time()

        proc = subprocess.Popen(
            self.cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            env=os.environ.copy(),
        )

        if not self.log_stdout:
            logger.info(
                f"ShellRunConsumerOp.run: {self.log_stdout=}. Not logging stdout. " f""
            )

        # https://gist.github.com/JLeClerc/831d400763b7020599d9
        for line in iter(proc.stdout.readline, b""):
            if self.log_stdout:
                logger.info(
                    f"ShellRunConsumerOp.run: STDOUT | {line.decode('utf-8').rstrip()}"
                )
            else:
                logger.info(
                    f"ShellRunConsumerOp.run: {self.log_stdout=} | Still running ..."
                )

        exit_code = proc.wait()

        logger.info(
            f"ShellRunConsumerOp.run: Finished running command with {exit_code=}. "
            f"Time taken: {time.time() - start:0.2f} seconds."
        )

        if exit_code not in self.accepted_exit_codes:
            raise ValueError(
                f"ShellRunConsumerOp.run: Command exited with {exit_code=} which is "
                f"not in the list of {self.accepted_exit_codes=}."
            )

        return OpMsg(
            data=None,
            metadata=ShellRunConsumerOpMetadataModel(exit_code=exit_code),
            audit=ShellRunConsumerOpAuditModel(),
        )
