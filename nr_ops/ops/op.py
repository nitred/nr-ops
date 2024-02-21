import logging
import time
from collections import Counter
from typing import Any, Dict, Generator, Optional, Union

import psutil
from pydantic import BaseModel, StrictBool, StrictStr, root_validator

from nr_ops.messages.op_msg import OpMsg, OpTimeStepMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import (
    BaseConnectorOp,
    BaseConsumerOp,
    BaseGeneratorOp,
    BaseGroupOp,
    BaseTimeStepOp,
)

logger = logging.getLogger(__name__)


class OpModel(BaseModel):
    op_id: Optional[StrictStr] = None
    op_docs: Optional[StrictStr] = None
    op_type: StrictStr
    op_config: Dict[StrictStr, Any]
    store_metadata: StrictBool = True
    store_data: StrictBool = False

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    @root_validator(pre=False)
    def validate_op_type_and_config(cls, values):
        """Validate that the config matches the type."""
        op_type = values.get("op_type")
        op_config = values.get("op_config")

        from .op_collection import OP_COLLECTION

        if op_type not in OP_COLLECTION:
            raise ValueError(
                f"Operator with {op_type=} was NOT FOUND in OP_COLLECTION. "
                f"Please check for typos in the `op_type` or make sure the operator "
                f"class is correctly imported in OP_COLLECTION."
            )

        # Check if op_config is a valid config for op_type.
        op_model_cls = OP_COLLECTION[op_type].OP_CONFIG_MODEL
        # op_model is a pydantic model that is used to validate the op_config.
        op_model_cls(**op_config)

        return values

    def get_self_and_child_op_ids(self) -> Generator[str, None, None]:
        """.

        NOTE: This method is called once from main on only the root op.
        - The root op will call this method on all of its children.
        """
        if self.op_id is not None:
            yield self.op_id

        from .op_collection import OP_COLLECTION

        op_model_cls = OP_COLLECTION[self.op_type].OP_CONFIG_MODEL
        op_model = op_model_cls(**self.op_config)

        yield from op_model.get_self_and_child_op_ids()

    def validate_unique_ids(self):
        """.

        NOTE: This method is called once from main on only the root op.
        - The root op will call this method on all of its children.
        """

        all_ids = list(self.get_self_and_child_op_ids())
        counts = Counter(all_ids)
        duplicates = [op_id for op_id, count in counts.items() if count > 1]
        if duplicates:
            raise ValueError(
                f"Duplicate op_ids found: {duplicates}. op_ids must be unique across "
                f"all operators."
            )


class Op(object):
    def __init__(
        self,
        op_type: str,
        op_config: Dict[str, Any],
        op_id: Optional[str] = None,
        op_docs: Optional[str] = None,
        store_metadata: bool = True,
        store_data: bool = False,
    ):
        self.op_id = op_id
        self.op_docs = op_docs
        self.op_type = op_type
        self.op_config = op_config
        self.store_metadata = store_metadata
        self.store_data = store_data

        from .op_collection import OP_COLLECTION

        self.op_class = OP_COLLECTION[self.op_type]
        self.op_obj = self.op_class(**self.op_config)

        from nr_ops.ops.op_manager import get_global_op_manager

        self.op_manager = get_global_op_manager()

        # Register the op with the op_manager.
        # Only registers the op if op_id is not None.
        self.op_manager.store_op(op=self)

    def log_time_taken(self, checkpoint: float, _msg_i: int):

        now = time.time()
        memory_stats = psutil.virtual_memory()
        memory_used = memory_stats.used / 1024**2
        memory_used_percent = memory_stats.percent
        logger.info(
            f"Op.run: Time taken for "
            f"{self.op_obj.OP_FAMILY=} | {self.op_type=} | {self.op_id=} | "
            f"{_msg_i=} | {now - checkpoint:0.3f} seconds | "
            f"{memory_used=:,.2f} MB | {memory_used_percent=:.2f}%"
        )

    def run(
        self,
        time_step: Optional[TimeStep] = None,
        msg: Optional[OpMsg] = None,
    ) -> Generator[Union[OpMsg, OpTimeStepMsg], None, None]:
        """Run the operator."""
        logger.info("-" * 40)
        logger.info(
            f"Op.run: Running | "
            f"{self.op_obj.OP_FAMILY=} | {self.op_type=} | {self.op_id=}"
        )

        checkpoint = time.time()

        if self.op_obj.OP_FAMILY == "group":
            self.op_obj: BaseGroupOp
            for _msg_i, _msg in enumerate(
                self.op_obj.run(time_step=time_step, msg=msg)
            ):
                self.log_time_taken(checkpoint=checkpoint, _msg_i=_msg_i)
                if self.store_metadata:
                    self.op_manager.store_metadata(op=self, msg=_msg)
                if self.store_data:
                    self.op_manager.store_data(op=self, msg=_msg)
                yield _msg
                checkpoint = time.time()

        elif self.op_obj.OP_FAMILY == "time_step":
            self.op_obj: BaseTimeStepOp
            for _msg_i, _msg in enumerate(self.op_obj.run()):
                self.log_time_taken(checkpoint=checkpoint, _msg_i=_msg_i)
                if not isinstance(_msg.data, TimeStep):
                    raise ValueError(
                        f"Expected msg.data to be of class TimeStep for BaseScheduleOp "
                        f"instead received {type(_msg.data)=}."
                    )
                if self.store_metadata:
                    self.op_manager.store_metadata(op=self, msg=_msg)
                if self.store_data:
                    self.op_manager.store_data(op=self, msg=_msg)
                yield _msg
                checkpoint = time.time()

        elif self.op_obj.OP_FAMILY == "connector":
            self.op_obj: BaseConnectorOp
            _msg = self.op_obj.run()
            self.log_time_taken(checkpoint=checkpoint, _msg_i=0)
            # ALWAYS STORE DATA FOR CONNECTOR
            self.op_manager.store_data(op=self, msg=_msg)
            if self.store_metadata:
                self.op_manager.store_metadata(op=self, msg=_msg)
            yield _msg
            checkpoint = time.time()

        elif self.op_obj.OP_FAMILY == "generator":
            self.op_obj: BaseGeneratorOp
            for _msg_i, _msg in enumerate(
                self.op_obj.run(time_step=time_step, msg=msg)
            ):
                self.log_time_taken(checkpoint=checkpoint, _msg_i=_msg_i)
                # Register output metadata with the op_manager.
                if self.store_metadata:
                    self.op_manager.store_metadata(op=self, msg=_msg)
                if self.store_data:
                    self.op_manager.store_data(op=self, msg=_msg)
                yield _msg
                checkpoint = time.time()

        elif self.op_obj.OP_FAMILY == "consumer":
            self.op_obj: BaseConsumerOp
            _msg = self.op_obj.run(time_step=time_step, msg=msg)
            self.log_time_taken(checkpoint=checkpoint, _msg_i=0)
            if self.store_metadata:
                self.op_manager.store_metadata(op=self, msg=_msg)
            if self.store_data:
                self.op_manager.store_data(op=self, msg=_msg)

        else:
            raise NotImplementedError()
