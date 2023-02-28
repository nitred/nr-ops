import logging
import os
from typing import Any, Dict, List, Optional

from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseOp
from nr_ops.ops.op import Op

logger = logging.getLogger(__name__)


class OpManager(object):
    def __init__(self):
        """."""
        # Currently it seems appropriate to store the environment variables in the
        # op_manager and not withing OpSubManager.
        self._env_vars = {}
        self.op_submanager = OpSubManager()
        self.connector_submanager = OpSubManager()
        self.generator_submanager = OpSubManager()

    def import_env_vars(self, env_vars: Optional[List[str]] = None):
        """Store environment variables from os.env."""
        logger.info(f"OpManager: Importing environment variables. {env_vars=}")
        if env_vars is None:
            return

        for env_var in env_vars:
            if env_var not in os.environ:
                raise ValueError(
                    f"{env_var=} not found in os.environ! Please check for typos in "
                    f"`import_env_vars` in the config file or make sure the "
                    f"environment variable is set."
                )
            self.env_vars[env_var] = os.getenv(env_var)

    @property
    def env_vars(self):
        return self._env_vars

    @property
    def op(self):
        return self.op_submanager

    @property
    def connector(self):
        return self.connector_submanager

    @property
    def generator(self):
        return self.generator_submanager


class OpSubManager(object):
    def __init__(self):
        self.op_store = {}
        self.data_store = {}
        self.metadata_store = {}

    def store_op(self, op: Op):
        if op.op_id is None:
            # NOT registering op with op_id=None
            return

        self.op_store[op.op_id] = op

    def store_data(self, op: Op, msg: OpMsg):
        if op.op_id is None:
            raise NotImplementedError()
        self.data_store[op.op_id] = msg.data

    def store_metadata(self, op: Op, msg: OpMsg):
        if op.op_id is None:
            # NOT registering op with op_id=None
            return
        self.metadata_store[op.op_id] = msg.metadata

    def store_audit(self, op: Op, msg: OpMsg):
        pass

    ####################################################################################
    # get_data
    ####################################################################################
    def get_data(self, op_id: str) -> Any:
        if op_id is None:
            raise NotImplementedError()
        return self.data_store[op_id]

    # Alias
    get_connector = get_data

    ####################################################################################
    # get_metadata
    ####################################################################################
    def get_metadata(self, op_id: str) -> BaseOpMetadataModel:
        if op_id is None:
            raise NotImplementedError()
        return self.metadata_store[op_id]

    ####################################################################################
    # get_op
    ####################################################################################
    def get_op(self, op_id: str) -> Op:
        if op_id is None:
            raise NotImplementedError()
        return self.op_store[op_id]


GLOBAL_OP_MANAGERS: Optional[OpManager] = None


def init_global_op_manager() -> OpManager:
    """."""
    global GLOBAL_OP_MANAGERS
    GLOBAL_OP_MANAGERS = OpManager()
    return GLOBAL_OP_MANAGERS


def get_global_op_manager() -> OpManager:
    """."""
    if GLOBAL_OP_MANAGERS is None:
        raise ValueError(
            f"GLOBAL_OP_MANAGERS has not been initialized. "
            f"Please call init_global_op_managers() first."
        )

    return GLOBAL_OP_MANAGERS
