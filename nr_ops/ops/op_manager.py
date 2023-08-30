import logging
import os
from typing import Any, Dict, List, Optional

from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp
from nr_ops.ops.op import Op

logger = logging.getLogger(__name__)


class OpManager(object):
    def __init__(self):
        """."""
        # Currently it seems appropriate to store the environment variables in the
        # op_manager and not withing OpSubManager.
        self._env_vars = {}
        self.__ops = {}
        self.__metadata = {}
        self.__data = {}

    def set_env_vars(self, env_vars: Optional[Dict[str, str]] = None):
        """Store environment variables from os.env."""
        env_vars = {} if env_vars is None else env_vars

        if not env_vars:
            return

        env_vars_keys = list(env_vars.keys())
        logger.info(f"OpManager: Settings environment variables. {env_vars_keys=}")

        for env_var_key, env_var_value in env_vars.items():

            if not isinstance(env_var_value, str):
                raise TypeError(
                    f"set_env_vars: The environment variable value for the "
                    f"{env_var_key=} is not a string. Please check the config file "
                    f"and make sure that ALL environment variable values are strings."
                )

            # Set the environment variable both in os.environ and self.env_vars
            os.environ[env_var_key] = env_var_value
            self.env_vars[env_var_key] = env_var_value

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
    def metadata(self):
        return self.__metadata

    @property
    def data(self):
        return self.__data

    @property
    def ops(self):
        return self.__ops

    def store_metadata(self, op: Op, msg: OpMsg):
        if op.op_id is None:
            # NOT registering op with op_id=None
            return
        self.__metadata[op.op_id] = msg.metadata

    def store_data(self, op: Op, msg: OpMsg):
        if op.op_id is None:
            # NOT registering op with op_id=None
            return
        self.__data[op.op_id] = msg.data

    def store_op(self, op: Op):
        if op.op_id is None:
            # NOT registering op with op_id=None
            return
        self.__ops[op.op_id] = op

    def get_data(self, op_id: str) -> Any:
        return self.__data[op_id]

    def get_metadata(self, op_id: str) -> Any:
        return self.__metadata[op_id]

    # Alias
    def get_connector(self, op_id: str) -> BaseConnectorOp:
        conn_op = self.__data[op_id]
        if not isinstance(conn_op, BaseConnectorOp):
            raise ValueError(
                f"Expected {op_id=} to be of type BaseConnectorOp instead "
                f"received {type(conn_op)=}."
            )
        return self.__data[op_id]


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
