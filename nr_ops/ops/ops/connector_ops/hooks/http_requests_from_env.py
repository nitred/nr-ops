"""."""
import json
import logging
import os
from typing import Literal, Optional

from pydantic import BaseModel, StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class HTTPRequestsHookFromEnvConnOpConfigModel(BaseOpConfigModel):
    env_var: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class HTTPRequestsHookFromEnvConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class HTTPRequestsHookFromEnvConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class HTTPConnEnvModel(BaseModel):
    conn_type: Literal["http"]
    host: StrictStr
    login: Optional[StrictStr] = None
    password: Optional[StrictStr] = None

    class Config:
        extra = "allow"
        arbitrary_types_allowed = False


class HTTPRequestsHookFromEnvConnOp(BaseConnectorOp):
    OP_TYPE = "connector.hooks.http_requests_from_env"
    OP_CONFIG_MODEL = HTTPRequestsHookFromEnvConnOpConfigModel
    OP_METADATA_MODEL = HTTPRequestsHookFromEnvConnOpMetadataModel
    OP_AUDIT_MODEL = HTTPRequestsHookFromEnvConnOpAuditModel

    templated_fields = None

    def __init__(self, env_var: str, **kwargs):
        self.env_var = env_var
        self.templated_fields = kwargs.get("templated_fields", [])

        # Loaded in run
        self.conn_model: Optional[HTTPConnEnvModel] = None

    def run(self) -> OpMsg:
        """."""
        logger.info(f"HTTPRequestsHookFromEnvConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="HTTPRequestsHookFromEnvConnOp.run:"
        )

        if self.env_var not in os.environ:
            raise ValueError(
                f"HTTPRequestsHookFromEnvConnOp.run: Environment variable "
                f"{self.env_var} not found!"
            )

        conn_json = json.loads(os.environ[self.env_var])
        self.conn_model = HTTPConnEnvModel(**conn_json)

        return OpMsg(
            data=self,
            metadata=HTTPRequestsHookFromEnvConnOpMetadataModel(),
            audit=HTTPRequestsHookFromEnvConnOpAuditModel(),
        )

    ####################################################################################
    # PROPERTIES
    ####################################################################################
    @property
    def base_url(self) -> str:
        """."""
        base_url = self.conn_model.host
        if base_url.endswith("/"):
            base_url = self.conn_model.host[:-1]
        return base_url

    @property
    def username(self) -> Optional[str]:
        """."""
        return self.conn_model.login

    @property
    def password(self) -> Optional[str]:
        """."""
        return self.conn_model.password
