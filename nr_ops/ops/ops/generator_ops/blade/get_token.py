import logging
import re
from typing import Any, Dict, Generator, List, Literal, Optional

import pandas as pd
from pydantic import BaseModel, StrictStr, conlist, validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.google_analytics import (
    GoogleAnalyticsConnOp,
)
from nr_ops.ops.ops.connector_ops.interfaces.http import HTTPConnOp

logger = logging.getLogger(__name__)


class BladeGetTokenOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeGetTokenOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeGetTokenOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeGetTokenResponseDataModel(BaseModel):
    session_token: StrictStr
    expiry: StrictStr


class BladeGetTokenResponseModel(BaseModel):
    meta: Dict[str, Any]
    data: BladeGetTokenResponseDataModel

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeGetTokenOp(BaseGeneratorOp):
    OP_TYPE = "generator.blade.get_token"
    OP_CONFIG_MODEL = BladeGetTokenOpConfigModel
    OP_METADATA_MODEL = BladeGetTokenOpMetadataModel
    OP_AUDIT_MODEL = BladeGetTokenOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        accepted_status_codes: Optional[List[int]] = None,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.accepted_status_codes = accepted_status_codes

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.connector.get_connector(
            op_id=self.http_conn_id
        )

        # Loaded in run
        self.auth_model: Optional[BladeGetTokenResponseModel] = None

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"BladeGetTokenOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="BladeGetTokenOp.run:"
        )

        logger.info(
            f"BladeGetTokenOp.run: Getting auth token from {self.http_conn_id=}"
        )

        # Call the refresh token the first time, so that the self.auth_model is set
        # You can then call self.token to get the token.
        self.refresh_token()

        logger.info(
            f"BladeGetTokenOp.run: Successfully fetched auth_token! The attribute "
            f"`self.auth_model` has been set for {self.http_conn_id=}. "
            f"The (refreshed) token is now accessible by calling "
            f"self.token OR op.token OR msg.data.token"
        )

        yield OpMsg(
            data=self,
            metadata=BladeGetTokenOpMetadataModel(),
            audit=BladeGetTokenOpAuditModel(),
        )

    ####################################################################################
    # PROPERTIES & METHODS
    ####################################################################################
    def refresh_token(self):
        """."""
        status_code, auth_json = self.http_conn.call(
            method="post",
            url=f"{self.http_conn.base_url}/v1/auth/login",
            requests_kwargs={
                "headers": {"Content-Type": "application/json"},
                "json": {
                    "username": self.http_conn.username,
                    "password": self.http_conn.password,
                },
            },
            accepted_status_codes=self.accepted_status_codes,
            return_type="json",
        )

        self.auth_model = BladeGetTokenResponseModel(**auth_json)

    @property
    def token(self) -> str:
        """."""
        if self.auth_model is None:
            raise ValueError(
                "BladeGetTokenOp.token: self.auth_model is not set, "
                "please call op.run() first!"
            )

        expiry = pd.Timestamp(self.auth_model.data.expiry, tz="UTC")
        now = pd.Timestamp.now(tz="UTC")
        now_plus_315 = now + pd.Timedelta(seconds=315)

        if expiry < now_plus_315:
            logger.info(
                f"BladeGetTokenOp.token: Token has either expired or will expire in "
                f"315 seconds. Refreshing token. {expiry=}, {now=}"
            )
            self.refresh_token()
        else:
            logger.info(
                f"BladeGetTokenOp.token: Token has not yet expired. It will expire in "
                f"{(expiry - now).seconds} seconds. {expiry=}, {now=}"
            )

        return self.auth_model.data.session_token
