import logging
from typing import Any, Dict, List, Literal, Optional

from googleapiclient.discovery import build
from pydantic import StrictStr, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel
from nr_ops.ops.ops.connector_ops.interfaces.base import validate_hook_type_and_config

logger = logging.getLogger(__name__)


class GoogleSheetsConnOpConfigModel(BaseOpConfigModel):
    hook_type: Literal[
        "connector.hooks.gcp_service_account_from_file",
    ]
    hook_config: Dict[StrictStr, Any]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    _validate_hook_type_and_config = root_validator(allow_reuse=True)(
        validate_hook_type_and_config
    )


class GoogleSheetsConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleSheetsConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleSheetsConnOp(BaseConnectorOp):
    OP_TYPE = "connector.google_sheets"
    OP_CONFIG_MODEL = GoogleSheetsConnOpConfigModel
    OP_METADATA_MODEL = GoogleSheetsConnOpMetadataModel
    OP_AUDIT_MODEL = GoogleSheetsConnOpAuditModel

    templated_fields = None

    def __init__(self, hook_type: str, hook_config: Dict[str, Any], **kwargs):
        self.hook_type = hook_type
        self.hook_config = hook_config
        self.templated_fields = kwargs.get("templated_fields", [])

        from nr_ops.ops.op_collection import OP_COLLECTION

        self.hook_op = OP_COLLECTION[self.hook_type](**self.hook_config)
        self.hook = self.hook_op.run().data

        # Will be populated in run()
        self.sheet_service: Any = None

    def run(self) -> OpMsg:
        """."""
        logger.info(f"GoogleSheetsConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="GoogleSheetsConnOp.run:"
        )

        # Source: https://googleapis.github.io/google-api-python-client/docs/dyn/analyticsreporting_v4.html
        if self.hook_type == "connector.hooks.gcp_service_account_from_file":
            # Type annotate for pycharm/mypy/autocomplete

            from oauth2client.service_account import ServiceAccountCredentials

            from nr_ops.ops.ops.connector_ops.hooks.gcp_service_account_from_file import (
                GCPServiceAccountFromFileConnOp,
            )

            self.hook: GCPServiceAccountFromFileConnOp
            credentials: ServiceAccountCredentials = self.hook.credentials

            service = build("sheets", "v4", credentials=credentials)

            # Init the sheet API
            self.sheet_service = service.spreadsheets()

        else:
            raise NotImplementedError()

        return OpMsg(
            data=self,
            metadata=GoogleSheetsConnOpMetadataModel(),
            audit=GoogleSheetsConnOpAuditModel(),
        )
