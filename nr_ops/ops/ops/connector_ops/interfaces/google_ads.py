import logging
import os
from typing import Any

from google.ads.googleads.client import GoogleAdsClient
from pydantic import BaseModel, StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class GoogleAdsConnectorOpConfigModel(BaseOpConfigModel):
    api_version: StrictStr = "v13"

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    # _validate_hook_type_and_config = root_validator(allow_reuse=True)(
    #     validate_hook_type_and_config
    # )


class GoogleAdsConnectorOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleAdsConnectorOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleAdsConnectorOp(BaseConnectorOp):
    OP_TYPE = "connector.google_ads"
    OP_CONFIG_MODEL = GoogleAdsConnectorOpConfigModel
    OP_METADATA_MODEL = GoogleAdsConnectorOpMetadataModel
    OP_AUDIT_MODEL = GoogleAdsConnectorOpAuditModel

    templated_fields = None

    def __init__(self, api_version: str = "v13", **kwargs):
        self.api_version = api_version

        self.templated_fields = kwargs.get("templated_fields", [])

        self.client: Any = None

    def run(self) -> OpMsg:
        """."""
        logger.info(f"GoogleAdsConnectorOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="GoogleAdsConnectorOp.run:"
        )

        if "GOOGLE_ADS_CONFIGURATION_FILE_PATH" not in os.environ:
            raise ValueError(
                f"GOOGLE_ADS_CONFIGURATION_FILE_PATH not in os.environ. It must exist "
                f"in environment variables to use be able to use the Google Ads API."
            )

        self.client = GoogleAdsClient.load_from_storage(version=self.api_version)

        return OpMsg(
            data=self,
            metadata=GoogleAdsConnectorOpMetadataModel(),
            audit=GoogleAdsConnectorOpAuditModel(),
        )
