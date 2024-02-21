import logging
from typing import List

from pydantic import StrictStr, conlist

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class GCPServiceAccountFromFileConnOpConfigModel(BaseOpConfigModel):
    key_file_path: StrictStr
    scopes: conlist(StrictStr, min_items=0)

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPServiceAccountFromFileConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPServiceAccountFromFileConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPServiceAccountFromFileConnOp(BaseConnectorOp):
    OP_TYPE = "connector.hooks.gcp_service_account_from_file"
    OP_CONFIG_MODEL = GCPServiceAccountFromFileConnOpConfigModel
    OP_METADATA_MODEL = GCPServiceAccountFromFileConnOpMetadataModel
    OP_AUDIT_MODEL = GCPServiceAccountFromFileConnOpAuditModel

    templated_fields = None

    def __init__(self, key_file_path: str, scopes: List[str], **kwargs):
        self.key_file_path = key_file_path
        self.scopes = scopes
        self.templated_fields = kwargs.get("templated_fields", [])

        # Will be populated by run()
        self.credentials = None

    def run(self) -> OpMsg:
        """.

        # ------------------------------------------------------------------------------
        # EXAMPLES - Google Analytics Reporting API v4
        # https://googleapis.github.io/google-api-python-client/docs/dyn/analyticsreporting_v4.html
        # ------------------------------------------------------------------------------
        self.api = build("analyticsreporting", "v4", credentials=credentials)
        return self.api.reports().batchGet(
          body={
            'reportRequests': [
            {
              'viewId': VIEW_ID,
              'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}],
              'metrics': [{'expression': 'ga:sessions'}],
              'dimensions': [{'name': 'ga:country'}]
            }]
          }
        ).execute()

        """
        logger.info(f"GCPServiceAccountFromFileConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="GCPServiceAccountFromFileConnOp.run:"
        )

        # Source: https://googleapis.github.io/google-api-python-client/docs/dyn/analyticsreporting_v4.html
        from oauth2client.service_account import ServiceAccountCredentials

        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.key_file_path, self.scopes
        )

        # https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py
        return OpMsg(
            data=self,
            metadata=GCPServiceAccountFromFileConnOpMetadataModel(),
            audit=GCPServiceAccountFromFileConnOpAuditModel(),
        )
