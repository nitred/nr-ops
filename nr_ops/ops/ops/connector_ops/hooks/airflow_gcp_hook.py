"""."""
import logging

from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class AirflowGCPHookConnOpConfigModel(BaseOpConfigModel):
    airflow_conn_id: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowGCPHookConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowGCPHookConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class AirflowGCPHookConnOp(BaseConnectorOp):
    OP_TYPE = "connector.hooks.airflow_gcp_hook"
    OP_CONFIG_MODEL = AirflowGCPHookConnOpConfigModel
    OP_METADATA_MODEL = AirflowGCPHookConnOpMetadataModel
    OP_AUDIT_MODEL = AirflowGCPHookConnOpAuditModel

    templated_fields = None

    def __init__(self, airflow_conn_id: str, **kwargs):
        self.airflow_conn_id = airflow_conn_id
        self.templated_fields = kwargs.get("templated_fields", [])

        from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

        self.hook = GoogleBaseHook(gcp_conn_id=self.airflow_conn_id)

    def run(self) -> OpMsg:
        """.

        With the returned `self.analytics` object you can do:

        # ------------------------------------------------------------------------------
        # For Google Analytics
        # https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py
        # ------------------------------------------------------------------------------
        import google.auth.credentials
        from googleapiclient.discovery import build
        credentials: google.auth.credentials.Credentials = (
            google_base_hook.get_credentials()
        )
        self.analytics = build("analyticsreporting", "v4", credentials=credentials)
        return analytics.reports().batchGet(
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
        logger.info(f"AirflowGCPHookConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="AirflowGCPHookConnOp.run:"
        )

        return OpMsg(
            data=self.hook,
            metadata=AirflowGCPHookConnOpMetadataModel(),
            audit=AirflowGCPHookConnOpAuditModel(),
        )
