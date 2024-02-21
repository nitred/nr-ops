""".
https://googleapis.github.io/google-api-python-client/docs/dyn/analyticsreporting_v4.html
"""
import logging
from typing import Any, Dict, List, Literal, Optional

from pydantic import StrictStr, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel
from nr_ops.ops.ops.connector_ops.interfaces.base import validate_hook_type_and_config

logger = logging.getLogger(__name__)


class GoogleAnalyticsConnOpConfigModel(BaseOpConfigModel):
    hook_type: Literal[
        "connector.hooks.airflow_gcp_hook",
        "connector.hooks.gcp_service_account_from_file",
    ]
    hook_config: Dict[StrictStr, Any]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    _validate_hook_type_and_config = root_validator(allow_reuse=True)(
        validate_hook_type_and_config
    )


class GoogleAnalyticsConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleAnalyticsConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleAnalyticsConnOp(BaseConnectorOp):
    OP_TYPE = "connector.google_analytics"
    OP_CONFIG_MODEL = GoogleAnalyticsConnOpConfigModel
    OP_METADATA_MODEL = GoogleAnalyticsConnOpMetadataModel
    OP_AUDIT_MODEL = GoogleAnalyticsConnOpAuditModel

    templated_fields = None

    def __init__(self, hook_type: str, hook_config: Dict[str, Any], **kwargs):
        self.hook_type = hook_type
        self.hook_config = hook_config
        self.templated_fields = kwargs.get("templated_fields", [])

        from nr_ops.ops.op_collection import OP_COLLECTION

        self.hook_op = OP_COLLECTION[self.hook_type](**self.hook_config)
        self.hook = self.hook_op.run().data

        # Will be populated in run()
        self.analytics: Any = None

    def get_report(
        self,
        view_id: str,
        date_ranges: Optional[List[Dict[str, str]]] = None,
        metrics: Optional[List[Dict[str, str]]] = None,
        dimensions: Optional[List[Dict[str, str]]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """."""
        if (
            self.hook_type == "connector.hooks.airflow_gcp_hook"
            or self.hook_type == "connector.hooks.gcp_service_account_from_file"
        ):
            payload = {"reportRequests": [{"viewId": view_id}]}
            if date_ranges:
                payload["reportRequests"][0]["dateRanges"] = date_ranges
            if metrics:
                payload["reportRequests"][0]["metrics"] = metrics
            if dimensions:
                payload["reportRequests"][0]["dimensions"] = dimensions

            results = self.analytics.reports().batchGet(body=payload).execute()

            if len(results["reports"]) > 1:
                raise NotImplementedError("Expected only 1 report.")

            results = results["reports"][0]
            data = [
                {
                    "metrics": {
                        metric["name"]: row["metrics"][i]["values"][0]
                        for i, metric in enumerate(
                            results["columnHeader"]["metricHeader"][
                                "metricHeaderEntries"
                            ]
                        )
                    },
                    "dimensions": {
                        dimension_name: row["dimensions"][i]
                        for i, dimension_name in enumerate(
                            results["columnHeader"]["dimensions"]
                        )
                    },
                }
                for row in results["data"]["rows"]
            ]
            output = {
                "ga_type": "universal",
                "request": {
                    "view_id": view_id,
                    "date_ranges": date_ranges,
                    "metrics": metrics,
                    "dimensions": dimensions,
                },
                "response": {
                    "view_id": view_id,
                    "date_ranges": date_ranges,
                    "data": data,
                },
            }
            return output
        else:
            raise NotImplementedError()

    def run(self) -> OpMsg:
        """."""
        logger.info(f"GoogleAnalyticsConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="GoogleAnalyticsConnOp.run:"
        )

        # Source: https://googleapis.github.io/google-api-python-client/docs/dyn/analyticsreporting_v4.html
        if self.hook_type == "connector.hooks.airflow_gcp_hook":
            # Type annotate for pycharm/mypy/autocomplete
            from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

            self.hook: GoogleBaseHook

            import google.auth.credentials
            from googleapiclient.discovery import build

            credentials: google.auth.credentials.Credentials = (
                self.hook._get_credentials()
            )
            self.analytics = build("analyticsreporting", "v4", credentials=credentials)
        elif self.hook_type == "connector.hooks.gcp_service_account_from_file":
            # Type annotate for pycharm/mypy/autocomplete
            from googleapiclient.discovery import build
            from oauth2client.service_account import ServiceAccountCredentials

            credentials: ServiceAccountCredentials = self.hook.credentials
            self.analytics = build("analyticsreporting", "v4", credentials=credentials)

        else:
            raise NotImplementedError()

        return OpMsg(
            data=self,
            metadata=GoogleAnalyticsConnOpMetadataModel(),
            audit=GoogleAnalyticsConnOpAuditModel(),
        )
