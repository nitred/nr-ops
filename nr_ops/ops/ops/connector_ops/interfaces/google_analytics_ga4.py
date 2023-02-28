""".
https://googleapis.github.io/google-api-python-client/docs/dyn/analyticsreporting_v4.html
"""
import logging
from typing import Any, Dict, Generator, List, Literal, Optional

import google
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    DimensionHeader,
    Metric,
    RunReportRequest,
    RunReportResponse,
)
from pydantic import BaseModel, StrictStr, conlist, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel
from nr_ops.ops.ops.connector_ops.interfaces.base import validate_hook_type_and_config

logger = logging.getLogger(__name__)


class GoogleAnalyticsGA4ConnOpConfigModel(BaseOpConfigModel):
    hook_type: Literal[
        "connector.hooks.airflow_gcp_hook",
        "connector.hooks.gcp_service_account_from_file",
        "connector.hooks.gcp_service_account_from_env",
    ]
    hook_config: Dict[StrictStr, Any]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    _validate_hook_type_and_config = root_validator(allow_reuse=True)(
        validate_hook_type_and_config
    )


class GoogleAnalyticsGA4ConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleAnalyticsGA4ConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleAnalyticsGA4ConnOp(BaseConnectorOp):
    OP_TYPE = "connector.google_analytics_ga4"
    OP_CONFIG_MODEL = GoogleAnalyticsGA4ConnOpConfigModel
    OP_METADATA_MODEL = GoogleAnalyticsGA4ConnOpMetadataModel
    OP_AUDIT_MODEL = GoogleAnalyticsGA4ConnOpAuditModel

    templated_fields = None

    def __init__(self, hook_type: str, hook_config: Dict[str, Any], **kwargs):
        self.hook_type = hook_type
        self.hook_config = hook_config
        self.templated_fields = kwargs.get("templated_fields", [])

        from nr_ops.ops.op_collection import OP_COLLECTION

        self.hook_op = OP_COLLECTION[self.hook_type](**self.hook_config)
        self.hook = self.hook_op.run().data

        # Will be poplated in run()
        self.analytics: Any = None

    def get_report(
        self,
        property_id: str,
        date_ranges: Optional[List[Dict[str, str]]] = None,
        metrics: Optional[List[Dict[str, str]]] = None,
        dimensions: Optional[List[Dict[str, str]]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """."""
        metric_names = []
        dimension_names = []
        if (
            self.hook_type == "connector.hooks.airflow_gcp_hook"
            or self.hook_type == "connector.hooks.gcp_service_account_from_file"
            or self.hook_type == "connector.hooks.gcp_service_account_from_env"
        ):
            payload = {"property": f"properties/{property_id}"}
            if date_ranges:
                payload["date_ranges"] = [
                    DateRange(**date_range) for date_range in date_ranges
                ]
            if metrics:
                payload["metrics"] = []
                for metric in metrics:
                    payload["metrics"].append(Metric(**metric))
                    metric_names.append(metric["name"])
            if dimensions:
                payload["dimensions"] = []
                for dimension in dimensions:
                    payload["dimensions"].append(Dimension(**dimension))
                    dimension_names.append(dimension["name"])

            from google.analytics.data_v1beta import BetaAnalyticsDataClient

            self.analytics: BetaAnalyticsDataClient
            request = RunReportRequest(**payload)
            results: RunReportResponse = self.analytics.run_report(request)
            data = [
                {
                    "metrics": {
                        metric_header.name: row.metric_values[i].value
                        for i, metric_header in enumerate(results.metric_headers)
                    },
                    "dimensions": {
                        dimension_header.name: row.dimension_values[i].value
                        for i, dimension_header in enumerate(results.dimension_headers)
                    },
                }
                for row in results.rows
            ]
            output = {
                "ga_type": "ga4",
                "request": {
                    "property_id": property_id,
                    "date_ranges": date_ranges,
                    "metrics": metrics,
                    "dimensions": dimensions,
                },
                "response": {
                    "property_id": property_id,
                    "date_ranges": date_ranges,
                    "data": data,
                },
            }
            return output
        else:
            raise NotImplementedError()

    def run(self) -> OpMsg:
        """."""
        logger.info(f"GoogleAnalyticsGA4ConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="GoogleAnalyticsGA4ConnOp.run:"
        )

        if self.hook_type == "connector.hooks.airflow_gcp_hook":
            # Type annotate for pycharm/mypy/autocomplete
            from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

            self.hook: GoogleBaseHook

            import google.auth.credentials
            from google.analytics.data_v1beta import BetaAnalyticsDataClient

            credentials: google.auth.credentials.Credentials = (
                self.hook._get_credentials()
            )
            self.analytics = BetaAnalyticsDataClient(credentials=credentials)
        # Source: https://googleapis.github.io/google-api-python-client/docs/dyn/analyticsreporting_v4.html
        elif self.hook_type == "connector.hooks.gcp_service_account_from_file":
            # Type annotate for pycharm/mypy/autocomplete
            from google.analytics.data_v1beta import BetaAnalyticsDataClient
            from google.api_core.client_options import ClientOptions, from_dict

            from nr_ops.ops.ops.connector_ops.hooks.gcp_service_account_from_file import (
                GCPServiceAccountFromFileConnOp,
            )

            self.hook: GCPServiceAccountFromFileConnOp
            client_options = ClientOptions(credentials_file=self.hook.key_file_path)
            self.analytics = BetaAnalyticsDataClient(client_options=client_options)

        elif self.hook_type == "connector.hooks.gcp_service_account_from_env":
            from google.analytics.data_v1beta import BetaAnalyticsDataClient
            from google.api_core.client_options import ClientOptions, from_dict
            from oauth2client.service_account import ServiceAccountCredentials

            credentials: ServiceAccountCredentials = self.hook.credentials

            self.analytics = BetaAnalyticsDataClient(credentials=credentials)

        else:
            raise NotImplementedError()

        return OpMsg(
            data=self,
            metadata=GoogleAnalyticsGA4ConnOpMetadataModel(),
            audit=GoogleAnalyticsGA4ConnOpAuditModel(),
        )
