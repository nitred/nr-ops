import logging
from typing import Any, Dict, Generator, List, Optional

from pydantic import StrictStr, conlist

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.google_analytics import (
    GoogleAnalyticsConnOp,
)
from nr_ops.ops.ops.connector_ops.interfaces.google_analytics_ga4 import (
    GoogleAnalyticsGA4ConnOp,
)

logger = logging.getLogger(__name__)


# Reference: https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py
class ReportConfig(BaseOpConfigModel):
    report_name: StrictStr
    report_docs: Optional[StrictStr] = None

    # Keeping the type enforcements to a minimum since I don't fully understand the API
    property_id: StrictStr
    date_ranges: Optional[List[Dict[str, str]]] = None
    metrics: Optional[List[Dict[str, str]]] = None
    dimensions: Optional[List[Dict[str, str]]] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GetGAReportsGA4OpConfigModel(BaseOpConfigModel):
    google_analytics_conn_id: StrictStr
    report_configs: conlist(ReportConfig, min_items=1)

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GetGAReportsGA4OpMetadataModel(BaseOpMetadataModel):
    property_id: StrictStr
    report_name: StrictStr
    report_docs: Optional[StrictStr] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GetGAReportsGA4OpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GetGAReportsGA4Op(BaseGeneratorOp):
    OP_TYPE = "generator.get_ga_reports_ga4"
    OP_CONFIG_MODEL = GetGAReportsGA4OpConfigModel
    OP_METADATA_MODEL = GetGAReportsGA4OpMetadataModel
    OP_AUDIT_MODEL = GetGAReportsGA4OpAuditModel

    templated_fields = None

    def __init__(
        self,
        google_analytics_conn_id: str,
        report_configs: List[Dict[str, str]],
        **kwargs,
    ):
        """."""
        self.google_analytics_conn_id = google_analytics_conn_id
        self.report_configs = report_configs
        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.analytics: GoogleAnalyticsGA4ConnOp = op_manager.connector.get_connector(
            op_id=self.google_analytics_conn_id
        )

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"GetGAReportsGA4Op.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="GetGAReportsGA4Op.run:"
        )

        for report_config in self.report_configs:
            logger.info(
                f"GetGAReportsGA4Op.run: Fetching report: "
                f"{report_config['report_name']}"
            )
            report = self.analytics.get_report(**report_config)

            logger.info(
                f"GetGAReportsGA4Op.run: Fetching report: "
                f"{report_config['report_name']} Done! {type(report)=}"
            )

            yield OpMsg(
                data=report,
                metadata=GetGAReportsGA4OpMetadataModel(
                    property_id=report_config.get("property_id"),
                    report_name=report_config.get("report_name"),
                    report_docs=report_config.get("report_docs"),
                ),
                audit=GetGAReportsGA4OpAuditModel(),
            )
