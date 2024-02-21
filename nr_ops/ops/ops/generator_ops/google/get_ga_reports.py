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

logger = logging.getLogger(__name__)


# Reference: https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py
class ReportConfig(BaseOpConfigModel):
    report_name: StrictStr
    report_docs: Optional[StrictStr] = None

    # Keeping the type enforcements to a minimum since I don't fully understand the API
    view_id: StrictStr
    date_ranges: Optional[List[Dict[str, str]]] = None
    metrics: Optional[List[Dict[str, str]]] = None
    dimensions: Optional[List[Dict[str, str]]] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GetGAReportsOpConfigModel(BaseOpConfigModel):
    google_analytics_conn_id: StrictStr
    report_configs: conlist(ReportConfig, min_items=1)

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GetGAReportsOpMetadataModel(BaseOpMetadataModel):
    view_id: StrictStr
    report_name: StrictStr
    report_docs: Optional[StrictStr] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GetGAReportsOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GetGAReportsOp(BaseGeneratorOp):
    OP_TYPE = "generator.get_ga_reports"
    OP_CONFIG_MODEL = GetGAReportsOpConfigModel
    OP_METADATA_MODEL = GetGAReportsOpMetadataModel
    OP_AUDIT_MODEL = GetGAReportsOpAuditModel

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

        self.analytics: GoogleAnalyticsConnOp = op_manager.get_connector(
            op_id=self.google_analytics_conn_id
        )

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"GetGAReportsOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="GetGAReportsOp.run:"
        )

        for report_config in self.report_configs:
            logger.info(
                f"GetGAReportsOp.run: Fetching report: "
                f"{report_config['report_name']}"
            )
            report = self.analytics.get_report(**report_config)

            logger.info(
                f"GetGAReportsOp.run: Fetching report: "
                f"{report_config['report_name']} Done! {type(report)=}"
            )

            yield OpMsg(
                data=report,
                metadata=GetGAReportsOpMetadataModel(
                    view_id=report_config.get("view_id"),
                    report_name=report_config.get("report_name"),
                    report_docs=report_config.get("report_docs"),
                ),
                audit=GetGAReportsOpAuditModel(),
            )
