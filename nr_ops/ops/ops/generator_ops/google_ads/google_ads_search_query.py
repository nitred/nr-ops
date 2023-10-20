import json
import logging
from typing import Generator, Optional

import pandas as pd
from google.protobuf import json_format
from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.google_ads import GoogleAdsConnectorOp

logger = logging.getLogger(__name__)


class GoogleAdsSearchQueryGeneratorOpConfigModel(BaseOpConfigModel):
    google_ads_conn_id: StrictStr
    customer_id: StrictStr
    query: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleAdsSearchQueryGeneratorOpMetadataModel(BaseOpMetadataModel):
    customer_id: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleAdsSearchQueryGeneratorOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleAdsSearchQueryGeneratorOp(BaseGeneratorOp):
    OP_TYPE = "generator.google_ads.search_query"
    OP_CONFIG_MODEL = GoogleAdsSearchQueryGeneratorOpConfigModel
    OP_METADATA_MODEL = GoogleAdsSearchQueryGeneratorOpMetadataModel
    OP_AUDIT_MODEL = GoogleAdsSearchQueryGeneratorOpAuditModel

    templated_fields = None

    def __init__(
        self,
        google_ads_conn_id: str,
        customer_id: str,
        query: str,
        **kwargs,
    ):
        self.google_ads_conn_id = google_ads_conn_id
        self.customer_id = customer_id
        self.query = query

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.google_ads_conn: GoogleAdsConnectorOp = op_manager.get_connector(
            op_id=self.google_ads_conn_id
        )
        self.client = self.google_ads_conn.client
        self.google_ads_service = self.client.get_service("GoogleAdsService")

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"GoogleAdsSearchQueryGeneratorOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="GoogleAdsSearchQueryGeneratorOp.run:",
        )

        etl_request_start_ts = pd.Timestamp.now(tz="UTC").isoformat()

        response = self.google_ads_service.search(
            customer_id=self.customer_id, query=self.query
        )

        etl_response_end_ts = pd.Timestamp.now(tz="UTC").isoformat()

        # Reference: https://stackoverflow.com/questions/55458027/convert-google-ads-api-googleadsrow-to-json
        final_records = [
            {
                "data": json.loads(json_format.MessageToJson(row._pb)),
                "etl_metadata": {
                    "time_step": time_step.to_json_dict(),
                    "etl_request_start_ts": etl_request_start_ts,
                    "etl_response_end_ts": etl_response_end_ts,
                    "query": self.query,
                    "customer_id": self.customer_id,
                },
            }
            for row in response
        ]

        logger.info(
            f"GoogleAdsSearchQueryGeneratorOp.run: Done running. {len(final_records)=}"
        )

        yield OpMsg(
            data=final_records,
            metadata=GoogleAdsSearchQueryGeneratorOpMetadataModel(
                customer_id=self.customer_id
            ),
            audit=GoogleAdsSearchQueryGeneratorOpAuditModel(),
        )
