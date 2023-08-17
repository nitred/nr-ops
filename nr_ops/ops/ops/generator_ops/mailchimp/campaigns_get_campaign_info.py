import json
import logging
import time
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple, Union

import pandas as pd
from pydantic import StrictBool, StrictInt, StrictStr, conlist, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.http import HTTPConnOp

logger = logging.getLogger(__name__)


class MailchimpCampaignsGetCampaignInfoOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None
    timeout_seconds_per_request: float = 60
    campaign_id: Optional[StrictStr] = None
    remove_links: StrictBool = True

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpCampaignsGetCampaignInfoOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpCampaignsGetCampaignInfoOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpCampaignsGetCampaignInfoOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.mailchimp.campaigns.get_campaign_info"
    OP_CONFIG_MODEL = MailchimpCampaignsGetCampaignInfoOpConfigModel
    OP_METADATA_MODEL = MailchimpCampaignsGetCampaignInfoOpMetadataModel
    OP_AUDIT_MODEL = MailchimpCampaignsGetCampaignInfoOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        campaign_id: Optional[str] = None,
        accepted_status_codes: Optional[List[int]] = None,
        timeout_seconds_per_request: float = 60,
        remove_links: bool = True,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.campaign_id = campaign_id
        self.accepted_status_codes = (
            accepted_status_codes if accepted_status_codes else [200]
        )
        self.timeout_seconds_per_request = timeout_seconds_per_request
        self.remove_links = remove_links

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.get_connector(op_id=self.http_conn_id)

    def get_page(self, campaign_id: str) -> Tuple[int, Dict[str, Any], Dict[str, Any]]:
        """."""
        params = {}

        # DOCS: https://mailchimp.com/developer/marketing/api/campaigns/get-campaign-info/
        url = f"{self.http_conn.base_url}/campaigns/{campaign_id}"
        logger.info(
            f"MailchimpCampaignsGetCampaignInfoOp.get_page: Fetching campaign info for "
            f"{campaign_id=} with {url=}"
        )

        etl_request_start_ts = str(pd.Timestamp.now(tz="UTC"))
        status_code, output_json = self.http_conn.call(
            method="get",
            url=url,
            requests_kwargs={
                "params": params,
                "timeout": self.timeout_seconds_per_request,
            },
            # NOTE: We expect status code to be 200 with return type JSON
            # even if there are no records.
            accepted_status_codes=self.accepted_status_codes,
            return_type="json",
        )
        etl_response_end_ts = str(pd.Timestamp.now(tz="UTC"))

        etl_metadata_json = {
            "etl_request_start_ts": etl_request_start_ts,
            "etl_response_end_ts": etl_response_end_ts,
        }

        return status_code, output_json, etl_metadata_json

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"MailchimpCampaignsGetCampaignInfoOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="MailchimpCampaignsGetCampaignInfoOp.run:",
        )

        if self.campaign_id:
            campaign_ids = [self.campaign_id]
        else:
            raise NotImplementedError()

        final_records = []
        for campaign_id in campaign_ids:
            logger.info(
                f"MailchimpCampaignsGetCampaignInfoOp.run: Fetching campaign info for "
                f"{campaign_id=}."
            )

            status_code, output_json, etl_metadata_json = self.get_page(
                campaign_id=campaign_id
            )

            logger.info(
                f"MailchimpCampaignsGetCampaignInfoOp.run: Done fetching campaign "
                f"info for {campaign_id=}."
            )

            ############################################################################
            # NOTE: Adding `etl_metadata` into response json (output_json).
            # * This is to add some metadata about the ETL process, primarily some
            #   timestamps which may help with creating event stores/logs.
            # * This may also help with incremental modelling.
            # * DO NOT USE this information when calculating dedup_uuid.
            ############################################################################
            etl_metadata_json["time_step"] = time_step.to_json_dict()
            records = [
                {
                    "data": output_json,
                    "etl_metadata": etl_metadata_json,
                }
            ]

            # Remove links from records
            # NOTE: Links can cause issues with deduplication.
            if self.remove_links:
                for record in records:
                    data = record["data"]
                    if "archive_url" in data:
                        data["archive_url"] = "REDACTED_BY_ETL_BEFORE_STORAGE"
                    if "long_archive_url" in data:
                        data["long_archive_url"] = "REDACTED_BY_ETL_BEFORE_STORAGE"
                    if "_links" in data:
                        data["_links"] = "REDACTED_BY_ETL_BEFORE_STORAGE"

            final_records.extend(records)

        yield OpMsg(
            data=final_records,
            metadata=MailchimpCampaignsGetCampaignInfoOpMetadataModel(),
            audit=MailchimpCampaignsGetCampaignInfoOpAuditModel(),
        )
