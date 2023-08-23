import json
import logging
import time
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple

import pandas as pd
from pydantic import StrictBool, StrictInt, StrictStr, conlist

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.http import HTTPConnOp

logger = logging.getLogger(__name__)


class MailchimpCampaignsGetLinksClickedMembersOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    campaign_id: StrictStr
    link_id: StrictStr
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None
    records_per_page: StrictInt
    iterate_over_pages: StrictBool
    sleep_time_between_pages: int = 5
    timeout_seconds_per_request: float = 60

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpCampaignsGetLinksClickedMembersOpMetadataModel(BaseOpMetadataModel):
    current_page: int
    total_pages: int
    total_records: int

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpCampaignsGetLinksClickedMembersOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpCampaignsGetLinksClickedMembersOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.mailchimp.campaigns.get_links_clicked_members"
    OP_CONFIG_MODEL = MailchimpCampaignsGetLinksClickedMembersOpConfigModel
    OP_METADATA_MODEL = MailchimpCampaignsGetLinksClickedMembersOpMetadataModel
    OP_AUDIT_MODEL = MailchimpCampaignsGetLinksClickedMembersOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        campaign_id: str,
        link_id: str,
        iterate_over_pages: bool,
        records_per_page: int,
        accepted_status_codes: Optional[List[int]] = None,
        sleep_time_between_pages: int = 5,
        timeout_seconds_per_request: float = 60,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.campaign_id = campaign_id
        self.link_id = link_id
        self.sleep_time_between_pages = sleep_time_between_pages
        self.accepted_status_codes = (
            accepted_status_codes if accepted_status_codes else [200]
        )
        self.records_per_page = records_per_page
        self.timeout_seconds_per_request = timeout_seconds_per_request
        self.iterate_over_pages = iterate_over_pages

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.get_connector(op_id=self.http_conn_id)

    def get_page(self, page: int) -> Tuple[int, Dict[str, Any], Dict[str, Any]]:
        """."""
        params = {}

        # IMPORTANT: The page number is 1-indexed and offset is 0-indexed.
        # Offset should be 0 if page = 1, 100 if page = 2 and records_per_page = 100
        offset = (page - 1) * self.records_per_page
        params["offset"] = offset
        params["count"] = self.records_per_page

        # DOCS: https://mailchimp.com/developer/marketing/api/link-clickers/list-clicked-link-subscribers/
        url = f"{self.http_conn.base_url}/reports/{self.campaign_id}/click-details/{self.link_id}/members"
        logger.info(
            f"MailchimpCampaignsGetLinksClickedMembersOp.get_page: "
            f"Fetching {page=} with {url=}"
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
        logger.info(f"MailchimpCampaignsGetLinksClickedMembersOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="MailchimpCampaignsGetLinksClickedMembersOp.run:",
        )

        final_records = []
        page, total_records, total_pages = 1, 0, None
        while True:
            logger.info(
                f"MailchimpCampaignsGetLinksClickedMembersOp.run: Fetching {page=}."
            )

            status_code, output_json, etl_metadata_json = self.get_page(page=page)
            if len(output_json["members"]) == 0:
                logger.info(
                    f"MailchimpCampaignsGetLinksClickedMembersOp.run: "
                    f"Received 0 members records. "
                    f"No data exists for current {page=}. Stopping the loop and no"
                    f"longer fetching additional pages."
                )
                total_pages = page - 1
                break

            n_records = len(output_json["members"])
            total_records += n_records

            logger.info(
                f"MailchimpCampaignsGetLinksClickedMembersOp.run: "
                f"Done fetching {page=}. Fetched {n_records=} this request. "
                f"Fetched {total_records=} records so far. "
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
                    "data": record,
                    "etl_metadata": etl_metadata_json,
                }
                for record in output_json["members"]
            ]

            final_records.extend(records)

            # Yield current page's results
            # If true, yield current records. Afterwards set final_records to empty list
            # If true, make sure to not yield records outside of loop.
            if self.iterate_over_pages:
                yield OpMsg(
                    data=final_records,
                    metadata=MailchimpCampaignsGetLinksClickedMembersOpMetadataModel(
                        current_page=page,
                        total_pages=-1,
                        total_records=total_records,
                    ),
                    audit=MailchimpCampaignsGetLinksClickedMembersOpAuditModel(),
                )
                final_records = []

            logger.info(
                f"MailchimpCampaignsGetLinksClickedMembersOp.run: Sleeping for "
                f"{self.sleep_time_between_pages} seconds between pages. "
                f"Next page is {page+1}."
            )
            time.sleep(self.sleep_time_between_pages)

            # IMPORTANT: The page number is 1-indexed.
            page += 1

        # Only yield records if iterate_over_pages is false.
        if self.iterate_over_pages:
            logger.info(
                f"MailchimpCampaignsGetLinksClickedMembersOp.run: "
                f"{self.iterate_over_pages=}. "
                f"Finished yielding all records, once per page."
            )

        else:
            logger.info(
                f"MailchimpCampaignsGetLinksClickedMembersOp.run: "
                f"{self.iterate_over_pages=}. "
                f"Yielding all pages at once now. {len(final_records)=}"
            )
            yield OpMsg(
                data=final_records,
                metadata=MailchimpCampaignsGetLinksClickedMembersOpMetadataModel(
                    current_page=page,
                    total_pages=total_pages,
                    total_records=total_records,
                ),
                audit=MailchimpCampaignsGetLinksClickedMembersOpAuditModel(),
            )
