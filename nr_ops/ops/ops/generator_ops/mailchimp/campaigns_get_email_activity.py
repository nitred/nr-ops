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


class MailchimpCampaignsGetEmailActivityOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    campaign_id: StrictStr
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None
    records_per_page: StrictInt
    iterate_over_pages: StrictBool
    sleep_time_between_pages: int = 5
    timeout_seconds_per_request: float = 60
    min_date_created: Optional[StrictStr] = None
    min_date_created_1us_offset: StrictBool = True

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpCampaignsGetEmailActivityOpMetadataModel(BaseOpMetadataModel):
    current_page: int
    total_pages: int
    total_records: int

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpCampaignsGetEmailActivityOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpCampaignsGetEmailActivityOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.mailchimp.campaigns.get_email_activity"
    OP_CONFIG_MODEL = MailchimpCampaignsGetEmailActivityOpConfigModel
    OP_METADATA_MODEL = MailchimpCampaignsGetEmailActivityOpMetadataModel
    OP_AUDIT_MODEL = MailchimpCampaignsGetEmailActivityOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        campaign_id: str,
        iterate_over_pages: bool,
        records_per_page: int,
        accepted_status_codes: Optional[List[int]] = None,
        sleep_time_between_pages: int = 5,
        timeout_seconds_per_request: float = 60,
        min_date_created: Optional[str] = None,
        min_date_created_1us_offset: bool = True,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.campaign_id = campaign_id
        self.sleep_time_between_pages = sleep_time_between_pages
        self.accepted_status_codes = (
            accepted_status_codes if accepted_status_codes else [200]
        )
        self.records_per_page = records_per_page
        self.timeout_seconds_per_request = timeout_seconds_per_request
        self.iterate_over_pages = iterate_over_pages
        self.min_date_created = min_date_created
        self.min_date_created_1us_offset = min_date_created_1us_offset

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

        if self.min_date_created is not None:
            if self.min_date_created_1us_offset:
                # NOTE: We subtract 1us from min_date_created to ensure that we
                # fetch records that were created at the same time as min_date_created.
                # The `since` parameter is non-inclusive and non-intuitive.
                min_date_created = pd.Timestamp(
                    self.min_date_created, tz="UTC"
                ) - pd.Timedelta("1us")
                min_date_created = min_date_created.isoformat()
                logger.info(
                    f"MailchimpCampaignsGetEmailActivityOp.get_page: "
                    f"Subtracting 1ms from {self.min_date_created=} to get "
                    f"{min_date_created=}. The `since` parameter is non-inclusive "
                    f"and subtracting 1us will make {self.min_date_created=} inclusive."
                )
            else:
                min_date_created = self.min_date_created
            params["since"] = min_date_created

        # DOCS: https://mailchimp.com/developer/marketing/api/email-activity-reports/
        url = f"{self.http_conn.base_url}/reports/{self.campaign_id}/email-activity"
        logger.info(
            f"MailchimpCampaignsGetEmailActivityOp.get_page: "
            f"Fetching {page=} with {url=}"
        )

        etl_request_start_ts = pd.Timestamp.now(tz="UTC").isoformat()
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
        etl_response_end_ts = pd.Timestamp.now(tz="UTC").isoformat()

        etl_metadata_json = {
            "etl_request_start_ts": etl_request_start_ts,
            "etl_response_end_ts": etl_response_end_ts,
        }

        return status_code, output_json, etl_metadata_json

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"MailchimpCampaignsGetEmailActivityOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="MailchimpCampaignsGetEmailActivityOp.run:",
        )

        final_records = []
        page, total_records, total_pages = 1, 0, None
        while True:
            logger.info(f"MailchimpCampaignsGetEmailActivityOp.run: Fetching {page=}.")

            status_code, output_json, etl_metadata_json = self.get_page(page=page)
            if len(output_json["emails"]) == 0:
                logger.info(
                    f"MailchimpCampaignsGetEmailActivityOp.run: "
                    f"Received 0 members records. "
                    f"No data exists for current {page=}. Stopping the loop and no"
                    f"longer fetching additional pages."
                )
                total_pages = page - 1
                break

            n_records = len(output_json["emails"])
            total_records += n_records

            logger.info(
                f"MailchimpCampaignsGetEmailActivityOp.run: "
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
                for record in output_json["emails"]
            ]

            final_records.extend(records)

            # Yield current page's results
            # If true, yield current records. Afterwards set final_records to empty list
            # If true, make sure to not yield records outside of loop.
            if self.iterate_over_pages:
                yield OpMsg(
                    data=final_records,
                    metadata=MailchimpCampaignsGetEmailActivityOpMetadataModel(
                        current_page=page,
                        total_pages=-1,
                        total_records=total_records,
                    ),
                    audit=MailchimpCampaignsGetEmailActivityOpAuditModel(),
                )
                final_records = []

            logger.info(
                f"MailchimpCampaignsGetEmailActivityOp.run: Sleeping for "
                f"{self.sleep_time_between_pages} seconds between pages. "
                f"Next page is {page+1}."
            )
            time.sleep(self.sleep_time_between_pages)

            # IMPORTANT: The page number is 1-indexed.
            page += 1

        # Only yield records if iterate_over_pages is false.
        if self.iterate_over_pages:
            logger.info(
                f"MailchimpCampaignsGetEmailActivityOp.run: "
                f"{self.iterate_over_pages=}. "
                f"Finished yielding all records, once per page."
            )

        else:
            logger.info(
                f"MailchimpCampaignsGetEmailActivityOp.run: "
                f"{self.iterate_over_pages=}. "
                f"Yielding all pages at once now. {len(final_records)=}"
            )
            yield OpMsg(
                data=final_records,
                metadata=MailchimpCampaignsGetEmailActivityOpMetadataModel(
                    current_page=page,
                    total_pages=total_pages,
                    total_records=total_records,
                ),
                audit=MailchimpCampaignsGetEmailActivityOpAuditModel(),
            )
