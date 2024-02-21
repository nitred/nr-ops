import json
import logging
import time
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple

import pandas as pd
from pydantic import StrictBool, StrictStr, conlist

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.http import HTTPConnOp

logger = logging.getLogger(__name__)


class BigComOrdersGetAllOrdersOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    store_hash: StrictStr
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None
    sleep_time_between_pages: int = 5
    sort_by: Optional[
        Literal[
            "date_created:asc",
            "date_created:desc",
            "date_modified:asc",
            "date_modified:desc",
        ]
    ] = None
    timeout_seconds_per_request: float = 60
    min_date_modified: Optional[StrictStr] = None
    max_date_modified: Optional[StrictStr] = None
    min_date_created: Optional[StrictStr] = None
    max_date_created: Optional[StrictStr] = None
    iterate_over_pages: StrictBool

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BigComOrdersGetAllOrdersOpMetadataModel(BaseOpMetadataModel):
    current_page: int
    total_pages: int
    total_records: int

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BigComOrdersGetAllOrdersOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BigComOrdersGetAllOrdersOp(BaseGeneratorOp):
    """.

    NOTES:
    - The `page` field in the metadata is 1-indexed.
    """

    OP_TYPE = "generator.bigcom.orders.get_all_orders"
    OP_CONFIG_MODEL = BigComOrdersGetAllOrdersOpConfigModel
    OP_METADATA_MODEL = BigComOrdersGetAllOrdersOpMetadataModel
    OP_AUDIT_MODEL = BigComOrdersGetAllOrdersOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        store_hash: str,
        iterate_over_pages: bool,
        accepted_status_codes: Optional[List[int]] = None,
        sleep_time_between_pages: int = 5,
        sort_by: Optional[str] = None,
        timeout_seconds_per_request: float = 60,
        min_date_modified: Optional[StrictStr] = None,
        max_date_modified: Optional[StrictStr] = None,
        min_date_created: Optional[StrictStr] = None,
        max_date_created: Optional[StrictStr] = None,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.store_hash = store_hash
        self.sleep_time_between_pages = sleep_time_between_pages
        self.accepted_status_codes = (
            accepted_status_codes if accepted_status_codes else [200, 204]
        )
        self.sort_by = sort_by
        self.timeout_seconds_per_request = timeout_seconds_per_request
        self.min_date_modified = min_date_modified
        self.max_date_modified = max_date_modified
        self.min_date_created = min_date_created
        self.max_date_created = max_date_created
        self.iterate_over_pages = iterate_over_pages

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.get_connector(op_id=self.http_conn_id)

    def get_page(self, page: int) -> Tuple[int, str, Dict[str, Any]]:
        """."""
        params = {}
        if self.sort_by:
            params["sort"] = self.sort_by

        params["page"] = page

        if self.min_date_modified:
            params["min_date_modified"] = self.min_date_modified
        if self.max_date_modified:
            params["max_date_modified"] = self.max_date_modified
        if self.min_date_created:
            params["min_date_created"] = self.min_date_created
        if self.max_date_created:
            params["max_date_created"] = self.max_date_created

        # DOCS: https://developer.bigcommerce.com/docs/rest-management/orders#get-all-orders
        url = f"{self.http_conn.base_url}/stores/{self.store_hash}/v2/orders"
        logger.info(
            f"BigComOrdersGetAllOrdersOp.get_page: Fetching {page=} with {url=}"
        )

        etl_request_start_ts = pd.Timestamp.now(tz="UTC").isoformat()
        status_code, output_text = self.http_conn.call(
            method="get",
            url=url,
            requests_kwargs={
                "params": params,
                "headers": {
                    "Content-Type": "application/json",
                    # AUTHENTICATION HEADERS WILL BE INJECTED BY THE HTTP CONN FROM EXTRAS
                },
                "timeout": self.timeout_seconds_per_request,
            },
            accepted_status_codes=self.accepted_status_codes,
            # If status_code = 200, then the return_type is JSON
            # If status_code = 204, then the return_type is TEXT
            # Therefore we use text as the default return_type. We will convert to JSON if needed.
            return_type="text",
        )
        etl_response_end_ts = pd.Timestamp.now(tz="UTC").isoformat()

        etl_metadata_json = {
            "etl_request_start_ts": etl_request_start_ts,
            "etl_response_end_ts": etl_response_end_ts,
        }

        return status_code, output_text, etl_metadata_json

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"BigComOrdersGetAllOrdersOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="BigComOrdersGetAllOrdersOp.run:"
        )

        final_records = []
        page, total_records, total_pages = 1, 0, None
        while True:
            logger.info(f"BigComOrdersGetAllOrdersOp.run: Fetching {page=}.")

            status_code, output_text, etl_metadata_json = self.get_page(page=page)
            if status_code == 204:
                logger.info(
                    f"BigComOrdersGetAllOrdersOp.run: Received 204. "
                    f"No data exists for current {page=}. Stopping the loop to fetch "
                    f"additional pages."
                )
                total_pages = page - 1
                break

            output_json = json.loads(output_text)
            n_records = len(output_json)
            total_records += n_records

            logger.info(
                f"BigComOrdersGetAllOrdersOp.run: Done fetching {page=}. "
                f"Fetched {n_records=} this request. "
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
                for record in output_json
            ]
            final_records.extend(records)

            # Yield current page's results
            # If true, yield current records. Afterwards set final_records to empty list
            # If true, make sure to not yield records outside of loop.
            if self.iterate_over_pages:
                yield OpMsg(
                    data=final_records,
                    metadata=BigComOrdersGetAllOrdersOpMetadataModel(
                        current_page=page,
                        total_pages=-1,
                        total_records=total_records,
                    ),
                    audit=BigComOrdersGetAllOrdersOpAuditModel(),
                )
                final_records = []

            logger.info(
                f"BigComOrdersGetAllOrdersOp.run: Sleeping for "
                f"{self.sleep_time_between_pages} seconds between pages. "
                f"Next page is {page+1}."
            )
            time.sleep(self.sleep_time_between_pages)

            # IMPORTANT: The page number is 1-indexed.
            page += 1

        # Only yield records if iterate_over_pages is false.
        if self.iterate_over_pages:
            logger.info(
                f"BigComOrdersGetAllOrdersOp.run: {self.iterate_over_pages=}. "
                f"Finished yielding all records, once per page."
            )

        else:
            logger.info(
                f"BigComOrdersGetAllOrdersOp.run: {self.iterate_over_pages=}. "
                f"Yielding all pages at once now. {len(final_records)=}"
            )
            yield OpMsg(
                data=final_records,
                metadata=BigComOrdersGetAllOrdersOpMetadataModel(
                    current_page=page,
                    total_pages=total_pages,
                    total_records=total_records,
                ),
                audit=BigComOrdersGetAllOrdersOpAuditModel(),
            )
