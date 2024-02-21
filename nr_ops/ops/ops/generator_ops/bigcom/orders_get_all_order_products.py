import json
import logging
import time
from typing import Any, Dict, Generator, List, Optional, Tuple

import pandas as pd
from pydantic import StrictStr, conlist

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.http import HTTPConnOp

logger = logging.getLogger(__name__)


class BigComOrdersGetAllOrderProductsOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    store_hash: StrictStr
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None
    sleep_time_between_pages: int = 5
    timeout_seconds_per_request: float = 60
    order_id: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BigComOrdersGetAllOrderProductsOpMetadataModel(BaseOpMetadataModel):
    current_page: int
    total_pages: int
    total_records: int

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BigComOrdersGetAllOrderProductsOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BigComOrdersGetAllOrderProductsOp(BaseGeneratorOp):
    """.

    NOTES:
    - The `page` field in the metadata is 1-indexed.
    """

    OP_TYPE = "generator.bigcom.orders.get_all_order_products"
    OP_CONFIG_MODEL = BigComOrdersGetAllOrderProductsOpConfigModel
    OP_METADATA_MODEL = BigComOrdersGetAllOrderProductsOpMetadataModel
    OP_AUDIT_MODEL = BigComOrdersGetAllOrderProductsOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        store_hash: str,
        order_id: str,
        accepted_status_codes: Optional[List[int]] = None,
        sleep_time_between_pages: int = 5,
        timeout_seconds_per_request: float = 60,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.store_hash = store_hash
        self.sleep_time_between_pages = sleep_time_between_pages
        self.accepted_status_codes = (
            accepted_status_codes if accepted_status_codes else [200, 204]
        )
        self.timeout_seconds_per_request = timeout_seconds_per_request
        self.order_id = order_id

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.get_connector(op_id=self.http_conn_id)

    def get_page(self, page: int) -> Tuple[int, str, Dict[str, Any]]:
        """."""
        params = {}
        params["page"] = page

        # DOCS: https://developer.bigcommerce.com/docs/rest-management/orders/order-products
        url = f"{self.http_conn.base_url}/stores/{self.store_hash}/v2/orders/{self.order_id}/products"
        logger.info(
            f"BigComOrdersGetAllOrderProductsOp.get_page: Fetching {page=} with {url=}"
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
            # v2 API returns JSON or TEXT
            # 200 - JSON
            # 204 - TEXT
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
        logger.info(f"BigComOrdersGetAllOrderProductsOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="BigComOrdersGetAllOrderProductsOp.run:",
        )

        final_records = []
        page, total_records, total_pages = 1, 0, None
        while True:
            logger.info(f"BigComOrdersGetAllOrderProductsOp.run: Fetching {page=}.")

            status_code, output_text, etl_metadata_json = self.get_page(page=page)
            if status_code == 204:
                logger.info(
                    f"BigComOrdersGetAllOrderProductsOp.run: Received 204. "
                    f"No data exists for current {page=}. Stopping the loop to fetch "
                    f"additional pages."
                )
                total_pages = page - 1
                break

            output_json = json.loads(output_text)

            n_records = len(output_json)
            total_records += n_records

            logger.info(
                f"BigComOrdersGetAllOrderProductsOp.run: Done fetching {page=}. "
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

            logger.info(
                f"BigComOrdersGetAllOrderProductsOp.run: Sleeping for "
                f"{self.sleep_time_between_pages} seconds between pages. "
                f"Next page is {page+1}."
            )
            time.sleep(self.sleep_time_between_pages)

            # IMPORTANT: The page number is 1-indexed.
            page += 1

        yield OpMsg(
            data=final_records,
            metadata=BigComOrdersGetAllOrderProductsOpMetadataModel(
                current_page=page,
                total_pages=total_pages,
                total_records=total_records,
            ),
            audit=BigComOrdersGetAllOrderProductsOpAuditModel(),
        )
