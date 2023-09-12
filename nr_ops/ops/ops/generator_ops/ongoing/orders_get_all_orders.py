import json
import logging
import time
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple

import pandas as pd
from pydantic import StrictBool, StrictInt, StrictStr, conlist
from zeep import Client
from zeep.helpers import serialize_object

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.http import HTTPConnOp

logger = logging.getLogger(__name__)


class OngoingOrdersSOAPGetAllOrdersOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    goods_owner_code: StrictStr
    sleep_time_between_pages: int = 5
    min_date_modified: Optional[StrictStr] = None
    max_date_modified: Optional[StrictStr] = None
    iterate_over_pages: StrictBool
    orders_per_page: StrictInt = 1000

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OngoingOrdersSOAPGetAllOrdersOpMetadataModel(BaseOpMetadataModel):
    current_page: int
    order_id_from: int
    total_pages: int
    total_records: int

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OngoingOrdersSOAPGetAllOrdersOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OngoingOrdersSOAPGetAllOrdersOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.ongoing.orders.soap_get_all_orders"
    OP_CONFIG_MODEL = OngoingOrdersSOAPGetAllOrdersOpConfigModel
    OP_METADATA_MODEL = OngoingOrdersSOAPGetAllOrdersOpMetadataModel
    OP_AUDIT_MODEL = OngoingOrdersSOAPGetAllOrdersOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        goods_owner_code: str,
        iterate_over_pages: bool,
        sleep_time_between_pages: int = 5,
        min_date_modified: Optional[StrictStr] = None,
        max_date_modified: Optional[StrictStr] = None,
        orders_per_page: int = 1000,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.goods_owner_code = goods_owner_code
        self.sleep_time_between_pages = sleep_time_between_pages
        self.min_date_modified = min_date_modified
        self.max_date_modified = max_date_modified
        self.iterate_over_pages = iterate_over_pages
        self.orders_per_page = orders_per_page

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.get_connector(op_id=self.http_conn_id)

        if not self.http_conn.base_url.endswith("/"):
            wdsl_url = self.http_conn.base_url + "/service.asmx?WSDL"
        else:
            raise NotImplementedError()

        self.zeep_client = Client(wsdl=wdsl_url)

    def get_page(self, order_id_from: int) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """."""
        params = {}

        params["OrderIdFrom"] = order_id_from

        if self.min_date_modified:
            params["LastUpdatedFrom"] = self.min_date_modified
        if self.max_date_modified:
            params["LastUpdatedTo"] = self.max_date_modified

        # DOCS: https://developer.ongoingwarehouse.com/get-orders-by-query
        logger.info(
            f"OngoingOrdersSOAPGetAllOrdersOp.get_page: Fetching page with "
            f"{order_id_from=}"
        )

        etl_request_start_ts = pd.Timestamp.now(tz="UTC").isoformat()
        response = self.zeep_client.service.GetOrdersByQuery(
            GoodsOwnerCode=self.goods_owner_code,
            UserName=self.http_conn.username,
            Password=self.http_conn.password,
            query=params,
        )
        etl_response_end_ts = pd.Timestamp.now(tz="UTC").isoformat()
        response_dict = serialize_object(response, dict)

        etl_metadata_json = {
            "etl_request_start_ts": etl_request_start_ts,
            "etl_response_end_ts": etl_response_end_ts,
        }

        return response_dict, etl_metadata_json

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"OngoingOrdersSOAPGetAllOrdersOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="OngoingOrdersSOAPGetAllOrdersOp.run:",
        )

        final_records = []
        page, total_records, total_pages, order_id_from = 1, 0, None, 1
        while True:
            logger.info(
                f"OngoingOrdersSOAPGetAllOrdersOp.run: Fetching {page=} and "
                f"{order_id_from=}."
            )

            output_json, etl_metadata_json = self.get_page(order_id_from=order_id_from)
            if output_json is None:
                logger.info(
                    f"OngoingOrdersSOAPGetAllOrdersOp.run: Received null Response. "
                    f"No data exists for current {page=}. Stopping the loop to fetch "
                    f"additional pages."
                )
                total_pages = page - 1
                break

            n_records = len(output_json["Order"])
            total_records += n_records

            logger.info(
                f"OngoingOrdersSOAPGetAllOrdersOp.run: Done fetching {page=} and "
                f"{order_id_from=}. "
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
                for record in output_json["Order"]
            ]
            final_records.extend(records)

            # Compute next order_id_from
            order_id_from = max(
                [record["data"]["OrderInfo"]["OrderId"] for record in records]
            )
            order_id_from += 1

            # Yield current page's results
            # If true, yield current records. Afterwards set final_records to empty list
            # If true, make sure to not yield records outside of loop.
            if self.iterate_over_pages:
                yield OpMsg(
                    data=final_records,
                    metadata=OngoingOrdersSOAPGetAllOrdersOpMetadataModel(
                        current_page=page,
                        total_pages=-1,
                        total_records=total_records,
                        order_id_from=order_id_from,
                    ),
                    audit=OngoingOrdersSOAPGetAllOrdersOpAuditModel(),
                )
                final_records = []

            logger.info(
                f"OngoingOrdersSOAPGetAllOrdersOp.run: Sleeping for "
                f"{self.sleep_time_between_pages} seconds between pages. "
                f"Next page is {page+1} and {order_id_from=}."
            )
            time.sleep(self.sleep_time_between_pages)

            # IMPORTANT: The page number is 1-indexed.
            page += 1

        # Only yield records if iterate_over_pages is false.
        if self.iterate_over_pages:
            logger.info(
                f"OngoingOrdersSOAPGetAllOrdersOp.run: {self.iterate_over_pages=}. "
                f"Finished yielding all records, once per page."
            )

        else:
            logger.info(
                f"OngoingOrdersSOAPGetAllOrdersOp.run: {self.iterate_over_pages=}. "
                f"Yielding all pages at once now. {len(final_records)=}"
            )
            yield OpMsg(
                data=final_records,
                metadata=OngoingOrdersSOAPGetAllOrdersOpMetadataModel(
                    current_page=page,
                    total_pages=total_pages,
                    total_records=total_records,
                    order_id_from=order_id_from,
                ),
                audit=OngoingOrdersSOAPGetAllOrdersOpAuditModel(),
            )
