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


class OngoingOrdersRESTGetWayBillRowsOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None
    timeout_seconds_per_request: float = 60
    order_id: Optional[StrictStr] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OngoingOrdersRESTGetWayBillRowsOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OngoingOrdersRESTGetWayBillRowsOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OngoingOrdersRESTGetWayBillRowsOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.ongoing.orders.rest_get_way_bill_rows"
    OP_CONFIG_MODEL = OngoingOrdersRESTGetWayBillRowsOpConfigModel
    OP_METADATA_MODEL = OngoingOrdersRESTGetWayBillRowsOpMetadataModel
    OP_AUDIT_MODEL = OngoingOrdersRESTGetWayBillRowsOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        order_id: Optional[str] = None,
        accepted_status_codes: Optional[List[int]] = None,
        timeout_seconds_per_request: float = 60,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.order_id = order_id
        self.accepted_status_codes = (
            accepted_status_codes if accepted_status_codes else [200]
        )
        self.timeout_seconds_per_request = timeout_seconds_per_request

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.get_connector(op_id=self.http_conn_id)

    def get_page(self, order_id: str) -> Tuple[int, Dict[str, Any], Dict[str, Any]]:
        """."""
        params = {}

        # DOCS: https://developer.ongoingwarehouse.com/REST/v1/index.html#/Orders/Orders_GetWayBillRows
        url = f"{self.http_conn.base_url}/api/v1/orders/{order_id}/wayBillRows"
        logger.info(
            f"OngoingOrdersRESTGetWayBillRowsOp.get_page: Fetching waybill rows for "
            f"{order_id=} with {url=}"
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
        logger.info(f"OngoingOrdersRESTGetWayBillRowsOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="OngoingOrdersRESTGetWayBillRowsOp.run:",
        )

        if self.order_id:
            order_ids = [self.order_id]
        else:
            raise NotImplementedError()

        final_records = []
        for order_id in order_ids:
            logger.info(
                f"OngoingOrdersRESTGetWayBillRowsOp.run: Fetching way bill rows for "
                f"{order_id=}."
            )

            status_code, output_json, etl_metadata_json = self.get_page(
                order_id=order_id
            )

            logger.info(
                f"OngoingOrdersRESTGetWayBillRowsOp.run: Done fetching way bill rows "
                f"for {order_id=}."
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

            # ADD `order_id` to each record since it is missing in the response.
            for record in records:  # type: dict
                record["data"]["orderId"] = order_id

            final_records.extend(records)

        yield OpMsg(
            data=final_records,
            metadata=OngoingOrdersRESTGetWayBillRowsOpMetadataModel(),
            audit=OngoingOrdersRESTGetWayBillRowsOpAuditModel(),
        )
