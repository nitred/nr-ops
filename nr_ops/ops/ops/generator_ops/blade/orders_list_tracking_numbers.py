import logging
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
from nr_ops.ops.ops.generator_ops.blade.get_token import BladeGetTokenOp

logger = logging.getLogger(__name__)


class BladeOrdersBulkListTrackingNumbersOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    blade_get_token_op_id: StrictStr
    goodsout_id: StrictStr
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None
    remove_pii: bool = True

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeOrdersBulkListTrackingNumbersOpMetadataModel(BaseOpMetadataModel):
    goodsout_id: str
    rate_limit_requests_remaining: int

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeOrdersBulkListTrackingNumbersOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeOrdersBulkListTrackingNumbersOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.blade.orders.bulk_list_tracking_numbers"
    OP_CONFIG_MODEL = BladeOrdersBulkListTrackingNumbersOpConfigModel
    OP_METADATA_MODEL = BladeOrdersBulkListTrackingNumbersOpMetadataModel
    OP_AUDIT_MODEL = BladeOrdersBulkListTrackingNumbersOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        blade_get_token_op_id: str,
        goodsout_id: str,
        accepted_status_codes: Optional[List[int]] = None,
        remove_pii: bool = True,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.blade_get_token_op_id = blade_get_token_op_id
        self.goodsout_id = goodsout_id
        self.accepted_status_codes = accepted_status_codes
        self.remove_pii = remove_pii

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.get_connector(op_id=self.http_conn_id)

        self.blade_get_token_op: BladeGetTokenOp = op_manager.op.get_op(
            op_id=self.blade_get_token_op_id
        ).op_obj

    def list_order_metadata(
        self, goodsout_id: str
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """."""
        # Example URL:
        # https://api.blade.co.uk/v1/orders/goodsouts/123456789/metadata
        url = f"{self.http_conn.base_url}/v1/orders/goodsouts/bulk_tracking_numbers"
        logger.info(
            f"BladeOrdersBulkListTrackingNumbersOp.get_page: Fetching {goodsout_id=} with {url=}"
        )

        etl_request_start_ts = pd.Timestamp.now(tz="UTC").isoformat()
        status_code, output_json = self.http_conn.call(
            # The endpoint expects a PUT request.
            method="put",
            url=url,
            requests_kwargs={
                "headers": {
                    "Content-Type": "application/json",
                    # Token gets refreshed if it has expired.
                    "Access-Token": self.blade_get_token_op.token,
                },
                "json": {
                    "order_goodsout_ids": [
                        # The endpoint expects an integer.
                        int(goodsout_id),
                    ]
                },
            },
            accepted_status_codes=self.accepted_status_codes,
            return_type="json",
        )
        etl_response_end_ts = pd.Timestamp.now(tz="UTC").isoformat()

        etl_metadata_json = {
            "etl_request_start_ts": etl_request_start_ts,
            "etl_response_end_ts": etl_response_end_ts,
        }

        return etl_metadata_json, output_json

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"BladeOrdersBulkListTrackingNumbersOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="BladeOrdersBulkListTrackingNumbersOp.run:",
        )

        logger.info(
            f"BladeOrdersBulkListTrackingNumbersOp.run: Fetching {self.goodsout_id=}."
        )

        etl_metadata_json, output_json = self.list_order_metadata(
            goodsout_id=self.goodsout_id
        )
        rate_limit_requests_remaining = output_json["meta"]["request_rate_limit"][
            "requests_remaining"
        ]

        logger.info(
            f"BladeOrdersBulkListTrackingNumbersOp.run: Done fetching {self.goodsout_id=}. "
            f"{rate_limit_requests_remaining=}"
        )

        ################################################################################
        # NOTE: Manipulating the output_json in place here to add order_id into it.
        # The output_json doesn't contain any order information, so we need to add it
        # so that it can be tied to other order tables.
        # Additionally, the output_json needs the order_id information to make sure
        # that different orders are guaranteed to generate different dedup_uuids.
        ################################################################################
        tracking_numbers = output_json.get("data", [])
        output_json["data"] = {}
        output_json["data"]["id"] = self.goodsout_id
        output_json["data"]["tracking_numbers"] = tracking_numbers

        ################################################################################
        # NOTE: Adding `etl_metadata` into response json (output_json).
        # * This is to add some metadata about the ETL process, primarily some
        #   timestamps which may help with creating event stores/logs.
        # * This may also help with incremental modelling.
        # * DO NOT USE this information when calculating dedup_uuid.
        ################################################################################
        etl_metadata_json["time_step"] = time_step.to_json_dict()
        etl_metadata_json["blade"] = {"meta": output_json.get("meta", {})}
        output_json["etl_metadata"] = etl_metadata_json

        yield OpMsg(
            data=output_json,
            metadata=BladeOrdersBulkListTrackingNumbersOpMetadataModel(
                goodsout_id=self.goodsout_id,
                rate_limit_requests_remaining=rate_limit_requests_remaining,
            ),
            audit=BladeOrdersBulkListTrackingNumbersOpAuditModel(),
        )
