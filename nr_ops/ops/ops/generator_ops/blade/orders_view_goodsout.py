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


class BladeOrdersViewGoodsoutOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    blade_get_token_op_id: StrictStr
    goodsout_id: StrictStr
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None
    remove_pii: bool = True

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeOrdersViewGoodsoutOpMetadataModel(BaseOpMetadataModel):
    goodsout_id: str
    rate_limit_requests_remaining: int

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeOrdersViewGoodsoutOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeOrdersViewGoodsoutOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.blade.orders.view_goodsout"
    OP_CONFIG_MODEL = BladeOrdersViewGoodsoutOpConfigModel
    OP_METADATA_MODEL = BladeOrdersViewGoodsoutOpMetadataModel
    OP_AUDIT_MODEL = BladeOrdersViewGoodsoutOpAuditModel

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

    def get_goodsout(self, goodsout_id: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """."""
        # Example URL:
        # https://api.blade.co.uk/v1/orders/goodsouts/123456789
        # ?expand=*
        url = f"{self.http_conn.base_url}/v1/orders/goodsouts/{goodsout_id}?expand=*"
        logger.info(
            f"BladeOrdersViewGoodsoutOp.get_page: Fetching {goodsout_id=} with {url=}"
        )

        etl_request_start_ts = str(pd.Timestamp.now(tz="UTC"))
        status_code, output_json = self.http_conn.call(
            method="get",
            url=url,
            requests_kwargs={
                "headers": {
                    "Content-Type": "application/json",
                    # Token gets refreshed if it has expired.
                    "Access-Token": self.blade_get_token_op.token,
                }
            },
            accepted_status_codes=self.accepted_status_codes,
            return_type="json",
        )
        etl_response_end_ts = str(pd.Timestamp.now(tz="UTC"))

        etl_metadata_json = {
            "etl_request_start_ts": etl_request_start_ts,
            "etl_response_end_ts": etl_response_end_ts,
        }

        return etl_metadata_json, output_json

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"BladeOrdersViewGoodsoutOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="BladeOrdersViewGoodsoutOp.run:"
        )

        logger.info(f"BladeOrdersViewGoodsoutOp.run: Fetching {self.goodsout_id=}.")

        etl_metadata_json, output_json = self.get_goodsout(goodsout_id=self.goodsout_id)
        rate_limit_requests_remaining = output_json["meta"]["request_rate_limit"][
            "requests_remaining"
        ]

        logger.info(
            f"BladeOrdersViewGoodsoutOp.run: Done fetching {self.goodsout_id=}. "
            f"{rate_limit_requests_remaining=}"
        )

        if self.remove_pii:
            logger.info(f"BladeOrdersViewGoodsoutOp.run: Redacting PII from the data.")
            # Redact PII from the data in place.
            for key in [
                "first_name",
                "last_name",
                "email",
                "mobile",
                "telephone",
                "secondary_telephone",
                "address_one",
                "address_two",
                "address_three",
                "company",
                # Specific to view_goodsout
                "vat_number",
            ]:
                output_json["data"]["shipping_address"][
                    key
                ] = "REDACTED_BY_ETL_BEFORE_STORAGE"

            for key in ["delivery_instructions", "gift_message"]:
                output_json["data"][key] = "REDACTED_BY_ETL_BEFORE_STORAGE"

            logger.info(
                f"BladeOrdersViewGoodsoutOp.run: Done redacting PII from the data."
            )

            logger.info(
                f"BladeOrdersViewGoodsoutOp.run: Yielding results for page "
                f"{self.goodsout_id=}."
            )

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
            metadata=BladeOrdersViewGoodsoutOpMetadataModel(
                goodsout_id=self.goodsout_id,
                rate_limit_requests_remaining=rate_limit_requests_remaining,
            ),
            audit=BladeOrdersViewGoodsoutOpAuditModel(),
        )
