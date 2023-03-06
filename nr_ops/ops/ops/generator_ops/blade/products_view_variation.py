import json
import logging
import re
import time
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple

import pandas as pd
from pydantic import BaseModel, StrictStr, conlist, validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.http import HTTPConnOp
from nr_ops.ops.ops.generator_ops.blade.get_token import BladeGetTokenOp

logger = logging.getLogger(__name__)


class BladeProductsViewVariationOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    blade_get_token_op_id: StrictStr
    variation_id: StrictStr
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeProductsViewVariationOpMetadataModel(BaseOpMetadataModel):
    variation_id: str
    rate_limit_requests_remaining: int

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeProductsViewVariationOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeProductsViewVariationOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.blade.products.view_variation"
    OP_CONFIG_MODEL = BladeProductsViewVariationOpConfigModel
    OP_METADATA_MODEL = BladeProductsViewVariationOpMetadataModel
    OP_AUDIT_MODEL = BladeProductsViewVariationOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        blade_get_token_op_id: str,
        variation_id: str,
        accepted_status_codes: Optional[List[int]] = None,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.blade_get_token_op_id = blade_get_token_op_id
        self.variation_id = variation_id
        self.accepted_status_codes = accepted_status_codes

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.connector.get_connector(
            op_id=self.http_conn_id
        )

        self.blade_get_token_op: BladeGetTokenOp = op_manager.op.get_op(
            op_id=self.blade_get_token_op_id
        ).op_obj

    def get_variation(self, variation_id: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """."""
        # Example URL:
        # https://api.blade.co.uk/v1/products/variations/123456789
        # ?expand=*
        url = (
            f"{self.http_conn.base_url}/v1/products/variations/{variation_id}?expand=*"
        )
        logger.info(
            f"BladeProductsViewVariationOp.get_page: "
            f"Fetching {variation_id=} with {url=}"
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
        logger.info(f"BladeProductsViewVariationOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="BladeProductsViewVariationOp.run:"
        )

        logger.info(f"BladeProductsViewVariationOp.run: Fetching {self.variation_id=}.")

        etl_metadata_json, output_json = self.get_variation(
            variation_id=self.variation_id
        )
        rate_limit_requests_remaining = output_json["meta"]["request_rate_limit"][
            "requests_remaining"
        ]

        logger.info(
            f"BladeProductsViewVariationOp.run: Done fetching {self.variation_id=}. "
            f"{rate_limit_requests_remaining=}"
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
            metadata=BladeProductsViewVariationOpMetadataModel(
                variation_id=self.variation_id,
                rate_limit_requests_remaining=rate_limit_requests_remaining,
            ),
            audit=BladeProductsViewVariationOpAuditModel(),
        )
