import logging
import time
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple

import pandas as pd
from pydantic import BaseModel, StrictStr, conlist

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.http import HTTPConnOp
from nr_ops.ops.ops.generator_ops.blade.get_token import BladeGetTokenOp

logger = logging.getLogger(__name__)


class FilterConfigModel(BaseModel):
    filter_by: Literal["status"]
    filter_op: Literal["IN", "NOT_IN"]
    filter_date: StrictStr


class BladeProductsListVariationsOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    blade_get_token_op_id: StrictStr
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None
    sleep_time_between_pages: int = 5
    filter_config: Optional[FilterConfigModel] = None
    sort_by: Optional[
        Literal[
            "created",
            "product.channel.name",
            "sku",
            "status",
        ]
    ] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeProductsListVariationsOpMetadataModel(BaseOpMetadataModel):
    page: int
    total_pages: int
    n_records: int
    n_records_processed: int
    total_records: int
    rate_limit_requests_remaining: int

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeProductsListVariationsOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BladeProductsListVariationsOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.blade.products.list_variations"
    OP_CONFIG_MODEL = BladeProductsListVariationsOpConfigModel
    OP_METADATA_MODEL = BladeProductsListVariationsOpMetadataModel
    OP_AUDIT_MODEL = BladeProductsListVariationsOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        blade_get_token_op_id: str,
        accepted_status_codes: Optional[List[int]] = None,
        sleep_time_between_pages: int = 5,
        filter_config: Optional[Dict[str, Any]] = None,
        sort_by: Optional[str] = None,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.blade_get_token_op_id = blade_get_token_op_id
        self.sleep_time_between_pages = sleep_time_between_pages
        self.accepted_status_codes = accepted_status_codes
        self.filter_config = filter_config
        self.sort_by = sort_by

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.get_connector(op_id=self.http_conn_id)

        self.blade_get_token_op: BladeGetTokenOp = op_manager.op.get_op(
            op_id=self.blade_get_token_op_id
        ).op_obj

        # Will be populated in `run`
        self.filter: Optional[FilterConfigModel] = None

    def get_page(self, page: int) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """."""
        if self.filter:
            # E.g. "&date_placed[ON_DATE]=2021-01-01"
            filter_string = f"&{self.filter.filter_by}[{self.filter.filter_op}]={self.filter.filter_date}"
        else:
            filter_string = ""

        if self.sort_by:
            sort_string = f"&sorting={self.sort_by}"
        else:
            sort_string = ""

        # Example URL:
        # https://api.blade.co.uk/v1/orders/goodsouts
        # ?expand=*&date_placed[ON_DATE]=2023-02-20&sorting=bid&page=1
        url = (
            f"{self.http_conn.base_url}/v1/products/variations"
            f"?expand=*{filter_string}{sort_string}&page={page}"
        )
        logger.info(
            f"BladeProductsListVariationsOp.get_page: Fetching {page=} with {url=}"
        )

        etl_request_start_ts = pd.Timestamp.now(tz="UTC").isoformat()
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
        logger.info(f"BladeProductsListVariationsOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="BladeProductsListVariationsOp.run:",
        )

        # Filter config could be templated so we apply this operation after templating.
        self.filter = (
            FilterConfigModel(**self.filter_config) if self.filter_config else None
        )

        page, total_pages, n_records_processed = 1, None, 0
        while (total_pages is None) or (page <= total_pages):

            if total_pages:
                logger.info(
                    f"BladeProductsListVariationsOp.run: Sleeping for "
                    f"{self.sleep_time_between_pages} seconds between pages. "
                    f"Next page is  {page=} of {total_pages=}."
                )
                time.sleep(self.sleep_time_between_pages)

            logger.info(
                f"BladeProductsListVariationsOp.run: Fetching {page=} of {total_pages=}."
            )

            etl_metadata_json, output_json = self.get_page(page=page)
            n_records = len(output_json["data"])
            n_records_processed += n_records
            rate_limit_requests_remaining = output_json["meta"]["request_rate_limit"][
                "requests_remaining"
            ]

            # We get the total_pages from the data on every iteration. This is
            # because the total_pages can change as the data is being fetched.
            # NOTE: This may be flaky.
            total_pages = output_json["meta"]["paging"]["total_pages"]
            total_records = output_json["meta"]["paging"]["total_records"]

            logger.info(
                f"BladeProductsListVariationsOp.run: Done fetching {page=} of {total_pages=}. "
                f"Fetched {n_records=} this request. Fetched "
                f"{n_records_processed}/{total_records} records so far. "
                f"{rate_limit_requests_remaining=}"
            )

            ############################################################################
            # NOTE: Adding `etl_metadata` into response json (output_json).
            # * This is to add some metadata about the ETL process, primarily some
            #   timestamps which may help with creating event stores/logs.
            # * This may also help with incremental modelling.
            # * DO NOT USE this information when calculating dedup_uuid.
            ############################################################################
            etl_metadata_json["time_step"] = time_step.to_json_dict()
            etl_metadata_json["blade"] = {"meta": output_json.get("meta", {})}
            output_json["etl_metadata"] = etl_metadata_json

            logger.info(
                f"BladeProductsListVariationsOp.run: Yielding results for page "
                f"{page}/{total_pages}."
            )

            yield OpMsg(
                data=output_json,
                metadata=BladeProductsListVariationsOpMetadataModel(
                    page=page,
                    total_pages=total_pages,
                    n_records=n_records,
                    n_records_processed=n_records_processed,
                    total_records=total_records,
                    rate_limit_requests_remaining=rate_limit_requests_remaining,
                ),
                audit=BladeProductsListVariationsOpAuditModel(),
            )

            # IMPORTANT: The page number is 1-indexed.
            # We increment the page number after we yield the data.
            page += 1
