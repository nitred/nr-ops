import json
import logging
import time
import traceback
from concurrent.futures import as_completed
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple

import pandas as pd
import requests
from pydantic import Field, StrictBool, StrictInt, StrictStr, conlist, validator
from requests_futures.sessions import FuturesSession

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.http import HTTPConnOp

logger = logging.getLogger(__name__)


class BrazeGenericPostJsonDataAsyncOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    endpoint: StrictStr
    accepted_status_codes: Optional[conlist(int, min_items=1)] = Field(
        default_factory=lambda: [200, 201]
    )
    number_of_concurrent_workers: StrictInt = 1
    timeout_seconds_per_request: StrictInt = 60

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    @validator("endpoint", pre=False)
    def validate_endpoint(cls, endpoint: str):
        if not endpoint.startswith("/"):
            raise ValueError(f"{endpoint=} must start with a forward slash (/). ")

        return endpoint


class BrazeGenericPostJsonDataAsyncOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BrazeGenericPostJsonDataAsyncOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BrazeGenericPostJsonDataAsyncOp(BaseConsumerOp):
    """."""

    OP_TYPE = "consumer.braze.generic_post_json_data_async"
    OP_CONFIG_MODEL = BrazeGenericPostJsonDataAsyncOpConfigModel
    OP_METADATA_MODEL = BrazeGenericPostJsonDataAsyncOpMetadataModel
    OP_AUDIT_MODEL = BrazeGenericPostJsonDataAsyncOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: StrictStr,
        endpoint: StrictStr,
        accepted_status_codes: Optional[List[int]] = None,
        timeout_seconds_per_request: int = 60,
        number_of_concurrent_workers: int = 1,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.accepted_status_codes = (
            accepted_status_codes if accepted_status_codes else [200]
        )
        self.timeout_seconds_per_request = timeout_seconds_per_request
        self.number_of_concurrent_workers = number_of_concurrent_workers

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.get_connector(op_id=self.http_conn_id)

    def run(self, time_step: TimeStep, msg: Optional[OpMsg] = None) -> OpMsg:
        """."""
        logger.info(f"BrazeGenericPostJsonDataAsyncOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="BrazeGenericPostJsonDataAsyncOp.run:",
        )

        if not isinstance(msg.data, list):
            raise TypeError(
                f"BrazeGenericPostJsonDataAsyncOp: msg.data is expected to be a "
                f"list/batch of json payloads. Instead received {type(msg.data)=}."
            )

        for payload_index, payload in enumerate(msg.data):
            if not isinstance(payload, dict):
                raise TypeError(
                    f"BrazeGenericPostJsonDataAsyncOp: "
                    f"msg.data[{payload_index}] is expected to be a dict/json payload. "
                    f"Instead received {type(payload)=}."
                )

        logger.info(
            f"BrazeGenericPostJsonDataAsyncOp.run: Initializing FuturesSession "
            f"with {self.number_of_concurrent_workers=}."
        )

        url = self.http_conn.base_url + self.endpoint
        logger.info(
            f"BrazeGenericPostJsonDataAsyncOp: Creating {len(msg.data)=} "
            f"requests futures with {self.timeout_seconds_per_request=} and {url=}."
        )
        futures = []

        session = FuturesSession(
            max_workers=self.number_of_concurrent_workers,
            adapter_kwargs={
                "pool_connections": self.number_of_concurrent_workers,
                "pool_maxsize": self.number_of_concurrent_workers,
            },
        )

        for payload_index, payload in enumerate(msg.data):
            # print(json.dumps(payload, indent=2))
            future = session.post(
                url=url,
                headers={
                    **self.http_conn.extras,
                },
                json=payload,
                timeout=self.timeout_seconds_per_request,
            )
            future.payload_index = payload_index
            future.request_start = pd.Timestamp.now(tz="UTC").isoformat()
            futures.append(future)

        logger.info(
            f"BrazeGenericPostJsonDataAsyncOp.run: " f"Done creating requests futures. "
        )

        failed_responses = {}
        successful_responses = {}
        wall_time_start = time.time()

        for future in as_completed(futures):
            try:
                response = future.result()
            except requests.exceptions.RequestException:
                logger.info(
                    f"BrazeGenericPostJsonDataAsyncOp: Caught RequestException "
                    f"for {future.payload_index=}. \n{traceback.format_exc()}\n"
                    f"CONTINUING WITH OTHER FUTURE REQUESTS..."
                )
                failed_responses[future.payload_index] = {
                    "payload_index": future.payload_index,
                    "exception": traceback.format_exc(),
                }
                continue

            # For failed responses, we will log the status code and the response text.
            if response.status_code not in self.accepted_status_codes:
                failed_responses[future.payload_index] = {
                    "payload_index": future.payload_index,
                    "status_code": response.status_code,
                    "text": response.text,
                    "response_time_taken_in_seconds": response.elapsed.total_seconds(),
                }
            # For successful responses, we will log the status code.
            else:
                successful_responses[future.payload_index] = {
                    "status_code": response.status_code,
                    "response_time_taken_in_seconds": response.elapsed.total_seconds(),
                }

        wall_time_end = time.time()

        if successful_responses:
            logger.info(
                f"BrazeGenericPostJsonDataAsyncOp.run: "
                f"SUCCESSFUL RESPONSES identified by their payload indexes "
                f"have acceptable status codes:\n"
                f"{json.dumps(successful_responses, indent=2, sort_keys=True)}"
            )

        if failed_responses:
            raise ValueError(
                f"BrazeGenericPostJsonDataAsyncOp.run: "
                f"FAILED RESPONSES identified by their payload indexes do not "
                f"have acceptable status codes:\n"
                f"{json.dumps(failed_responses, indent=2, sort_keys=True)}"
            )

        df_successful_response_times = pd.DataFrame(
            data={
                "successful_response_times_in_seconds": [
                    response["response_time_taken_in_seconds"]
                    for response in successful_responses.values()
                ],
            }
        )

        logger.info(
            f"BrazeGenericPostJsonDataAsyncOp.run: \n"
            f"{df_successful_response_times.describe(percentiles=[0.25, 0.5, 0.75, 0.9, 0.95])}\n\n"
            f"successful_response_times_total = {df_successful_response_times.successful_response_times_in_seconds.sum()}\n"
            f"wall_time = {wall_time_end - wall_time_start}\n"
        )

        df_failed_response_times = pd.DataFrame(
            data={
                "failed_response_times_in_seconds": [
                    response["response_time_taken_in_seconds"]
                    for response in failed_responses.values()
                ],
            }
        )

        logger.info(
            f"BrazeGenericPostJsonDataAsyncOp.run: \n"
            f"{df_failed_response_times.describe(percentiles=[0.25, 0.5, 0.75, 0.9, 0.95])}\n\n"
            f"failed_response_times_total = {df_failed_response_times.failed_response_times_in_seconds.sum()}\n"
            f"wall_time = {wall_time_end - wall_time_start}\n"
        )

        return OpMsg(
            data=None,
            metadata=BrazeGenericPostJsonDataAsyncOpMetadataModel(),
            audit=BrazeGenericPostJsonDataAsyncOpAuditModel(),
        )
