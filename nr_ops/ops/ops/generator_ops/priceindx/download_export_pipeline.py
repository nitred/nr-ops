import io
import json
import logging
import time
import uuid
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple, Union

import pandas as pd
import requests
from pydantic import StrictBool, StrictStr, conlist

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.http import HTTPConnOp

logger = logging.getLogger(__name__)


class PriceindxDownloadExportPipelineOpConfigModel(BaseOpConfigModel):
    http_conn_id: StrictStr
    setup_id: StrictStr
    setup_name: StrictStr
    # Could be templated
    export_type: Union[StrictStr, Literal["all_offers_excluded", "all_offers_included"]]
    accepted_status_codes: Optional[conlist(int, min_items=1)] = None
    max_wait_time_for_export: int = 900
    sleep_time_between_requests: int = 5
    timeout_seconds_per_request: float = 180

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PriceindxDownloadExportPipelineOpMetadataModel(BaseOpMetadataModel):
    setup_id: str
    setup_name: str
    export_type: str
    report_name: str
    export_id: int

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PriceindxDownloadExportPipelineOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PriceindxDownloadExportPipelineOp(BaseGeneratorOp):
    """.

    This is an operator to download PriceIndx exports which are available via the
    webpage. This is a full pipeline that does the following:
    * Logs into the website
    * Changes the customer setup i.e. change the setup_id we want to download the
      export for different setups or countries etc.
    * Starts an export
    * Waits for the export to be ready
    * Downloads the export file
    * Deletes the export

    This is meant to be a temporary Op while we wait for PriceIndx to implement an
    API to access this data.

    IMPORTANT:
    1. We hardcode URLs
    2. We hardcode the JSON payload for the POST request that starts the export.
       This is the most likely thing to CHANGE or BREAK in the future.
       In order to update the JSON payload, you need to goto the website and start an
       export manually and note the JSON payload using the browser's developer tools.
    """

    OP_TYPE = "generator.priceindx.download_export_pipeline"
    OP_CONFIG_MODEL = PriceindxDownloadExportPipelineOpConfigModel
    OP_METADATA_MODEL = PriceindxDownloadExportPipelineOpMetadataModel
    OP_AUDIT_MODEL = PriceindxDownloadExportPipelineOpAuditModel

    templated_fields = None

    def __init__(
        self,
        http_conn_id: str,
        setup_id: str,
        setup_name: str,
        export_type: Literal["all_offers_excluded", "all_offers_included"],
        accepted_status_codes: Optional[List[int]] = None,
        max_wait_time_for_export: int = 900,
        sleep_time_between_requests: int = 5,
        timeout_seconds_per_request: float = 180,
        **kwargs,
    ):
        """."""
        self.http_conn_id = http_conn_id
        self.setup_id = setup_id
        self.setup_name = setup_name
        self.export_type = export_type
        self.max_wait_time_for_export = max_wait_time_for_export
        self.sleep_time_between_requests = sleep_time_between_requests
        self.accepted_status_codes = (
            accepted_status_codes if accepted_status_codes else [200]
        )
        self.timeout_seconds_per_request = timeout_seconds_per_request

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.http_conn: HTTPConnOp = op_manager.get_connector(op_id=self.http_conn_id)

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"PriceindxDownloadExportPipelineOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="PriceindxDownloadExportPipelineOp.run:",
        )

        session = requests.session()
        session, cookie_jar, response, csrf_token = self.first_communication(session)
        time.sleep(self.sleep_time_between_requests)
        session, cookie_jar, response, csrf_token = self.login(
            session,
            csrf_token,
            username=self.http_conn.username,
            password=self.http_conn.password,
        )
        time.sleep(self.sleep_time_between_requests)
        session, cookie_jar, response, csrf_token = self.change_customersetup(
            session, csrf_token
        )
        time.sleep(self.sleep_time_between_requests)
        if self.export_type == "all_offers_included":
            post_export_method = self.post_export_large_file
        elif self.export_type == "all_offers_excluded":
            post_export_method = self.post_export_small_file
        else:
            raise NotImplementedError()
        (
            session,
            cookie_jar,
            response,
            csrf_token,
            report_name,
        ) = post_export_method(session, csrf_token)
        time.sleep(self.sleep_time_between_requests)
        (
            session,
            cookie_jar,
            response,
            csrf_token,
            export_id,
        ) = self.wait_until_export_is_ready(
            session, csrf_token, report_name=report_name
        )
        time.sleep(self.sleep_time_between_requests)
        (
            session,
            cookie_jar,
            response,
            csrf_token,
            bytesio,
            etl_metadata_json,
        ) = self.download_export_file(session, csrf_token, export_id)
        time.sleep(self.sleep_time_between_requests)
        session, cookie_jar, response, csrf_token = self.delete_export(
            session, csrf_token, export_id
        )

        df = pd.read_csv(bytesio, sep=",")
        records = df.to_dict(orient="records")
        records = [
            {
                "data": record,
                "etl_metadata": etl_metadata_json,
            }
            for record in records
        ]

        yield OpMsg(
            data=records,
            metadata=PriceindxDownloadExportPipelineOpMetadataModel(
                setup_id=self.setup_id,
                setup_name=self.setup_name,
                export_type=self.export_type,
                report_name=report_name,
                export_id=export_id,
            ),
            audit=PriceindxDownloadExportPipelineOpAuditModel(),
        )

    def first_communication(self, session):
        """."""
        logger.info(f"PriceindxDownloadExportPipelineOp.first_communication: Running.")
        response = session.get("https://customer.priceindx.com/accounts/login/")
        csrf_token = response.cookies["csrftoken"]
        cookie_jar = session.cookies
        if response.status_code not in self.accepted_status_codes:
            raise ValueError(
                f"PriceindxDownloadExportPipelineOp.first_communication: ",
                f"{response.status_code=} not in self.accepted_status_codes. "
                f"{response.text=}",
            )
        logger.info(f"PriceindxDownloadExportPipelineOp.first_communication: Done.")
        return session, cookie_jar, response, csrf_token

    def login(self, session, csrf_token, username, password):
        logger.info(f"PriceindxDownloadExportPipelineOp.login: Running.")
        headers = {"X-CSRFToken": csrf_token}
        data = {
            "username": username,
            "password": password,
            "csrfmiddlewaretoken": csrf_token,
        }
        response = session.post(
            "https://customer.priceindx.com/accounts/login/",
            data=data,
            headers=headers,
        )
        csrf_token = response.cookies["csrftoken"]
        cookie_jar = session.cookies
        if response.status_code not in self.accepted_status_codes:
            raise ValueError(
                f"PriceindxDownloadExportPipelineOp.login: ",
                f"{response.status_code=} not in self.accepted_status_codes. "
                f"{response.text=}",
            )
        logger.info(f"PriceindxDownloadExportPipelineOp.login: Done.")
        return session, cookie_jar, response, csrf_token

    def change_customersetup(self, session, csrf_token):
        logger.info(
            f"PriceindxDownloadExportPipelineOp.change_customersetup: Running "
            f"for {self.setup_id=} and {self.setup_name=}."
        )
        data = {"csrfmiddlewaretoken": csrf_token, "setupid": self.setup_id}
        headers = {"X-CSRFToken": csrf_token}
        response = session.post(
            "https://customer.priceindx.com/account/ajax/change_customersetup/",
            data=data,
            headers=headers,
        )
        cookie_jar = session.cookies
        if response.status_code not in self.accepted_status_codes:
            raise ValueError(
                f"PriceindxDownloadExportPipelineOp.change_customersetup: ",
                f"{response.status_code=} not in self.accepted_status_codes. "
                f"{response.text=}",
            )
        logger.info(f"PriceindxDownloadExportPipelineOp.change_customersetup: Done.")
        return session, cookie_jar, response, csrf_token

    def post_export_large_file(self, session, csrf_token):
        report_name = str(uuid.uuid4())
        logger.info(
            f"PriceindxDownloadExportPipelineOp.post_export_large_file: Running for "
            f"{report_name=}."
        )
        data = json.loads(
            """
            {"shared":false,"cfg":{"col_order":{"product":{"columns":[{"label":"Image","key":"image","vis":true},{"label":"SKU","key":"sku","vis":true},{"label":"Category","key":"category","vis":true},{"label":"Brand","key":"brand","vis":true},{"label":"Name","key":"name","vis":true},{"label":"Position","key":"position","vis":true},{"label":"Price pos.","key":"price_pos","vis":true},{"label":"Price pos. change","key":"price_pos_change","vis":true},{"label":"Price","key":"price","vis":true},{"label":"Price change","key":"price_change","vis":true},{"label":"Purchase Price","key":"in_price","vis":true},{"label":"Margin","key":"margin","vis":true},{"label":"Profit","key":"profit","vis":true},{"label":"Stock","key":"stock","vis":true}]},"dp":{"columns":[{"label":"Suggested price","key":"suggested_price","vis":true},{"label":"Price Margin","key":"suggested_price_margin","vis":true},{"label":"Price Based On","key":"suggestion_based_on","vis":true},{"label":"Group / Rule / Limit","key":"group","vis":true},{"label":"Limit: Lower margin","key":"margin_lower_limit","vis":true},{"label":"Limit: Upper margin","key":"margin_upper_limit","vis":true},{"label":"Price clamped","key":"clamped","vis":true}],"groupsets":[{"label":"Group set 0","key":"0","vis":true}]},"competitors":{"columns":[{"label":"Lowest competitor","key":"lowest_comp","vis":true}]}},"date_settings":{"report_sts":1692655200000,"ga_timestamps":{},"changes_occur":"last_week","offer_dates":["2023-08-22"]},"table_filters":{"dp_group_set":["0"]},"active_sorting":[],"global_filters":{},"nulls_last_sort":false},"name":"EMPTY","offers_included":true}
            """
        )
        data["name"] = report_name
        headers = {"X-CSRFToken": csrf_token}
        response = session.post(
            "https://customer.priceindx.com/reports/ajax_rundeck_table_export/",
            headers=headers,
            json=data,
        )
        cookie_jar = session.cookies
        if response.status_code not in self.accepted_status_codes:
            raise ValueError(
                f"PriceindxDownloadExportPipelineOp.post_export_large_file: ",
                f"{response.status_code=} not in self.accepted_status_codes. "
                f"{response.text=}",
            )
        logger.info(
            f"PriceindxDownloadExportPipelineOp.post_export_large_file: Started export "
            f"for {report_name=}."
        )
        return session, cookie_jar, response, csrf_token, report_name

    def post_export_small_file(self, session, csrf_token):
        report_name = str(uuid.uuid4())
        logger.info(
            f"PriceindxDownloadExportPipelineOp.post_export_small_file: Running for "
            f"{report_name=}."
        )
        data = json.loads(
            """
            {"shared":false,"cfg":{"col_order":{"product":{"columns":[{"label":"Image","key":"image","vis":true},{"label":"SKU","key":"sku","vis":true},{"label":"Category","key":"category","vis":true},{"label":"Brand","key":"brand","vis":true},{"label":"Name","key":"name","vis":true},{"label":"Position","key":"position","vis":true},{"label":"Price pos.","key":"price_pos","vis":true},{"label":"Price pos. change","key":"price_pos_change","vis":true},{"label":"Price","key":"price","vis":true},{"label":"Price change","key":"price_change","vis":true},{"label":"Purchase Price","key":"in_price","vis":true},{"label":"Margin","key":"margin","vis":true},{"label":"Profit","key":"profit","vis":true},{"label":"Stock","key":"stock","vis":true}]},"dp":{"columns":[{"label":"Suggested price","key":"suggested_price","vis":true},{"label":"Price Margin","key":"suggested_price_margin","vis":true},{"label":"Price Based On","key":"suggestion_based_on","vis":true},{"label":"Group / Rule / Limit","key":"group","vis":true},{"label":"Limit: Lower margin","key":"margin_lower_limit","vis":true},{"label":"Limit: Upper margin","key":"margin_upper_limit","vis":true},{"label":"Price clamped","key":"clamped","vis":true}],"groupsets":[{"label":"Group set 0","key":"0","vis":true}]},"competitors":{"columns":[{"label":"Lowest competitor","key":"lowest_comp","vis":true}]}},"date_settings":{"report_sts":1692655200000,"ga_timestamps":{},"changes_occur":"last_week","offer_dates":["2023-08-22"]},"table_filters":{"dp_group_set":["0"]},"active_sorting":[],"global_filters":{},"nulls_last_sort":false},"name":"EMPTY","offers_included":false}
            """
        )
        data["name"] = report_name
        headers = {"X-CSRFToken": csrf_token}
        response = session.post(
            "https://customer.priceindx.com/reports/ajax_rundeck_table_export/",
            headers=headers,
            json=data,
        )
        cookie_jar = session.cookies
        if response.status_code not in self.accepted_status_codes:
            raise ValueError(
                f"PriceindxDownloadExportPipelineOp.post_export_small_file: ",
                f"{response.status_code=} not in self.accepted_status_codes. "
                f"{response.text=}",
            )
        logger.info(
            f"PriceindxDownloadExportPipelineOp.post_export_small_file: Started export "
            f"for {report_name=}."
        )
        return session, cookie_jar, response, csrf_token, report_name

    def get_exports(self, session, csrf_token):
        logger.info(f"PriceindxDownloadExportPipelineOp.get_exports: Running.")
        headers = {"X-CSRFToken": csrf_token}
        response = session.get(
            "https://customer.priceindx.com/reports/get_user_exports/",
            headers=headers,
        )
        cookie_jar = session.cookies
        if response.status_code not in self.accepted_status_codes:
            raise ValueError(
                f"PriceindxDownloadExportPipelineOp.get_exports: ",
                f"{response.status_code=} not in self.accepted_status_codes. "
                f"{response.text=}",
            )
        logger.info(f"PriceindxDownloadExportPipelineOp.get_exports: Done.")
        return session, cookie_jar, response, csrf_token

    def wait_until_export_is_ready(
        self,
        session,
        csrf_token,
        report_name,
    ):
        logger.info(
            f"PriceindxDownloadExportPipelineOp.wait_until_export_is_ready: Running."
        )
        start_time = time.time()
        while True:
            session, cookie_jar, response, csrf_token = self.get_exports(
                session, csrf_token
            )
            if response.status_code not in self.accepted_status_codes:
                raise ValueError(
                    f"PriceindxDownloadExportPipelineOp.wait_until_export_is_ready: ",
                    f"{response.status_code=} not in self.accepted_status_codes",
                )

            exports = response.json()["exports"]
            for export in exports:
                if export["name"] == report_name:
                    if export["status"] == 1:
                        logger.info(
                            f"PriceindxDownloadExportPipelineOp.wait_until_export_is_ready: "
                            f"Done waiting for export {report_name=} to be ready. "
                            f"Export is ready with export_id={export['id']}."
                        )
                        return (
                            session,
                            cookie_jar,
                            response,
                            csrf_token,
                            export["id"],
                        )
                    else:
                        logger.info(
                            f"PriceindxDownloadExportPipelineOp.wait_until_export_is_ready: "
                            f"Waiting for export {report_name=} to be ready. "
                            f"Sleeping for {self.sleep_time_between_requests} seconds."
                        )
                        break
            else:
                raise ValueError(
                    f"PriceindxDownloadExportPipelineOp.wait_until_export_is_ready: "
                    f"Export with {report_name=} not found in exports."
                )

            time.sleep(self.sleep_time_between_requests)
            wait_time = time.time() - start_time

            if wait_time > self.max_wait_time_for_export:
                raise ValueError(
                    f"PriceindxDownloadExportPipelineOp.wait_until_export_is_ready: "
                    f"Exceeded {self.max_wait_time_for_export=} while waiting for "
                    f"export with {report_name=}."
                )

    def download_export_file(self, session, csrf_token, export_id):
        logger.info(f"PriceindxDownloadExportPipelineOp.download_export_file: Running.")
        headers = {"X-CSRFToken": csrf_token}
        etl_request_start_ts = pd.Timestamp.now(tz="UTC").isoformat()
        response = session.get(
            f"https://customer.priceindx.com/reports/get_file/{export_id}",
            headers=headers,
        )
        etl_response_end_ts = pd.Timestamp.now(tz="UTC").isoformat()
        etl_metadata_json = {
            "etl_request_start_ts": etl_request_start_ts,
            "etl_response_end_ts": etl_response_end_ts,
        }
        cookie_jar = session.cookies
        if response.status_code not in self.accepted_status_codes:
            raise ValueError(
                f"PriceindxDownloadExportPipelineOp.download_export_file: ",
                f"{response.status_code=} not in self.accepted_status_codes. "
                f"{response.text=}",
            )
        bytesio = io.BytesIO(response.content)
        bytesio.seek(0)
        logger.info(
            f"PriceindxDownloadExportPipelineOp.download_export_file: Done "
            f"downloading export file with {export_id=}. "
            f"File size approx {bytesio.getbuffer().nbytes//1000:,} KiB"
        )
        return session, cookie_jar, response, csrf_token, bytesio, etl_metadata_json

    def delete_export(self, session, csrf_token, export_id):
        logger.info(f"PriceindxDownloadExportPipelineOp.delete_export: Running.")
        data = {"id": export_id}
        headers = {"X-CSRFToken": csrf_token}
        response = session.post(
            f"https://customer.priceindx.com/reports/delete_user_export/",
            headers=headers,
            json=data,
        )
        cookie_jar = session.cookies
        if response.status_code not in self.accepted_status_codes:
            raise ValueError(
                f"PriceindxDownloadExportPipelineOp.delete_export: ",
                f"{response.status_code=} not in self.accepted_status_codes. "
                f"{response.text=}",
            )
        logger.info(
            f"PriceindxDownloadExportPipelineOp.delete_export: Done "
            f"deleting export file with {export_id=}"
        )
        return session, cookie_jar, response, csrf_token
