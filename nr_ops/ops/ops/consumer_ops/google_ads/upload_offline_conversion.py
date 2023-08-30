import logging
from typing import Optional

from pydantic import StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.google_ads import GoogleAdsConnectorOp

logger = logging.getLogger(__name__)


class GoogleAdsUploadOfflineConversionOpConfigModel(BaseOpConfigModel):
    google_ads_conn_id: StrictStr
    customer_id: StrictStr
    conversion_action_id: StrictStr
    gclid: StrictStr
    conversion_date_time: StrictStr
    conversion_value: StrictStr
    currency_code: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleAdsUploadOfflineConversionOpMetadataModel(BaseOpMetadataModel):
    successful_upload: bool
    gclid: str
    conversion_date_time: str
    conversion_value: str

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleAdsUploadOfflineConversionOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleAdsUploadOfflineConversionOp(BaseConsumerOp):
    OP_TYPE = "consumer.google.ads.upload_offline_conversion"
    OP_CONFIG_MODEL = GoogleAdsUploadOfflineConversionOpConfigModel
    OP_METADATA_MODEL = GoogleAdsUploadOfflineConversionOpMetadataModel
    OP_AUDIT_MODEL = GoogleAdsUploadOfflineConversionOpAuditModel

    templated_fields = None

    def __init__(
        self,
        google_ads_conn_id: str,
        customer_id: str,
        conversion_action_id: str,
        currency_code: str,
        gclid: str,
        conversion_date_time: str,
        conversion_value: str,
        **kwargs,
    ):
        """."""
        self.google_ads_conn_id = google_ads_conn_id
        self.customer_id = customer_id
        self.conversion_action_id = conversion_action_id
        self.gclid = gclid
        self.conversion_date_time = conversion_date_time
        self.conversion_value = conversion_value
        self.currency_code = currency_code

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.google_ads_conn: GoogleAdsConnectorOp = op_manager.get_connector(
            op_id=self.google_ads_conn_id
        )
        self.client = self.google_ads_conn.client

    def run(self, time_step: TimeStep, msg: Optional[OpMsg] = None) -> OpMsg:
        """."""
        logger.info(f"GoogleAdsUploadOfflineConversionOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="GoogleAdsUploadOfflineConversionOp.run:",
        )

        click_conversion = self.client.get_type("ClickConversion")

        conversion_upload_service = self.client.get_service("ConversionUploadService")
        conversion_action_service = self.client.get_service("ConversionActionService")
        click_conversion.conversion_action = (
            conversion_action_service.conversion_action_path(
                self.customer_id, self.conversion_action_id
            )
        )

        # Sets the single specified ID field.
        click_conversion.gclid = self.gclid

        click_conversion.conversion_value = float(self.conversion_value)
        click_conversion.conversion_date_time = self.conversion_date_time
        click_conversion.currency_code = self.currency_code

        request = self.client.get_type("UploadClickConversionsRequest")
        request.customer_id = self.customer_id
        request.conversions.append(click_conversion)
        request.partial_failure = True
        conversion_upload_response = conversion_upload_service.upload_click_conversions(
            request=request,
        )
        uploaded_click_conversion = conversion_upload_response.results[0]

        if (
            uploaded_click_conversion.conversion_date_time == ""
            and uploaded_click_conversion.gclid == ""
            and uploaded_click_conversion.conversion_action == ""
        ):
            successful_upload = False
            logger.info(
                f"GoogleAdsUploadOfflineConversionOp.run: POSSIBLE FAILED UPLOAD. "
                f"Google Ads API returned an empty response. This most likely means "
                f"that the conversion was not accepted, likely because it was a "
                f"duplicate. The data that was sent to Google Ads is as follows: "
                f"gclid: {self.gclid}, "
                f"conversion_date_time: {self.conversion_date_time}, "
                f"conversion_value: {self.conversion_value}"
            )
        else:
            logger.info("GoogleAdsUploadOfflineConversionOp.run: SUCCESSFUL UPLOAD")
            successful_upload = True

        return OpMsg(
            data=None,
            metadata=GoogleAdsUploadOfflineConversionOpMetadataModel(
                successful_upload=successful_upload,
                gclid=self.gclid,
                conversion_date_time=self.conversion_date_time,
                conversion_value=self.conversion_value,
            ),
            audit=GoogleAdsUploadOfflineConversionOpAuditModel(),
        )
