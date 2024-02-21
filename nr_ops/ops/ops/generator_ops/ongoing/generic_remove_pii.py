import logging
from typing import Generator, Optional

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class OngoingGenericRemovePiiOpConfigModel(BaseOpConfigModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OngoingGenericRemovePiiOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OngoingGenericRemovePiiOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class OngoingGenericRemovePiiOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.ongoing.generic.remove_pii"
    OP_CONFIG_MODEL = OngoingGenericRemovePiiOpConfigModel
    OP_METADATA_MODEL = OngoingGenericRemovePiiOpMetadataModel
    OP_AUDIT_MODEL = OngoingGenericRemovePiiOpAuditModel

    templated_fields = None

    def __init__(
        self,
        **kwargs,
    ):
        """."""
        self.templated_fields = kwargs.get("templated_fields", [])

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"OngoingGenericRemovePiiOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="OngoingGenericRemovePiiOp.run:",
        )

        input_records = msg.data

        if not isinstance(input_records, list):
            raise NotImplementedError("Input records must be a list of dicts.")

        final_records = []

        logger.info(f"OngoingGenericRemovePiiOp.run: Redacting PII from the data.")

        keys_to_redact = [
            "Name",
            "Address",
            "Address2",
            "Address3",
            "TelePhone",
            "MobilePhone",
            "Email",
            "Remark",
        ]
        for record in input_records:
            for key in keys_to_redact:
                if "Consignee" in record["data"]:
                    record["data"]["Consignee"][key] = "REDACTED_BY_ETL_BEFORE_STORAGE"

            if "OrderInfo" in record["data"]:
                if "EmailNotification" in record["data"]["OrderInfo"]:
                    record["data"]["OrderInfo"]["EmailNotification"][
                        "Value"
                    ] = "REDACTED_BY_ETL_BEFORE_STORAGE"

                if "TelephoneNotification" in record["data"]["OrderInfo"]:
                    record["data"]["OrderInfo"]["TelephoneNotification"][
                        "Value"
                    ] = "REDACTED_BY_ETL_BEFORE_STORAGE"

                if "SMSNotification" in record["data"]["OrderInfo"]:
                    record["data"]["OrderInfo"]["SMSNotification"][
                        "Value"
                    ] = "REDACTED_BY_ETL_BEFORE_STORAGE"

            # record['data'] has been updated in place.
            final_records.append(record)

        logger.info(f"OngoingGenericRemovePiiOp.run: Done redacting PII from the data.")

        yield OpMsg(
            data=final_records,
            metadata=OngoingGenericRemovePiiOpMetadataModel(),
            audit=OngoingGenericRemovePiiOpAuditModel(),
        )
