import logging
from typing import Generator, Optional

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class BigcomGenericRemovePiiOpConfigModel(BaseOpConfigModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BigcomGenericRemovePiiOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BigcomGenericRemovePiiOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BigcomGenericRemovePiiOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.bigcom.generic.remove_pii"
    OP_CONFIG_MODEL = BigcomGenericRemovePiiOpConfigModel
    OP_METADATA_MODEL = BigcomGenericRemovePiiOpMetadataModel
    OP_AUDIT_MODEL = BigcomGenericRemovePiiOpAuditModel

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
        logger.info(f"BigcomGenericRemovePiiOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="BigcomGenericRemovePiiOp.run:",
        )

        input_records = msg.data

        if not isinstance(input_records, list):
            raise NotImplementedError("Input records must be a list of dicts.")

        final_records = []

        logger.info(f"BigcomGenericRemovePiiOp.run: Redacting PII from the data.")

        keys_to_redact = [
            "email",
            "phone",
            "company",
            "first_name",
            "last_name",
            "address1",
            "address2",
            "street_1",
            "street_2",
            "ip_address",
            "ip_address_v6",
        ]
        for record in input_records:
            for key in keys_to_redact:
                if key in record["data"]:
                    record["data"][key] = "REDACTED_BY_ETL_BEFORE_STORAGE"

                if "addresses" in record["data"]:
                    for address in record["data"]["addresses"]:
                        if key in address:
                            address[key] = "REDACTED_BY_ETL_BEFORE_STORAGE"

                if "billing_address" in record["data"]:
                    if key in record["data"]["billing_address"]:
                        record["data"]["billing_address"][
                            key
                        ] = "REDACTED_BY_ETL_BEFORE_STORAGE"

            # record['data'] has been updated in place.
            final_records.append(record)

        logger.info(f"BigcomGenericRemovePiiOp.run: Done redacting PII from the data.")

        yield OpMsg(
            data=final_records,
            metadata=BigcomGenericRemovePiiOpMetadataModel(),
            audit=BigcomGenericRemovePiiOpAuditModel(),
        )
