import hashlib
import json
import logging
import time
import uuid
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple, Union

import bcrypt
import pandas as pd
from pydantic import StrictBool, StrictInt, StrictStr, conlist, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class MailchimpGenericRemovePiiOpConfigModel(BaseOpConfigModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpGenericRemovePiiOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpGenericRemovePiiOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpGenericRemovePiiOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.mailchimp.generic.remove_pii"
    OP_CONFIG_MODEL = MailchimpGenericRemovePiiOpConfigModel
    OP_METADATA_MODEL = MailchimpGenericRemovePiiOpMetadataModel
    OP_AUDIT_MODEL = MailchimpGenericRemovePiiOpAuditModel

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
        logger.info(f"MailchimpGenericRemovePiiOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="MailchimpGenericRemovePiiOp.run:",
        )

        input_records = msg.data

        if not isinstance(input_records, list):
            raise NotImplementedError("Input records must be a list of dicts.")

        final_records = []

        logger.info(
            f"MailchimpGenericRemovePiiOp.run: " f"Redacting PII from the data."
        )
        for record in input_records:
            for key in [
                "email_address",
                "merge_fields",
                "full_name",
                "location",
                "ip_signup",
                "ip_opt",
            ]:
                if key in record["data"]:
                    record["data"][key] = "REDACTED_BY_ETL_BEFORE_STORAGE"

            # record['data'] has been updated in place.
            final_records.append(record)

        logger.info(
            f"MailchimpGenericRemovePiiOp.run: Done redacting PII from the data."
        )

        yield OpMsg(
            data=final_records,
            metadata=MailchimpGenericRemovePiiOpMetadataModel(),
            audit=MailchimpGenericRemovePiiOpAuditModel(),
        )
