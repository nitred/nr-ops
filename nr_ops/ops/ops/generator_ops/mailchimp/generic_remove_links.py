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


class MailchimpGenericRemoveLinksOpConfigModel(BaseOpConfigModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpGenericRemoveLinksOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpGenericRemoveLinksOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpGenericRemoveLinksOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.mailchimp.generic.remove_links"
    OP_CONFIG_MODEL = MailchimpGenericRemoveLinksOpConfigModel
    OP_METADATA_MODEL = MailchimpGenericRemoveLinksOpMetadataModel
    OP_AUDIT_MODEL = MailchimpGenericRemoveLinksOpAuditModel

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
        logger.info(f"MailchimpGenericRemoveLinksOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="MailchimpGenericRemoveLinksOp.run:",
        )

        input_records = msg.data

        if not isinstance(input_records, list):
            raise NotImplementedError("Input records must be a list of dicts.")

        final_records = []

        logger.info(
            f"MailchimpGenericRemoveLinksOp.run: Redacting links from the data."
        )
        # Remove links from records
        # NOTE: Links can cause issues with deduplication.
        for record in input_records:
            data = record["data"]
            if "archive_url" in data:
                data["archive_url"] = "REDACTED_BY_ETL_BEFORE_STORAGE"
            if "long_archive_url" in data:
                data["long_archive_url"] = "REDACTED_BY_ETL_BEFORE_STORAGE"
            if "subscribe_url_short" in data:
                data["subscribe_url_short"] = "REDACTED_BY_ETL_BEFORE_STORAGE"
            if "subscribe_url_long" in data:
                data["subscribe_url_long"] = "REDACTED_BY_ETL_BEFORE_STORAGE"
            if "_links" in data:
                data["_links"] = "REDACTED_BY_ETL_BEFORE_STORAGE"

            # record['data'] has been updated in place.
            final_records.append(record)

        logger.info(
            f"MailchimpGenericRemoveLinksOp.run: Done redacting links from the data."
        )

        yield OpMsg(
            data=final_records,
            metadata=MailchimpGenericRemoveLinksOpMetadataModel(),
            audit=MailchimpGenericRemoveLinksOpAuditModel(),
        )
