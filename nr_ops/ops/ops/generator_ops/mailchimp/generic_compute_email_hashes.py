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


class MailchimpGenericComputeEmailHashesOpConfigModel(BaseOpConfigModel):
    yield_only_email_data: StrictBool = True
    compute_hash_md5: StrictBool = True
    compute_hash_uuid5: StrictBool = True
    compute_hash_sha256: StrictBool = True
    compute_hash_sha512: StrictBool = True
    compute_hash_bcrypt: StrictBool = False
    bcrypt_salt_work_factor: StrictInt = 4

    @root_validator(pre=False)
    def validate_bcrypt_config(cls, values):
        # If compute_hash_bcrypt is True, bcrypt_salt_work_factor must be set.
        if values.get("compute_hash_bcrypt") is True and not isinstance(
            values.get("bcrypt_salt_work_factor", None), int
        ):
            raise ValueError(
                "If compute_hash_bcrypt is True, bcrypt_salt_work_factor must be set."
            )
        return values

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpGenericComputeEmailHashesOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpGenericComputeEmailHashesOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class MailchimpGenericComputeEmailHashesOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.mailchimp.generic.compute_email_hashes"
    OP_CONFIG_MODEL = MailchimpGenericComputeEmailHashesOpConfigModel
    OP_METADATA_MODEL = MailchimpGenericComputeEmailHashesOpMetadataModel
    OP_AUDIT_MODEL = MailchimpGenericComputeEmailHashesOpAuditModel

    templated_fields = None

    def __init__(
        self,
        yield_only_email_data: bool = True,
        compute_hash_md5: bool = True,
        compute_hash_uuid5: bool = True,
        compute_hash_sha256: bool = True,
        compute_hash_sha512: bool = True,
        compute_hash_bcrypt: bool = False,
        bcrypt_salt_work_factor: int = 4,
        **kwargs,
    ):
        """."""
        self.yield_only_email_data = yield_only_email_data
        self.compute_hash_md5 = compute_hash_md5
        self.compute_hash_uuid5 = compute_hash_uuid5
        self.compute_hash_sha256 = compute_hash_sha256
        self.compute_hash_sha512 = compute_hash_sha512
        self.compute_hash_bcrypt = compute_hash_bcrypt
        self.bcrypt_salt_work_factor = bcrypt_salt_work_factor
        self.templated_fields = kwargs.get("templated_fields", [])

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"MailchimpGenericComputeEmailHashesOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="MailchimpGenericComputeEmailHashesOp.run:",
        )

        input_records = msg.data

        if not isinstance(input_records, list):
            raise NotImplementedError("Input records must be a list of dicts.")

        final_records = []

        for record in input_records:
            if "email_address" not in record["data"]:
                raise NotImplementedError(
                    "Input records must have an email_address field."
                )

            if self.yield_only_email_data:
                record["data"] = {"email_address": record["data"]["email_address"]}

            data = record["data"]

            if self.compute_hash_md5:
                data["email_address_hash_md5"] = hashlib.md5(
                    data["email_address"].encode("utf-8")
                ).hexdigest()

            if self.compute_hash_uuid5:
                data["email_address_hash_uuid5"] = str(
                    uuid.uuid5(uuid.NAMESPACE_URL, data["email_address"])
                )

            if self.compute_hash_sha256:
                data["email_address_hash_sha256"] = hashlib.sha256(
                    data["email_address"].encode("utf-8")
                ).hexdigest()

            if self.compute_hash_sha512:
                data["email_address_hash_sha512"] = hashlib.sha512(
                    data["email_address"].encode("utf-8")
                ).hexdigest()

            if self.compute_hash_bcrypt:
                wf = self.bcrypt_salt_work_factor
                data[f"email_address_hash_bcrypt_wf_{wf}"] = bcrypt.hashpw(
                    data["email_address"].encode("utf-8"),
                    bcrypt.gensalt(rounds=self.bcrypt_salt_work_factor),
                ).decode("utf-8")

            # record['data'] has been updated in place.
            final_records.append(record)

        yield OpMsg(
            data=final_records,
            metadata=MailchimpGenericComputeEmailHashesOpMetadataModel(),
            audit=MailchimpGenericComputeEmailHashesOpAuditModel(),
        )
