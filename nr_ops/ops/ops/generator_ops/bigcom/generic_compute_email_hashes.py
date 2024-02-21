import hashlib
import logging
import uuid
from typing import Any, Dict, Generator, Optional

import bcrypt
from pydantic import StrictBool, StrictInt, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel

logger = logging.getLogger(__name__)


class BigcomGenericComputeEmailHashesOpConfigModel(BaseOpConfigModel):
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


class BigcomGenericComputeEmailHashesOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BigcomGenericComputeEmailHashesOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class BigcomGenericComputeEmailHashesOp(BaseGeneratorOp):
    """."""

    OP_TYPE = "generator.bigcom.generic.compute_email_hashes"
    OP_CONFIG_MODEL = BigcomGenericComputeEmailHashesOpConfigModel
    OP_METADATA_MODEL = BigcomGenericComputeEmailHashesOpMetadataModel
    OP_AUDIT_MODEL = BigcomGenericComputeEmailHashesOpAuditModel

    templated_fields = None

    def __init__(
        self,
        compute_hash_md5: bool = True,
        compute_hash_uuid5: bool = True,
        compute_hash_sha256: bool = True,
        compute_hash_sha512: bool = True,
        compute_hash_bcrypt: bool = False,
        bcrypt_salt_work_factor: int = 4,
        **kwargs,
    ):
        """."""
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
        logger.info(f"BigcomGenericComputeEmailHashesOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step,
            msg=msg,
            log_prefix="BigcomGenericComputeEmailHashesOp.run:",
        )

        input_records = msg.data

        if not isinstance(input_records, list):
            raise NotImplementedError("Input records must be a list of dicts.")

        final_records = []

        def compute_hashes_and_update_in_place(payload: Dict[str, Any], email_key: str):
            if self.compute_hash_md5:
                payload["email_address_hash_md5"] = hashlib.md5(
                    payload[email_key].encode("utf-8")
                ).hexdigest()

            if self.compute_hash_uuid5:
                payload["email_address_hash_uuid5"] = str(
                    uuid.uuid5(uuid.NAMESPACE_URL, payload[email_key])
                )

            if self.compute_hash_sha256:
                payload["email_address_hash_sha256"] = hashlib.sha256(
                    payload[email_key].encode("utf-8")
                ).hexdigest()

            if self.compute_hash_sha512:
                payload["email_address_hash_sha512"] = hashlib.sha512(
                    payload[email_key].encode("utf-8")
                ).hexdigest()

            if self.compute_hash_bcrypt:
                wf = self.bcrypt_salt_work_factor
                payload[f"email_address_hash_bcrypt_wf_{wf}"] = bcrypt.hashpw(
                    payload[email_key].encode("utf-8"),
                    bcrypt.gensalt(rounds=self.bcrypt_salt_work_factor),
                ).decode("utf-8")

        for record in input_records:
            email_exists = False

            if "email" in record["data"]:
                email_exists = True
                compute_hashes_and_update_in_place(
                    payload=record["data"], email_key="email"
                )

            if "billing_address" in record["data"]:
                if "email" in record["data"]["billing_address"]:
                    email_exists = True
                    compute_hashes_and_update_in_place(
                        payload=record["data"]["billing_address"], email_key="email"
                    )

            if not email_exists:
                raise ValueError(
                    f"BigcomGenericComputeEmailHashesOp.run: "
                    f"No email address found in record."
                )

            # record['data'] has been updated in place.
            final_records.append(record)

        yield OpMsg(
            data=final_records,
            metadata=BigcomGenericComputeEmailHashesOpMetadataModel(),
            audit=BigcomGenericComputeEmailHashesOpAuditModel(),
        )
