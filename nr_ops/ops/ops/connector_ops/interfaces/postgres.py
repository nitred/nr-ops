""".
https://googleapis.github.io/google-api-python-client/docs/dyn/analyticsreporting_v4.html
"""
import logging
from typing import Any, Dict, Generator, List, Literal, Optional

from pydantic import BaseModel, StrictStr, conlist, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel
from nr_ops.ops.ops.connector_ops.interfaces.base import validate_hook_type_and_config

logger = logging.getLogger(__name__)


class PostgresConnOpConfigModel(BaseOpConfigModel):
    hook_type: Literal[
        "connector.hooks.airflow_postgres_hook",
    ]
    hook_config: Dict[StrictStr, Any]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    _validate_hook_type_and_config = root_validator(allow_reuse=True)(
        validate_hook_type_and_config
    )


class PostgresConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PostgresConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class PostgresConnOp(BaseConnectorOp):
    OP_TYPE = "connector.postgres"
    OP_CONFIG_MODEL = PostgresConnOpConfigModel
    OP_METADATA_MODEL = PostgresConnOpMetadataModel
    OP_AUDIT_MODEL = PostgresConnOpAuditModel

    templated_fields = None

    def __init__(self, hook_type: str, hook_config: Dict[str, Any], **kwargs):
        self.hook_type = hook_type
        self.hook_config = hook_config
        self.templated_fields = kwargs.get("templated_fields", [])

        from nr_ops.ops.op_collection import OP_COLLECTION

        self.hook_op = OP_COLLECTION[self.hook_type](**self.hook_config)
        self.hook = self.hook_op.run().data
        self.engine = None

        if self.hook_type == "connector.hooks.airflow_postgres_hook":
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            self.hook: PostgresHook
        else:
            raise NotImplementedError()

    def get_engine(self):
        """."""
        if self.hook_type == "connector.hooks.airflow_postgres_hook":
            if self.engine is None:
                self.engine = self.hook.get_sqlalchemy_engine()

            return self.engine
        else:
            raise NotImplementedError()

    def get_connection(self):
        """."""
        if self.hook_type == "connector.hooks.airflow_postgres_hook":
            return self.hook.get_conn()
        else:
            raise NotImplementedError()

    def run(self) -> OpMsg:
        """."""
        logger.info(f"PostgresConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=None, msg=None, log_prefix="PostgresConnOp.run:")

        if self.hook_type == "connector.hooks.airflow_postgres_hook":
            pass
        else:
            raise NotImplementedError()

        return OpMsg(
            data=self,
            metadata=PostgresConnOpMetadataModel(),
            audit=PostgresConnOpAuditModel(),
        )
