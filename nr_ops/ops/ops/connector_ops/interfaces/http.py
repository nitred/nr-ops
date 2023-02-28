""".
https://googleapis.github.io/google-api-python-client/docs/dyn/analyticsreporting_v4.html
"""
import importlib
import logging
from functools import partial
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple

import backoff
import requests
from pydantic import BaseModel, Field, StrictBool, StrictStr, conlist, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel
from nr_ops.ops.ops.connector_ops.hooks.http_requests_from_env import (
    HTTPRequestsHookFromEnvConnOp,
)
from nr_ops.ops.ops.connector_ops.interfaces.base import validate_hook_type_and_config

logger = logging.getLogger(__name__)


class BackoffExpoKwargsModel(BaseModel):
    base: float = 2
    factor: float = 1.5
    max_value: Optional[float] = None


class BackoffConfigModel(BaseModel):
    exceptions: conlist(StrictStr, min_items=1) = Field(
        default_factory=lambda: [
            "requests.exceptions.RequestException",
            "urllib3.exceptions.HTTPError",
        ]
    )
    on_exception_kwargs: Dict[str, Any] = Field(
        default_factory=lambda: {"max_time": 300}
    )
    expo_kwargs: BackoffExpoKwargsModel = Field(default_factory=BackoffExpoKwargsModel)


class HTTPConnOpConfigModel(BaseOpConfigModel):
    hook_type: Literal[
        "connector.hooks.http_requests_from_env",
    ]
    hook_config: Dict[StrictStr, Any]
    backoff_config: Optional[BackoffConfigModel] = Field(
        default_factory=BackoffConfigModel
    )

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    _validate_hook_type_and_config = root_validator(allow_reuse=True)(
        validate_hook_type_and_config
    )


class HTTPConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class HTTPConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class HTTPConnOp(BaseConnectorOp):
    OP_TYPE = "connector.http"
    OP_CONFIG_MODEL = HTTPConnOpConfigModel
    OP_METADATA_MODEL = HTTPConnOpMetadataModel
    OP_AUDIT_MODEL = HTTPConnOpAuditModel

    templated_fields = None

    def __init__(
        self,
        hook_type: str,
        hook_config: Dict[str, Any],
        backoff_config: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self.hook_type = hook_type
        self.hook_config = hook_config
        self.backoff_config = backoff_config

        self.templated_fields = kwargs.get("templated_fields", [])

        from nr_ops.ops.op_collection import OP_COLLECTION

        self.hook_op = OP_COLLECTION[self.hook_type](**self.hook_config)
        self.hook = self.hook_op.run().data

        if self.hook_type == "connector.hooks.http_requests_from_env":
            self.hook: HTTPRequestsHookFromEnvConnOp
        else:
            raise NotImplementedError()

    def get_backoff_wrapper(self):
        """."""
        if self.backoff_config:
            backoff_model = BackoffConfigModel(**self.backoff_config)
            exception_strs = (
                ["requests.exceptions.RequestException"]
                if backoff_model.exceptions is None
                else backoff_model.exceptions
            )
            exception_classes = []
            for exception_str in exception_strs:
                exception_split = exception_str.split(".")
                if len(exception_split) == 1:
                    exception_module = "builtins"
                    exception_name = exception_split[0]
                else:
                    exception_module = ".".join(exception_split[:-1])
                    exception_name = exception_split[-1]

                exception_class = getattr(
                    importlib.import_module(exception_module), exception_name
                )
                exception_classes.append(exception_class)

            exception_classes = tuple(exception_classes)

            backoff_expo = partial(backoff.expo, **backoff_model.expo_kwargs.dict())

            return backoff.on_exception(
                wait_gen=backoff_expo,
                exception=exception_classes,
                **backoff_model.on_exception_kwargs,
            )

    def run(self) -> OpMsg:
        """."""
        logger.info(f"HTTPConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(time_step=None, msg=None, log_prefix="HTTPConnOp.run:")

        if self.hook_type == "connector.hooks.http_requests_from_env":
            pass
        else:
            raise NotImplementedError()

        return OpMsg(
            data=self,
            metadata=HTTPConnOpMetadataModel(),
            audit=HTTPConnOpAuditModel(),
        )

    ####################################################################################
    # PROPERTIES
    ####################################################################################
    @property
    def base_url(self) -> str:
        if self.hook_type == "connector.hooks.http_requests_from_env":
            # Base URL guaranteed to NOT have a trailing slash by hook.
            base_url = self.hook.base_url
        else:
            raise NotImplementedError()

        return base_url

    @property
    def username(self) -> str:
        if self.hook_type == "connector.hooks.http_requests_from_env":
            # Base URL guaranteed to NOT have a trailing slash by hook.
            username = self.hook.username
        else:
            raise NotImplementedError()

        return username

    @property
    def password(self) -> str:
        if self.hook_type == "connector.hooks.http_requests_from_env":
            # Base URL guaranteed to NOT have a trailing slash by hook.
            password = self.hook.password
        else:
            raise NotImplementedError()

        return password

    ####################################################################################
    # METHODS
    ####################################################################################
    def call(
        self,
        method: Literal["get", "post", "put", "delete", "patch"],
        url: str,
        return_type: Literal["json", "text"],
        requests_kwargs: Optional[Dict[str, Any]] = None,
        accepted_status_codes: Optional[List[int]] = None,
    ) -> Tuple[int, Any]:
        """."""
        requests_kwargs = {} if requests_kwargs is None else requests_kwargs
        accepted_status_codes = (
            [200] if accepted_status_codes is None else accepted_status_codes
        )

        requests_method = getattr(requests, method)

        if self.backoff_config:
            requests_method = self.get_backoff_wrapper()(requests_method)

        response = requests_method(url, **requests_kwargs)

        if response.status_code in accepted_status_codes:
            if return_type == "json":
                return response.status_code, response.json()
            elif return_type == "text":
                return response.status_code, response.text
            else:
                raise NotImplementedError()
        else:
            # TODO: Consider raising HTTPError here in case status code is wrong so that
            #  it can be handled downstream.
            raise ValueError(
                f"HTTPConnOp.get: Unexpected status code: {response.status_code}. "
                f"Response: {response.text}"
            )
