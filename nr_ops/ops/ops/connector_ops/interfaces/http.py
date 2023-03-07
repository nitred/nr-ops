""".
https://googleapis.github.io/google-api-python-client/docs/dyn/analyticsreporting_v4.html
"""
import importlib
import logging
from functools import partial
from typing import Any, Dict, Generator, List, Literal, Optional, Tuple, Union

import backoff
import requests
from pydantic import (
    BaseModel,
    Field,
    StrictBool,
    StrictStr,
    conlist,
    root_validator,
    validator,
)

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel
from nr_ops.ops.ops.connector_ops.hooks.http_requests_from_env import (
    HTTPRequestsHookFromEnvConnOp,
)
from nr_ops.ops.ops.connector_ops.interfaces.base import validate_hook_type_and_config

logger = logging.getLogger(__name__)


class BackoffExpoModel(BaseModel):
    base: float = 2
    factor: float = 2
    max_value: Optional[float] = None

    def wait_gen(self, *args, **kwargs) -> Generator[float, None, None]:
        return backoff.expo(
            base=self.base,
            factor=self.factor,
            max_value=self.max_value,
        )


class BackoffCustomValuesModel(BaseModel):
    values: conlist(Union[None, float], min_items=2) = Field(
        default_factory=lambda: [None, 5, 30, 60, 120, 180]
    )

    @validator("values")
    def validate_values(cls, values):
        if values[0] is not None:
            raise ValueError("First value must be None")

        for value in values[1:]:
            if value is None:
                raise ValueError(
                    "All values after the first value must be floats. "
                    "Only the first value must be None."
                )
        return values

    def wait_gen(self, *args, **kwargs) -> Generator[float, None, None]:
        for value in self.values:
            yield value


class BackoffConfigModel(BaseModel):
    exceptions: conlist(StrictStr, min_items=1) = Field(
        default_factory=lambda: [
            "requests.exceptions.RequestException",
            "urllib3.exceptions.HTTPError",
        ]
    )
    on_exception_kwargs: Dict[str, Any] = Field(
        default_factory=lambda: {"max_time": 400}
    )
    backoff_wait_gen_type: Literal["custom_values", "backoff.expo"] = "custom_values"
    backoff_wait_gen_config: Dict[str, Any] = Field(
        default_factory=lambda: BackoffCustomValuesModel().dict()
    )

    @root_validator
    def validate_backoff_wait_gen_config(cls, values):
        if values.get("backoff_wait_gen_type", None) == "custom_values":
            BackoffCustomValuesModel(**values["backoff_wait_gen_config"])
        elif values.get("backoff_wait_gen_type", None) == "backoff.expo":
            BackoffExpoModel(**values["backoff_wait_gen_config"])
        else:
            raise ValueError(
                f"Invalid backoff_wait_gen_type: "
                f"{values.get('backoff_wait_gen_type', None)}"
            )
        return values


class HTTPConnOpConfigModel(BaseOpConfigModel):
    hook_type: Literal[
        "connector.hooks.http_requests_from_env",
    ]
    hook_config: Dict[StrictStr, Any]

    # NOTE: If user does not specify backoff_config, it will be set to NONE and no
    # backoff config will be enabled. If user specifies backoff_config={} then
    # default backoff config will be enabled!
    backoff_config: Optional[BackoffConfigModel] = None

    # Example backoff_config:
    #   backoff_config:
    #     on_exception_kwargs:
    #       max_time: 400
    #     backoff_wait_gen_type: "custom_values"
    #     backoff_wait_gen_config:
    #       values: [null, 5, 30, 60, 120, 180]

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
        # NOTE: If user does not specify backoff_config, it will be set to NONE and no
        # backoff config will be enabled. If user specifies backoff_config={} then
        # default backoff config will be enabled!
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
        """.

        * This function is called only if backoff_config is not None.
        """
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

        if backoff_model.backoff_wait_gen_type == "custom_values":
            wait_gen = BackoffCustomValuesModel(
                **backoff_model.backoff_wait_gen_config
            ).wait_gen
        elif backoff_model.backoff_wait_gen_type == "backoff.expo":
            wait_gen = BackoffExpoModel(
                **backoff_model.backoff_wait_gen_config
            ).wait_gen
        else:
            raise NotImplementedError()

        return backoff.on_exception(
            wait_gen=wait_gen,
            exception=exception_classes,
            jitter=None,
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

    def __call_logic(
        self, requests_method, url, return_type, requests_kwargs, accepted_status_codes
    ):
        """."""
        response = requests_method(url, **requests_kwargs)

        if response.status_code in accepted_status_codes:
            if return_type == "json":
                return response.status_code, response.json()
            elif return_type == "text":
                return response.status_code, response.text
            else:
                raise NotImplementedError()
        else:
            msg = (
                f"HTTPConnOp.__call_logic: {accepted_status_codes=}, instead received "
                f"{response.status_code=}. Raising HTTPError so that it can be caught "
                f"by the backoff wrapper. response.text:\n{response.text}"
            )
            logger.exception(msg)
            raise requests.exceptions.HTTPError(msg)

    def call(
        self,
        method: Literal["get", "post", "put", "delete", "patch"],
        url: str,
        return_type: Literal["json", "text"],
        requests_kwargs: Optional[Dict[str, Any]] = None,
        accepted_status_codes: Optional[List[int]] = None,
    ) -> Tuple[int, Any]:
        """."""
        logger.info(f"HTTPConnOp.call: Running")

        logger.info(
            f"HTTPConnOp.call: Running for {method=}, {return_type=}, "
            f"{accepted_status_codes=}. Not logging URL since it may contain secrets."
        )
        requests_kwargs = {} if requests_kwargs is None else requests_kwargs
        accepted_status_codes = (
            [200] if accepted_status_codes is None else accepted_status_codes
        )

        requests_method = getattr(requests, method)

        # NOTE: If user does not specify backoff_config, it will be set to NONE and no
        # backoff config will be enabled. If user specifies backoff_config={} then
        # default backoff config will be enabled!
        if self.backoff_config is not None:
            logger.info(f"HTTPConnOp.call: Backoff enabled.")
            call_logic = self.get_backoff_wrapper()(self.__call_logic)
        else:
            logger.info(f"HTTPConnOp.call: Backoff NOT enabled.")
            call_logic = self.__call_logic

        return call_logic(
            requests_method=requests_method,
            url=url,
            return_type=return_type,
            requests_kwargs=requests_kwargs,
            accepted_status_codes=accepted_status_codes,
        )
