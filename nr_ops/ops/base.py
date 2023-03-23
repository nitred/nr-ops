from __future__ import annotations

import abc
import builtins
import copy
import logging
from typing import Any, Dict, Generator, List, Literal, Optional, Type, Union

import jinja2
import jinja2.meta
from pydantic import BaseModel, StrictBool, StrictStr, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg, OpTimeStepMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.utils.eval.eval_globals import EVAL_GLOBALS

logger = logging.getLogger(__name__)


class RecursiveTemplateConfigModel(BaseModel):
    match_type: Literal["at_least_one", "all"]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class FieldTemplateConfigModel(BaseModel):
    field: StrictStr
    is_secret: StrictBool = False
    is_recursive: StrictBool = False
    recursive_config: Optional[RecursiveTemplateConfigModel] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    @root_validator(pre=False)
    def validate_recursive_config(cls, values):
        if values.get("is_recursive") and not values.get("recursive_config"):
            raise ValueError("is_recursive is True, but no recursive_config provided")
        return values


class BaseOpConfigModel(BaseModel):
    """."""

    templated_fields: Optional[List[FieldTemplateConfigModel]] = None

    def get_self_and_child_op_ids(self) -> Generator[str, None, None]:
        """.

        NOTE: This method should be re-implemented by any Op that is from the
        `group` OP_FAMILY.
        """
        return
        yield

    @staticmethod
    def __is_templated_field_str(field: str, field_value: str) -> bool:
        """."""
        try:
            # source: https://jinja.palletsprojects.com/en/3.0.x/api/#jinja2.meta.find_undeclared_variables
            ast = jinja2.Environment().parse(source=field_value)
            vars_in_field_value = jinja2.meta.find_undeclared_variables(ast)
        except jinja2.exceptions.TemplateSyntaxError:
            logger.exception(
                f"BaseOpConfigModel.__is_templated_field_str: A TemplateSyntaxError "
                f"occurred when parsing: {field=}, {field_value=}. "
            )
            raise
        if len(vars_in_field_value) == 0:
            return False
        else:
            return True

    # A staticmethod that recursively loops through a list or dicts and calls the
    # __validate_templated_field_str method on each value that is of type str and calls
    # itself on each value that is of type list or dict.
    @staticmethod
    def __validate_templated_field_recursive(
        field: str,
        field_value: Union[List[Any], Dict[Any, Any]],
        n_templated_fields_found: int,
        n_templated_fields_validated: int,
    ) -> (int, int):
        """."""

        def _next_field_value_gen(field, field_value):
            if isinstance(field_value, list):
                for i, _value in enumerate(field_value):
                    yield f"{field}[{i}]", _value
            elif isinstance(field_value, dict):
                for key, _value in field_value.items():
                    yield f"{field}.{key}", _value
            elif isinstance(field_value, BaseModel):
                for key, _value in field_value.dict().items():
                    yield f"{field}.{key}", _value
            else:
                raise NotImplementedError()

        for next_field, next_value in _next_field_value_gen(field, field_value):
            if isinstance(next_value, str):
                is_validated = BaseOpConfigModel.__is_templated_field_str(
                    field=next_field, field_value=next_value
                )
                n_templated_fields_found += 1
                if is_validated:
                    n_templated_fields_validated += 1
            elif isinstance(next_value, (list, dict, BaseModel)):
                (
                    n_templated_fields_found,
                    n_templated_fields_validated,
                ) = BaseOpConfigModel.__validate_templated_field_recursive(
                    field=next_field,
                    field_value=next_value,
                    n_templated_fields_found=n_templated_fields_found,
                    n_templated_fields_validated=n_templated_fields_validated,
                )
            else:
                pass

        return (
            n_templated_fields_found,
            n_templated_fields_validated,
        )

    @root_validator(pre=False)
    def validate_templated_fields(cls, values):
        """."""
        templated_fields = values.get("templated_fields", None)
        if templated_fields is None:
            templated_fields = []

        for field_config in templated_fields:
            if isinstance(field_config, dict):
                field_config = FieldTemplateConfigModel(**field_config)
            elif isinstance(field_config, FieldTemplateConfigModel):
                pass
            else:
                raise NotImplementedError()

            field = field_config.field
            field_value = values.get(field, None)

            # In case we use nested pydantic models we need to convert them to dicts.
            # This is just for validation of strings, so we don't need to worry having
            # data types being messed up. Non string values will be skipped anyway.
            if isinstance(field_value, BaseModel):
                field_value = field_value.dict()

            if field_config.is_recursive:
                recursive_config = field_config.recursive_config
                if recursive_config is None:
                    raise ValueError(
                        f"validate_templated_fields: The `is_recursive` flag is set to "
                        f"True for the {field=} but `recursive_config` is None. A "
                        f"recursive_config is required when `is_recursive` is True."
                    )

                if isinstance(recursive_config, dict):
                    recursive_config = RecursiveTemplateConfigModel(**recursive_config)
                elif isinstance(recursive_config, RecursiveTemplateConfigModel):
                    pass
                else:
                    raise NotImplementedError()
            else:
                recursive_config = None

            if field == "templated_fields":
                raise ValueError(
                    f"validate_templated_fields: Cannot use `templated_fields` as a "
                    f"templated field. Please remove this from the config."
                )
            elif field not in values:
                raise ValueError(
                    f"validate_templated_fields: {field=} is not defined in the model "
                    f"but was added as one of templated_field. "
                    f"Please remove this from the config."
                )
            elif isinstance(field_value, str):
                is_validated = BaseOpConfigModel.__is_templated_field_str(
                    field=field, field_value=field_value
                )
                if not is_validated:
                    raise ValueError(
                        f"validate_templated_fields: {field=} is in the list of "
                        f"`templated_fields` but it does not have any templated "
                        f"variables inside its {field_value=}."
                    )
            elif isinstance(field_value, (list, dict)):
                if not field_config.is_recursive:
                    raise ValueError(
                        f"validate_templated_fields: {field=} has a field_value of "
                        f"type list or dict ({type(field_value)=}) but `is_recursive` "
                        f"flag is set to False. Please set `is_recursive` to True if "
                        f"you want to template this field_value."
                    )

                (
                    n_templated_fields_found,
                    n_templated_fields_validated,
                ) = BaseOpConfigModel.__validate_templated_field_recursive(
                    field=field,
                    field_value=field_value,
                    n_templated_fields_found=0,
                    n_templated_fields_validated=0,
                )

                if (recursive_config.match_type == "all") and (
                    n_templated_fields_validated != n_templated_fields_found
                ):
                    raise ValueError(
                        f"validate_templated_fields: {field=} is in the list of "
                        f"`templated_fields` with `is_recursive` set to True and "
                        f"{recursive_config.match_type=} and "
                        f"{n_templated_fields_validated=} and "
                        f"{n_templated_fields_found=} for the {field_value=}. Not ALL "
                        f"strings found in the templated field recursively are "
                        f"templated!"
                    )
                elif (recursive_config.match_type == "at_least_one") and (
                    n_templated_fields_validated == 0
                ):
                    raise ValueError(
                        f"validate_templated_fields: {field=} is in the list of "
                        f"`templated_fields` with `is_recursive` set to True and "
                        f"{recursive_config.match_type=} and "
                        f"{n_templated_fields_validated=} and "
                        f"{n_templated_fields_found=} for the {field_value=}. At least "
                        f"one string found in the templated field recursively are "
                        f"must be templated but None were found!"
                    )
                else:
                    pass
            else:
                raise NotImplementedError(
                    f"validate_templated_fields: {field=} of type {type(field_value)=} "
                    f"which is not supported."
                )

        return values


class BaseOp(abc.ABC):
    @property
    @classmethod
    @abc.abstractmethod
    def OP_FAMILY(cls) -> Literal["input_only", "output_only", "input_output"]:
        raise NotImplementedError()

    @property
    @classmethod
    @abc.abstractmethod
    def OP_TYPE(cls) -> str:
        raise NotImplementedError()

    @property
    @classmethod
    @abc.abstractmethod
    def OP_CONFIG_MODEL(cls) -> Type[BaseOpConfigModel]:
        raise NotImplementedError()

    @property
    @classmethod
    @abc.abstractmethod
    def OP_METADATA_MODEL(cls) -> Type[BaseOpMetadataModel]:
        raise NotImplementedError()

    @property
    @classmethod
    @abc.abstractmethod
    def OP_AUDIT_MODEL(cls) -> Type[BaseOpAuditModel]:
        raise NotImplementedError()

    # TODO: Should this be exposed to the user or should be take care of this in the
    #  background implicitly?
    # @property
    # @abc.abstractmethod
    # def raw_fields(self) -> Optional[Any]:
    #     raise NotImplementedError()

    @property
    @abc.abstractmethod
    def templated_fields(self) -> Optional[Any]:
        raise NotImplementedError()

    def __render_field_str(
        self,
        field: str,
        field_value: str,
        field_config: Any,
        op_manager: Any,
        rendered_fields: Any,
        time_step: Optional[TimeStep],
        msg: Optional[OpMsg],
        log_prefix: str = "",
    ):
        """."""
        ast = jinja2.Environment().parse(source=field_value)
        vars_in_field_value = jinja2.meta.find_undeclared_variables(ast)

        if len(vars_in_field_value) == 0:
            logger.info(f"{log_prefix} NOT Rendering field, no vars found: {field=}")
            return field_value
        else:
            pass

        logger.info(f"{log_prefix} Rendering field: {field=}")
        rendered_field_value = jinja2.Template(field_value).render(
            **{
                "builtins": builtins,
                "this_op": self.__dict__,
                "op_manager": op_manager,
                "time_step": time_step,
                "msg": msg,
                "rendered_fields": rendered_fields,
                **EVAL_GLOBALS,
            }
        )
        if field_config.is_secret:
            logger.info(f"{log_prefix} Rendered {field=}: rendered_field_value=******")
        else:
            logger.info(f"{log_prefix} Rendered {field=}: {rendered_field_value=}")
        return rendered_field_value

    def __render_field_recursive(
        self,
        field: str,
        field_value: Any,
        field_config: Any,
        op_manager: Any,
        rendered_fields: Any,
        time_step: Optional[TimeStep],
        msg: Optional[OpMsg],
        log_prefix: str = "",
    ):
        """."""
        if isinstance(field_value, list):
            rendered_list = [
                self.__render_field_recursive(
                    field=f"{field}[{i}]",
                    field_value=next_field_value,
                    field_config=field_config,
                    op_manager=op_manager,
                    rendered_fields=rendered_fields,
                    time_step=time_step,
                    msg=msg,
                    log_prefix=log_prefix,
                )
                for i, next_field_value in enumerate(field_value)
            ]
            return rendered_list

        elif isinstance(field_value, dict):
            rendered_dict = {
                key: self.__render_field_recursive(
                    field=f"{field}.{key}",
                    field_value=next_field_value,
                    field_config=field_config,
                    op_manager=op_manager,
                    rendered_fields=rendered_fields,
                    time_step=time_step,
                    msg=msg,
                    log_prefix=log_prefix,
                )
                for key, next_field_value in field_value.items()
            }
            return rendered_dict

        elif isinstance(field_value, BaseModel):
            rendered_model = type(field_value)(
                **{
                    key: self.__render_field_recursive(
                        field=f"{field}.{key}",
                        field_value=next_field_value,
                        field_config=field_config,
                        op_manager=op_manager,
                        rendered_fields=rendered_fields,
                        time_step=time_step,
                        msg=msg,
                        log_prefix=log_prefix,
                    )
                    for key, next_field_value in field_value.dict().items()
                }
            )
            return rendered_model

        elif isinstance(field_value, str):
            return self.__render_field_str(
                field=field,
                field_value=field_value,
                field_config=field_config,
                op_manager=op_manager,
                rendered_fields=rendered_fields,
                time_step=time_step,
                msg=msg,
                log_prefix=log_prefix,
            )
        else:
            pass

    def render_fields(
        self, time_step: Optional[TimeStep], msg: Optional[OpMsg], log_prefix: str = ""
    ) -> Dict[str, str]:
        """Render all fields from templated fields.

        The templates are rendered using the `self` object as the context i.e.
        the templates are the attributes of the `self` object which are defined in the
        templated_fields list.

        We rerender the fields for every time_step and msg. This is because the
        rendered fields may depend on the time_step and msg.

        We store the original raw fields (as provided by the user) in the raw_fields
        attribute. This is so that we can re-render the fields if the time_step or msg
        changes.
        """
        logger.info(f"{log_prefix} Rendering all templated_fields")

        if self.templated_fields is None:
            raise ValueError(
                f"{log_prefix}: {self.OP_TYPE=} has not handled templated_fields in "
                f"its __init__ method."
            )

        # TODO: Should the raw_fields be exposed to the user or is the current behavior
        #  of taking care of it implicitly in the background okay?
        # Initialize raw fields
        if not hasattr(self, "raw_fields"):
            self.raw_fields = {}
            for field_config in self.templated_fields:
                field_config = FieldTemplateConfigModel(**field_config)
                field = field_config.field
                field_value = getattr(self, field)
                # NOTE: Absolutely necessary to use deepcopy here.
                # NOTE: We are *almost* guaranteed that only JSON serializable types
                # are used in the templated fields.
                self.raw_fields[field] = copy.deepcopy(field_value)

        from nr_ops.ops.op_manager import get_global_op_manager

        op_manager = get_global_op_manager()
        rendered_fields = {}
        for field_config in self.templated_fields:
            field_config = FieldTemplateConfigModel(**field_config)
            if field_config.is_recursive:
                field = field_config.field
                # NOTE: Absolutely necessary to use deepcopy here.
                # Use raw_fields[field] instead of getattr(self, field)
                field_value = copy.deepcopy(self.raw_fields[field])
                rendered_fields[field] = self.__render_field_recursive(
                    field=field,
                    field_value=field_value,
                    field_config=field_config,
                    op_manager=op_manager,
                    rendered_fields=rendered_fields,
                    time_step=time_step,
                    msg=msg,
                    log_prefix=log_prefix,
                )
            else:
                field = field_config.field
                # NOTE: Absolutely necessary to use deepcopy here.
                # Use raw_fields[field] instead of getattr(self, field)
                field_value = copy.deepcopy(self.raw_fields[field])
                rendered_fields[field] = self.__render_field_str(
                    field=field,
                    field_value=field_value,
                    field_config=field_config,
                    op_manager=op_manager,
                    rendered_fields=rendered_fields,
                    time_step=time_step,
                    msg=msg,
                    log_prefix=log_prefix,
                )

        # ------------------------------------------------------------------------------
        # WARNING: THIS IS A MAJOR HACK/FEATURE THAT CAN HAVE UNEXPECTED SIDE EFFECTS
        # ------------------------------------------------------------------------------
        for field, rendered_field_value in rendered_fields.items():
            setattr(self, field, rendered_field_value)

        logger.info(f"{log_prefix} Done rendering all templated_fields")

        return rendered_fields


class BaseGeneratorOp(BaseOp, abc.ABC):
    OP_FAMILY: Literal["generator"] = "generator"

    @abc.abstractmethod
    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        raise NotImplementedError()


class BaseConsumerOp(BaseOp, abc.ABC):
    OP_FAMILY: Literal["consumer"] = "consumer"

    @abc.abstractmethod
    def run(self, time_step: TimeStep, msg: OpMsg) -> OpMsg:  # Return not Yield
        raise NotImplementedError()


class BaseGroupOp(BaseOp, abc.ABC):
    OP_FAMILY: Literal["group"] = "group"

    @abc.abstractmethod
    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[Optional[OpMsg], None, None]:
        raise NotImplementedError()


class BaseTimeStepOp(BaseOp, abc.ABC):
    OP_FAMILY: Literal["time_step"] = "time_step"

    @abc.abstractmethod
    def run(self) -> Generator[OpTimeStepMsg, None, None]:
        raise NotImplementedError()


class BaseConnectorOp(BaseOp, abc.ABC):
    OP_FAMILY: Literal["connector"] = "connector"

    @abc.abstractmethod
    def run(self) -> OpMsg:  # Return not Yield
        raise NotImplementedError()
