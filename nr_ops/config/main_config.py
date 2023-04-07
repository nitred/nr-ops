import importlib.metadata
from typing import Any, Dict, List, Optional

import pkg_resources
from pydantic import BaseModel, Field, StrictStr, validator

from nr_ops.ops.op import OpModel
from nr_ops.utils.logging.dict_config import LoggingDictConfigModel


class MainConfigModel(BaseModel):
    docs: Optional[StrictStr] = None
    app_version: StrictStr
    set_env_vars: Optional[Dict[StrictStr, StrictStr]] = None
    import_env_vars: Optional[List[StrictStr]] = None
    logging: LoggingDictConfigModel = Field(default_factory=LoggingDictConfigModel)
    connector_op: Optional[OpModel] = None
    schedule_op: OpModel
    root_op: OpModel

    @validator("app_version")
    def validate_app_version(cls, app_version):
        """Validate that `app_version` in config file is the nr_ops_version."""
        nr_ops_version = pkg_resources.get_distribution("nr_ops").version
        if app_version != nr_ops_version:
            raise ValueError(
                f"{app_version=} provided in the config file does not match "
                f"{nr_ops_version=}! Please update the config file or use a different "
                f"version of nr-ops."
            )
        return app_version

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False
