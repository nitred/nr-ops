import json
import logging
import pathlib
from typing import Any, Dict, Generator, List, Optional

from pydantic import BaseModel, Field, StrictBool, StrictInt, StrictStr, conlist

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel
from nr_ops.ops.ops.consumer_ops.shell.shell_run import ShellRunConsumerOp

logger = logging.getLogger(__name__)


class DBTKwargsModel(BaseModel):
    project_dir: StrictStr
    profiles_dir: StrictStr
    profile: StrictStr
    target: StrictStr
    vars: Dict[StrictStr, Any] = Field(default_factory=dict)
    threads: StrictInt = 1

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class DBTRunConsumerOpConfigModel(BaseOpConfigModel):
    dbt_kwargs: DBTKwargsModel
    models_include: conlist(StrictStr, min_items=1)
    models_exclude: Optional[conlist(StrictStr, min_items=1)] = None
    log_dbt_logs: StrictBool = False
    log_dbt_run_results: StrictBool = False

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class DBTRunConsumerOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class DBTRunConsumerOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class DBTRunConsumerOp(BaseConsumerOp):
    OP_TYPE = "consumer.dbt.run"
    OP_CONFIG_MODEL = DBTRunConsumerOpConfigModel
    OP_METADATA_MODEL = DBTRunConsumerOpMetadataModel
    OP_AUDIT_MODEL = DBTRunConsumerOpAuditModel

    templated_fields = None

    def __init__(
        self,
        dbt_kwargs: Dict[str, Any],
        models_include: List[str],
        models_exclude: Optional[List[str]] = None,
        log_dbt_logs: bool = False,
        log_dbt_run_results: bool = False,
        **kwargs,
    ):
        self.dbt_kwargs = dbt_kwargs
        self.models_include = models_include
        self.models_exclude = models_exclude if models_exclude else []
        self.log_dbt_logs = log_dbt_logs
        self.log_dbt_run_results = log_dbt_run_results

        self.templated_fields = kwargs.get("templated_fields", [])

        self.dbt_kwargs_model = DBTKwargsModel(**self.dbt_kwargs)

    def run(self, time_step: TimeStep, msg: Optional[OpMsg] = None) -> OpMsg:
        """."""
        logger.info(f"DBTRunConsumerOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=None, log_prefix="DBTRunConsumerOp.run:"
        )

        models_include_str = "--select " + " ".join(self.models_include)

        if self.models_exclude:
            models_exclude_str = "--exclude " + " ".join(self.models_exclude)
        else:
            models_exclude_str = " "

        dbt_run_shell = ShellRunConsumerOp(
            cmd=(
                f"dbt "
                f"--no-anonymous-usage-stats "
                f"run "
                f"--project-dir {self.dbt_kwargs_model.project_dir} "
                f"--profiles-dir {self.dbt_kwargs_model.profiles_dir} "
                f"--profile {self.dbt_kwargs_model.profile} "
                f"--target {self.dbt_kwargs_model.target} "
                f"--vars '{json.dumps(self.dbt_kwargs_model.vars)}' "
                f"--threads {self.dbt_kwargs_model.threads} "
                f"{models_include_str} "
                f"{models_exclude_str} "
            ),
            accepted_exit_codes=[0],
            log_cmd=True,
            log_stdout=True,
        )
        dbt_run_shell.run(time_step=time_step)

        if self.log_dbt_logs:
            logs_dir = pathlib.Path(self.dbt_kwargs_model.project_dir) / "logs"
            logs_files = list(
                sorted([x for x in logs_dir.glob("*") if x.is_file()], reverse=True)
            )
            for log_file in logs_files:
                logger.info(
                    f"DBTRunConsumerOp.run: Logging dbt log file: {str(log_file)}: \n"
                    f"{log_file.read_text()}"
                )

        if self.log_dbt_run_results:
            run_results_json = (
                pathlib.Path(self.dbt_kwargs_model.project_dir)
                / "target"
                / "run_results.json"
            )
            logger.info(
                f"DBTRunConsumerOp.run: Logging run_results_json file: "
                f"{str(run_results_json)}: \n"
                f"{run_results_json.read_text()}"
            )

        return OpMsg(
            data=None,
            metadata=DBTRunConsumerOpMetadataModel(),
            audit=DBTRunConsumerOpAuditModel(),
        )
