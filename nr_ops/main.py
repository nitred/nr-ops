import argparse
import base64
import logging
import traceback
from logging.config import dictConfig
from typing import Optional

import ruamel.yaml

from nr_ops.config.main_config import MainConfigModel
from nr_ops.messages.op_depth import BaseOpDepthModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.op import Op
from nr_ops.ops.op_manager import init_global_op_manager
from nr_ops.utils.logging.dict_config import LoggingDictConfigModel

logger = logging.getLogger(__name__)


def core(
    config,
    return_generator: bool = False,
    root_msg: Optional[OpMsg] = None,
):
    """Run core logic after logging has been initialized."""

    # Validate config
    logger.info("Validating config against MainConfigModel...")
    config_model = MainConfigModel(**config)
    config_model.root_op.validate_unique_ids()
    logger.info("Done validating config against MainConfigModel...")

    # Configure user provided logging.
    logger.info("Configuring user provided logging config...")
    while logger.handlers:
        logger.handlers.pop()

    dictConfig(config=config_model.logging.dict())
    logger.info("Configured user provided logging!")

    # Start with nr_ops...

    # Global op manager needs to be initialized before any ops are created.
    op_manager = init_global_op_manager()
    op_manager.import_env_vars(env_vars=config_model.import_env_vars)

    root_depth = BaseOpDepthModel(op_type_depth="root", op_id_depth="root")

    # ----------------------------------------------------------------------------------
    # Initialize Connectors (if any)
    # ----------------------------------------------------------------------------------
    if config_model.connector_op is not None:
        connector = Op(**config_model.connector_op.dict())
        for _ in connector.run(depth=root_depth):
            pass

    schedule_op = Op(**config_model.schedule_op.dict())

    root_op = Op(**config["root_op"])

    logger.info(f"main: Running schedule_op")
    for time_step_index, time_step_msg in enumerate(schedule_op.run(depth=root_depth)):
        time_step = time_step_msg.data
        time_step.index = time_step_index

        logger.info(f"{'#' * 80}")
        logger.info(f"main: Running root_op for {time_step=}")
        logger.info(f"{'#' * 80}")

        if return_generator:
            # This can be used by a server to collect the results (if there's any
            # results)
            return root_op.run(
                depth=root_depth,
                time_step=time_step,
                msg=root_msg,
            )
        else:
            for _msg in root_op.run(
                depth=root_depth,
                time_step=time_step,
                msg=root_msg,
            ):
                # This exhausts the generator of `root_op` which is the core behavior
                # of nr-ops CLI.
                pass

    logger.info("Done running nr_ops.main.core!")


def run():
    """Main entrypoint for the nr_ops package."""
    msg = (
        "Running nr_ops.main.run(), no logging initialized yet. Attempting to "
        "configure default logging..."
    )
    print(msg)
    logger.info(msg)

    # Configure default logging until the user provided logging config is loaded.
    dictConfig(config=LoggingDictConfigModel().dict())
    logger.info("Configured default logging...")

    try:
        # Parse command line arguments.
        parser = argparse.ArgumentParser()
        parser.add_argument("--config", help="Path to the config file.")
        args = parser.parse_args()
        logger.info("Parsed CLI arguments...")

        # Read config
        yaml = ruamel.yaml.YAML(typ="safe", pure=True)

        with open(args.config, "r") as f:
            config = yaml.load(f)

        logger.info("Parsed config...")
        logger.info("Running core...")
        core(config=config)
        logger.info("Done running core...")
    except Exception:
        logger.exception(
            f"Exception occurred in nr_ops.main.core()!\n{traceback.format_exc()}"
        )
        raise

    logger.info("Done running nr_ops.main.run!")
