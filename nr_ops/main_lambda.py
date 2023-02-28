import base64
import logging
import traceback

import ruamel.yaml

from .main import core

logger = logging.getLogger(__name__)


def run_lambda(b64_config_yaml: str):
    """Entrypoint for the nr_ops package."""
    msg = (
        "Running nr_ops.main.run(), no logging initialized yet. Attempting to "
        "configure default logging..."
    )
    print(msg)
    logger.info(msg)

    # Configure default logging until the user provided logging config is loaded.
    # dictConfig(config=LoggingDictConfigModel().dict())
    logger.info("Configured default logging!")

    try:
        config_str = base64.b64decode(b64_config_yaml)
        config = ruamel.yaml.YAML(typ="safe", pure=True).load(config_str)
        yield from core(config=config, return_generator=True)
    except Exception:
        logger.exception(
            f"Exception occurred in nr_ops.main.core()!\n{traceback.format_exc()}"
        )
        raise

    logger.info("Done running nr_ops.main.run_lambda!")
