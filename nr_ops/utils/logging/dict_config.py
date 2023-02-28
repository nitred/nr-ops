from typing import Any, Dict

from pydantic import BaseModel, Field, StrictBool, StrictStr, conint


def default_formatters() -> Dict[str, Any]:
    return {
        "default": {
            "format": (
                "[%(asctime)s][%(filename)-28s:%(lineno)-4d][%(levelname)9s] %(message)s"
            ),
        },
    }


def default_handlers() -> Dict[str, Any]:
    return {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": "DEBUG",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "level": "DEBUG",
            "filename": "logs/all.log",
            "maxBytes": 1024 * 1024,
            "backupCount": 100,
        },
    }


def default_loggers() -> Dict[str, Any]:
    # TODO: Better handing of root and nr_ops and airflow loggers.
    #  - currently, the file has duplicate logs for nr_ops and airflow
    return {
        "": {
            "handlers": ["file"],
            "level": "INFO",
            "propagate": False,
        },
        "airflow": {
            "handlers": ["console", "file"],
            "level": "WARNING",
            "propagate": True,
        },
        "nr_ops": {
            "handlers": ["console", "file"],
            "level": "DEBUG",
            "propagate": False,
        },
    }


class LoggingDictConfigModel(BaseModel):
    version: conint(ge=1) = 1
    disable_existing_loggers: StrictBool = False
    formatters: Dict[StrictStr, Dict[StrictStr, StrictStr]] = Field(
        default_factory=default_formatters
    )
    handlers: Dict[StrictStr, Dict[StrictStr, Any]] = Field(
        default_factory=default_handlers
    )
    loggers: Dict[StrictStr, Dict[StrictStr, Any]] = Field(
        default_factory=default_loggers
    )

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False
