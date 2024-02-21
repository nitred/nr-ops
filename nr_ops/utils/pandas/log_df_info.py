import logging
from io import StringIO

import pandas as pd

logger = logging.getLogger(__name__)


def log_df_info(df: pd.DataFrame, log_prefix: str = ""):
    buf = StringIO()
    df.info(buf=buf)
    buf.seek(0)
    logger.info(f"{log_prefix}DataFrame info:\n{buf.read()}")
