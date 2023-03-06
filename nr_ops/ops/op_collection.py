import collections
import logging
from typing import Dict, Iterable, List, Type

from nr_ops.ops.base import BaseOp

# --------------------------------------------------------------------------------------
# connector_ops (Hooks)
# --------------------------------------------------------------------------------------
from nr_ops.ops.ops.connector_ops.hooks.airflow_gcp_hook import AirflowGCPHookConnOp
from nr_ops.ops.ops.connector_ops.hooks.airflow_mysql_hook import AirflowMysqlHookConnOp
from nr_ops.ops.ops.connector_ops.hooks.airflow_postgres_hook import (
    AirflowPostgresHookConnOp,
)
from nr_ops.ops.ops.connector_ops.hooks.airflow_s3_hook import AirflowS3HookConnOp
from nr_ops.ops.ops.connector_ops.hooks.gcp_service_account_from_file import (
    GCPServiceAccountFromFileConnOp,
)
from nr_ops.ops.ops.connector_ops.hooks.http_requests_from_env import (
    HTTPRequestsHookFromEnvConnOp,
)
from nr_ops.ops.ops.connector_ops.hooks.python_list import PythonListHookConnOp

# --------------------------------------------------------------------------------------
# connector_ops (Interfaces)
# --------------------------------------------------------------------------------------
from nr_ops.ops.ops.connector_ops.interfaces.google_analytics import (
    GoogleAnalyticsConnOp,
)
from nr_ops.ops.ops.connector_ops.interfaces.google_analytics_ga4 import (
    GoogleAnalyticsGA4ConnOp,
)
from nr_ops.ops.ops.connector_ops.interfaces.http import HTTPConnOp
from nr_ops.ops.ops.connector_ops.interfaces.list import ListConnOp
from nr_ops.ops.ops.connector_ops.interfaces.mysql import MysqlConnOp
from nr_ops.ops.ops.connector_ops.interfaces.postgres import PostgresConnOp
from nr_ops.ops.ops.connector_ops.interfaces.s3 import S3ConnOp

# --------------------------------------------------------------------------------------
# consumer_ops
# --------------------------------------------------------------------------------------
from nr_ops.ops.ops.consumer_ops.mock import MockConsumerOp
from nr_ops.ops.ops.consumer_ops.pangres.df_to_sql_db import PangresDFToSQLDBOp
from nr_ops.ops.ops.consumer_ops.put_list import PutListConsumerOp
from nr_ops.ops.ops.consumer_ops.s3.put_key import S3PutKeyOp
from nr_ops.ops.ops.consumer_ops.sql_query import SQLQueryConsumerOp

# --------------------------------------------------------------------------------------
# generator_ops
# --------------------------------------------------------------------------------------
from nr_ops.ops.ops.generator_ops.blade.get_token import BladeGetTokenOp
from nr_ops.ops.ops.generator_ops.blade.orders_list_goodsout import (
    BladeOrdersListGoodsoutOp,
)
from nr_ops.ops.ops.generator_ops.blade.orders_list_metadata import (
    BladeOrdersListMetadataOp,
)
from nr_ops.ops.ops.generator_ops.blade.orders_list_tracking_numbers import (
    BladeOrdersBulkListTrackingNumbersOp,
)
from nr_ops.ops.ops.generator_ops.blade.orders_view_goodsout import (
    BladeOrdersViewGoodsoutOp,
)
from nr_ops.ops.ops.generator_ops.blade.products_list_variations import (
    BladeProductsListVariationsOp,
)
from nr_ops.ops.ops.generator_ops.blade.products_view_variation import (
    BladeProductsViewVariationOp,
)
from nr_ops.ops.ops.generator_ops.compression.compress import CompressOp
from nr_ops.ops.ops.generator_ops.compression.decompress import DecompressOp
from nr_ops.ops.ops.generator_ops.eval_expr.eval_expr import EvalExprOp
from nr_ops.ops.ops.generator_ops.eval_expr.eval_expr_as_metadata import (
    EvalExprAsMetadataOp,
)
from nr_ops.ops.ops.generator_ops.get_list import GetListGeneratorOp
from nr_ops.ops.ops.generator_ops.google.get_ga_reports import GetGAReportsOp
from nr_ops.ops.ops.generator_ops.google.get_ga_reports_ga4 import GetGAReportsGA4Op
from nr_ops.ops.ops.generator_ops.mock import MockGeneratorOp
from nr_ops.ops.ops.generator_ops.s3.get_key import S3GetKeyOp
from nr_ops.ops.ops.generator_ops.s3.list_keys import S3ListKeysOp
from nr_ops.ops.ops.generator_ops.sleep import SleepGeneratorOp

# --------------------------------------------------------------------------------------
# group_ops (Groups)
# --------------------------------------------------------------------------------------
from nr_ops.ops.ops.group_ops.op_chain import OpChainGroupOp
from nr_ops.ops.ops.group_ops.op_set import OpSetGroupOp
from nr_ops.ops.ops.time_step_ops.date_range import DateRangeTimeStepOp

# --------------------------------------------------------------------------------------
# time_step_ops (TimeSteps)
# --------------------------------------------------------------------------------------
from nr_ops.ops.ops.time_step_ops.mock_ts import MockTimeStepOp
from nr_ops.ops.ops.time_step_ops.simple_start_offset import SimpleStartOffsetTimeStepOp
from nr_ops.ops.ops.time_step_ops.simple_ts import SimpleTimeStepOp

logger = logging.getLogger(__name__)
OP_CLASSES: List[Type[BaseOp]] = [
    # connector_ops (Hooks)
    AirflowGCPHookConnOp,
    AirflowPostgresHookConnOp,
    AirflowMysqlHookConnOp,
    GCPServiceAccountFromFileConnOp,
    AirflowS3HookConnOp,
    HTTPRequestsHookFromEnvConnOp,
    PythonListHookConnOp,
    # connector_ops (Interfaces)
    GoogleAnalyticsConnOp,
    GoogleAnalyticsGA4ConnOp,
    PostgresConnOp,
    MysqlConnOp,
    S3ConnOp,
    HTTPConnOp,
    ListConnOp,
    # consumer_ops
    MockConsumerOp,
    PangresDFToSQLDBOp,
    SQLQueryConsumerOp,
    S3PutKeyOp,
    PutListConsumerOp,
    # generator_ops
    EvalExprOp,
    EvalExprAsMetadataOp,
    GetGAReportsOp,
    GetGAReportsGA4Op,
    MockGeneratorOp,
    SleepGeneratorOp,
    S3ListKeysOp,
    S3GetKeyOp,
    CompressOp,
    DecompressOp,
    BladeGetTokenOp,
    BladeOrdersListGoodsoutOp,
    BladeOrdersViewGoodsoutOp,
    BladeOrdersListMetadataOp,
    BladeOrdersBulkListTrackingNumbersOp,
    BladeProductsListVariationsOp,
    BladeProductsViewVariationOp,
    GetListGeneratorOp,
    # group_ops (Groups)
    OpChainGroupOp,
    OpSetGroupOp,
    # time_step_ops (TimeSteps)
    MockTimeStepOp,
    SimpleTimeStepOp,
    SimpleStartOffsetTimeStepOp,
    DateRangeTimeStepOp,
]


def validate_duplicate_op_types(op_classes: Iterable[Type[BaseOp]]):
    """."""
    op_type_counts = collections.Counter([op_class.OP_TYPE for op_class in op_classes])
    duplicate_op_types = [
        op_type for op_type, count in op_type_counts.items() if count > 1
    ]
    if duplicate_op_types:
        raise ValueError(
            f"validate_duplicate_op_types: The following op_types have been duplicated "
            f"{duplicate_op_types=}. Please ensure that all op_types are unique."
        )


validate_duplicate_op_types(OP_CLASSES)


OP_COLLECTION: Dict[str, Type[BaseOp]] = {}  # type: ignore

for op_class in OP_CLASSES:
    OP_COLLECTION[op_class.OP_TYPE] = op_class  # type: ignore

# ----------------------------------------------------------------------------------
# ALIASES
# ----------------------------------------------------------------------------------
ALIAS_COLLECTION: Dict[str, Type[BaseOp]] = {
    "group.op_list": OpChainGroupOp,
}

for alias, op_class in ALIAS_COLLECTION.items():
    if alias in OP_COLLECTION:
        raise ValueError(
            f"ALIAS_COLLECTION: The {alias=} is already in use. Please choose "
            f"a different alias."
        )
    OP_COLLECTION[alias] = op_class
