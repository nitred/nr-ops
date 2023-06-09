import collections
import logging
from typing import Dict, Iterable, List, Type

from nr_ops.ops.base import BaseOp
from nr_ops.ops.ops.connector_ops.hooks.airflow_gcp_bigquery_hook import (
    AirflowGCPBigQueryHookConnOp,
)
from nr_ops.ops.ops.connector_ops.hooks.airflow_gcp_gcs_hook import (
    AirflowGCPGCSHookConnOp,
)
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
from nr_ops.ops.ops.connector_ops.hooks.python_queue import PythonQueueHookConnOp
from nr_ops.ops.ops.connector_ops.interfaces.gcp_bigquery import GCPBigQueryConnOp
from nr_ops.ops.ops.connector_ops.interfaces.gcp_gcs import GCPGCSConnOp
from nr_ops.ops.ops.connector_ops.interfaces.google_ads import GoogleAdsConnectorOp
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
from nr_ops.ops.ops.connector_ops.interfaces.queue import QueueConnOp
from nr_ops.ops.ops.connector_ops.interfaces.s3 import S3ConnOp
from nr_ops.ops.ops.consumer_ops.dbt.dbt_run import DBTRunConsumerOp
from nr_ops.ops.ops.consumer_ops.gcp_bigquery.extract_table import GCPBigQueryToGCSOp
from nr_ops.ops.ops.consumer_ops.gcp_gcs.delete_key import GCPGCSDeleteKeyOp
from nr_ops.ops.ops.consumer_ops.gcp_gcs.put_key import GCPGCSPutKeyOp
from nr_ops.ops.ops.consumer_ops.google_ads.upload_offline_conversion import (
    GoogleAdsUploadOfflineConversionOp,
)
from nr_ops.ops.ops.consumer_ops.list.list_put import ListPutConsumerOp
from nr_ops.ops.ops.consumer_ops.mock import MockConsumerOp
from nr_ops.ops.ops.consumer_ops.nr_ops.postgres_create_etl_table import (
    NROpsPostgresCreateETLTableConsumerOp,
)
from nr_ops.ops.ops.consumer_ops.pangres.df_to_sql_db import PangresDFToSQLDBOp
from nr_ops.ops.ops.consumer_ops.put_list import PutListConsumerOp
from nr_ops.ops.ops.consumer_ops.queue.queue_put import QueuePutConsumerOp
from nr_ops.ops.ops.consumer_ops.s3.put_key import S3PutKeyOp
from nr_ops.ops.ops.consumer_ops.shell.shell_run import ShellRunConsumerOp
from nr_ops.ops.ops.consumer_ops.sql_query import SQLQueryConsumerOp
from nr_ops.ops.ops.generator_ops.airflow.dag.clear_task_instances import (
    AirflowDagClearTaskInstancesOp,
)
from nr_ops.ops.ops.generator_ops.airflow.dagruns.clear_dagrun import (
    AirflowDagRunClearDagRunOp,
)
from nr_ops.ops.ops.generator_ops.airflow.dagruns.get_dagrun import (
    AirflowDagRunGetDagRunOp,
)
from nr_ops.ops.ops.generator_ops.airflow.dagruns.trigger_dagrun import (
    AirflowDagRunTriggerDagRunOp,
)
from nr_ops.ops.ops.generator_ops.batch import BatchGeneratorOp
from nr_ops.ops.ops.generator_ops.bigcom.catalog_get_all_brands import (
    BigComCatalogGetAllBrandsOp,
)
from nr_ops.ops.ops.generator_ops.bigcom.catalog_get_all_categories import (
    BigComCatalogGetAllCategoriesOp,
)
from nr_ops.ops.ops.generator_ops.bigcom.customers_get_all_customers import (
    BigComCustomersGetAllCustomersOp,
)
from nr_ops.ops.ops.generator_ops.bigcom.orders_get_all_order_coupons import (
    BigComOrdersGetAllOrderCouponsOp,
)
from nr_ops.ops.ops.generator_ops.bigcom.orders_get_all_order_products import (
    BigComOrdersGetAllOrderProductsOp,
)
from nr_ops.ops.ops.generator_ops.bigcom.orders_get_all_order_shipping_addresses import (
    BigComOrdersGetAllOrderShippingAddressesOp,
)
from nr_ops.ops.ops.generator_ops.bigcom.orders_get_all_orders import (
    BigComOrdersGetAllOrdersOp,
)
from nr_ops.ops.ops.generator_ops.bigcom.products_get_all_product_variants import (
    BigComProductsGetAllProductVariantsOp,
)
from nr_ops.ops.ops.generator_ops.bigcom.products_get_all_products import (
    BigComProductsGetAllProductsOp,
)
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
from nr_ops.ops.ops.generator_ops.eval_expr.eval_expr_conditional import (
    EvalExprConditionalOp,
)
from nr_ops.ops.ops.generator_ops.gcp_gcs.get_key import GCPGCSGetKeyOp
from nr_ops.ops.ops.generator_ops.gcp_gcs.is_key_exists import GCPGCSIsKeyExistsOp
from nr_ops.ops.ops.generator_ops.gcp_gcs.list_keys import GCPGCSListKeysOp
from nr_ops.ops.ops.generator_ops.google.get_ga_reports import GetGAReportsOp
from nr_ops.ops.ops.generator_ops.google.get_ga_reports_ga4 import GetGAReportsGA4Op
from nr_ops.ops.ops.generator_ops.list.list_get import ListGetGeneratorOp
from nr_ops.ops.ops.generator_ops.mock import MockGeneratorOp
from nr_ops.ops.ops.generator_ops.pandas.read_generic import PandasReadGenericOp
from nr_ops.ops.ops.generator_ops.pandas.train_and_test_split import (
    PandasTrainTestSplit,
)
from nr_ops.ops.ops.generator_ops.pickle.pickle import PickleGeneratorOp
from nr_ops.ops.ops.generator_ops.pickle.unpickle import UnPickleGeneratorOp
from nr_ops.ops.ops.generator_ops.queue.queue_get import QueueGetGeneratorOp
from nr_ops.ops.ops.generator_ops.s3.get_key import S3GetKeyOp
from nr_ops.ops.ops.generator_ops.s3.list_keys import S3ListKeysOp
from nr_ops.ops.ops.generator_ops.sleep import SleepGeneratorOp
from nr_ops.ops.ops.generator_ops.sql_query import SQLQueryGeneratorOp
from nr_ops.ops.ops.group_ops.op_chain import OpChainGroupOp
from nr_ops.ops.ops.group_ops.op_chain_branch import OpChainBranchGroupOp
from nr_ops.ops.ops.group_ops.op_fan_in import OpFanInGroupOp
from nr_ops.ops.ops.group_ops.op_set import OpSetGroupOp
from nr_ops.ops.ops.time_step_ops.date_range import DateRangeTimeStepOp
from nr_ops.ops.ops.time_step_ops.mock_ts import MockTimeStepOp
from nr_ops.ops.ops.time_step_ops.simple_start_offset import SimpleStartOffsetTimeStepOp
from nr_ops.ops.ops.time_step_ops.simple_ts import SimpleTimeStepOp

logger = logging.getLogger(__name__)


OP_CLASSES: List[Type[BaseOp]] = [
    # connector_ops (Hooks)
    AirflowGCPHookConnOp,
    AirflowGCPBigQueryHookConnOp,
    AirflowGCPGCSHookConnOp,
    AirflowPostgresHookConnOp,
    AirflowMysqlHookConnOp,
    GCPServiceAccountFromFileConnOp,
    AirflowS3HookConnOp,
    HTTPRequestsHookFromEnvConnOp,
    PythonListHookConnOp,
    PythonQueueHookConnOp,
    # connector_ops (Interfaces)
    GoogleAnalyticsConnOp,
    GoogleAnalyticsGA4ConnOp,
    PostgresConnOp,
    MysqlConnOp,
    S3ConnOp,
    HTTPConnOp,
    ListConnOp,
    QueueConnOp,
    GCPGCSConnOp,
    GCPBigQueryConnOp,
    GoogleAdsConnectorOp,
    # consumer_ops
    MockConsumerOp,
    PangresDFToSQLDBOp,
    SQLQueryConsumerOp,
    S3PutKeyOp,
    PutListConsumerOp,
    GCPBigQueryToGCSOp,
    GCPGCSPutKeyOp,
    GCPGCSDeleteKeyOp,
    ShellRunConsumerOp,
    DBTRunConsumerOp,
    GoogleAdsUploadOfflineConversionOp,
    QueuePutConsumerOp,
    ListPutConsumerOp,
    NROpsPostgresCreateETLTableConsumerOp,
    # generator_ops
    AirflowDagRunTriggerDagRunOp,
    AirflowDagRunGetDagRunOp,
    AirflowDagRunClearDagRunOp,
    AirflowDagClearTaskInstancesOp,
    EvalExprOp,
    EvalExprAsMetadataOp,
    EvalExprConditionalOp,
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
    ListGetGeneratorOp,
    QueueGetGeneratorOp,
    BigComOrdersGetAllOrdersOp,
    BigComOrdersGetAllOrderProductsOp,
    BigComOrdersGetAllOrderCouponsOp,
    BigComOrdersGetAllOrderShippingAddressesOp,
    BigComProductsGetAllProductsOp,
    BigComProductsGetAllProductVariantsOp,
    BigComCustomersGetAllCustomersOp,
    BigComCatalogGetAllBrandsOp,
    BigComCatalogGetAllCategoriesOp,
    PandasReadGenericOp,
    PandasTrainTestSplit,
    GCPGCSGetKeyOp,
    GCPGCSListKeysOp,
    GCPGCSIsKeyExistsOp,
    BatchGeneratorOp,
    SQLQueryGeneratorOp,
    PickleGeneratorOp,
    UnPickleGeneratorOp,
    # group_ops (Groups)
    OpChainGroupOp,
    OpSetGroupOp,
    OpFanInGroupOp,
    OpChainBranchGroupOp,
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
