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
from nr_ops.ops.ops.connector_ops.hooks.python_counter import PythonCounterHookConnOp
from nr_ops.ops.ops.connector_ops.hooks.python_file import PythonFileHookConnOp
from nr_ops.ops.ops.connector_ops.hooks.python_list import PythonListHookConnOp
from nr_ops.ops.ops.connector_ops.hooks.python_queue import PythonQueueHookConnOp
from nr_ops.ops.ops.connector_ops.interfaces.counter import CounterConnOp
from nr_ops.ops.ops.connector_ops.interfaces.file import FileConnOp
from nr_ops.ops.ops.connector_ops.interfaces.gcp_bigquery import GCPBigQueryConnOp
from nr_ops.ops.ops.connector_ops.interfaces.gcp_gcs import GCPGCSConnOp
from nr_ops.ops.ops.connector_ops.interfaces.google_ads import GoogleAdsConnectorOp
from nr_ops.ops.ops.connector_ops.interfaces.google_analytics import (
    GoogleAnalyticsConnOp,
)
from nr_ops.ops.ops.connector_ops.interfaces.google_analytics_ga4 import (
    GoogleAnalyticsGA4ConnOp,
)
from nr_ops.ops.ops.connector_ops.interfaces.google_sheets import GoogleSheetsConnOp
from nr_ops.ops.ops.connector_ops.interfaces.http import HTTPConnOp
from nr_ops.ops.ops.connector_ops.interfaces.list import ListConnOp
from nr_ops.ops.ops.connector_ops.interfaces.mysql import MysqlConnOp
from nr_ops.ops.ops.connector_ops.interfaces.postgres import PostgresConnOp
from nr_ops.ops.ops.connector_ops.interfaces.queue import QueueConnOp
from nr_ops.ops.ops.connector_ops.interfaces.s3 import S3ConnOp
from nr_ops.ops.ops.consumer_ops.braze.generic_post_json_data_batched_async import (
    BrazeGenericPostJsonDataAsyncOp,
)
from nr_ops.ops.ops.consumer_ops.counter.counter_put import CounterPutConsumerOp
from nr_ops.ops.ops.consumer_ops.dbt.dbt_run import DBTRunConsumerOp
from nr_ops.ops.ops.consumer_ops.file.file_put import FilePutConsumerOp
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
from nr_ops.ops.ops.generator_ops.airflow.dagruns.trigger_dagrun_and_wait_until_completion import (
    AirflowDagRunTriggerDagAndWaitUntilCompletionOp,
)
from nr_ops.ops.ops.generator_ops.batch import BatchGeneratorOp
from nr_ops.ops.ops.generator_ops.bigcom.catalog_get_all_brands import (
    BigComCatalogGetAllBrandsOp,
)
from nr_ops.ops.ops.generator_ops.bigcom.catalog_get_all_categories import (
    BigComCatalogGetAllCategoriesOp,
)
from nr_ops.ops.ops.generator_ops.bigcom.coupons_get_all_coupons import (
    BigComCouponsGetAllCouponsOp,
)
from nr_ops.ops.ops.generator_ops.bigcom.customers_get_all_customers import (
    BigComCustomersGetAllCustomersOp,
)
from nr_ops.ops.ops.generator_ops.bigcom.generic_compute_email_hashes import (
    BigcomGenericComputeEmailHashesOp,
)
from nr_ops.ops.ops.generator_ops.bigcom.generic_remove_pii import (
    BigcomGenericRemovePiiOp,
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
from nr_ops.ops.ops.generator_ops.compression.decompress_utils.decompress_newline_separated_streams_with_zlib import (
    DecompressNewlineSeparatedStreamsWithZlibOp,
)
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
from nr_ops.ops.ops.generator_ops.google.google_sheets_get_cell_range import (
    GoogleSheetsGetCellRangeOp,
)
from nr_ops.ops.ops.generator_ops.google_ads.google_ads_search_query import (
    GoogleAdsSearchQueryGeneratorOp,
)
from nr_ops.ops.ops.generator_ops.list.list_get import ListGetGeneratorOp
from nr_ops.ops.ops.generator_ops.mailchimp.campaigns_get_campaign_info import (
    MailchimpCampaignsGetCampaignInfoOp,
)
from nr_ops.ops.ops.generator_ops.mailchimp.campaigns_get_campaign_reports import (
    MailchimpCampaignsGetCampaignReportsOp,
)
from nr_ops.ops.ops.generator_ops.mailchimp.campaigns_get_campaigns import (
    MailchimpCampaignsGetCampaignsOp,
)
from nr_ops.ops.ops.generator_ops.mailchimp.campaigns_get_email_activity import (
    MailchimpCampaignsGetEmailActivityOp,
)
from nr_ops.ops.ops.generator_ops.mailchimp.campaigns_get_links_clicked import (
    MailchimpCampaignsGetLinksClickedOp,
)
from nr_ops.ops.ops.generator_ops.mailchimp.campaigns_get_links_clicked_members import (
    MailchimpCampaignsGetLinksClickedMembersOp,
)
from nr_ops.ops.ops.generator_ops.mailchimp.campaigns_get_sent_to import (
    MailchimpCampaignsGetSentToOp,
)
from nr_ops.ops.ops.generator_ops.mailchimp.campaigns_get_unsubscribed_members import (
    MailchimpCampaignsGetUnsubscribedMembersOp,
)
from nr_ops.ops.ops.generator_ops.mailchimp.generic_compute_email_hashes import (
    MailchimpGenericComputeEmailHashesOp,
)
from nr_ops.ops.ops.generator_ops.mailchimp.generic_remove_links import (
    MailchimpGenericRemoveLinksOp,
)
from nr_ops.ops.ops.generator_ops.mailchimp.generic_remove_pii import (
    MailchimpGenericRemovePiiOp,
)
from nr_ops.ops.ops.generator_ops.mailchimp.lists_get_list_members import (
    MailchimpListsGetListMembersOp,
)
from nr_ops.ops.ops.generator_ops.mailchimp.lists_get_lists import (
    MailchimpListsGetListsOp,
)
from nr_ops.ops.ops.generator_ops.mock import MockGeneratorOp
from nr_ops.ops.ops.generator_ops.ongoing.generic_remove_pii import (
    OngoingGenericRemovePiiOp,
)
from nr_ops.ops.ops.generator_ops.ongoing.orders_rest_get_all_orders import (
    OngoingOrdersRESTGetAllOrdersOp,
)
from nr_ops.ops.ops.generator_ops.ongoing.orders_rest_get_order import (
    OngoingOrdersRESTGetOrderOp,
)
from nr_ops.ops.ops.generator_ops.ongoing.orders_rest_get_waybill_rows import (
    OngoingOrdersRESTGetWayBillRowsOp,
)
from nr_ops.ops.ops.generator_ops.ongoing.orders_soap_get_all_orders import (
    OngoingOrdersSOAPGetAllOrdersOp,
)
from nr_ops.ops.ops.generator_ops.pandas.read_generic import PandasReadGenericOp
from nr_ops.ops.ops.generator_ops.pandas.train_and_test_split import (
    PandasTrainTestSplit,
)
from nr_ops.ops.ops.generator_ops.pickle.pickle import PickleGeneratorOp
from nr_ops.ops.ops.generator_ops.pickle.unpickle import UnPickleGeneratorOp
from nr_ops.ops.ops.generator_ops.priceindx.download_export_pipeline import (
    PriceindxDownloadExportPipelineOp,
)
from nr_ops.ops.ops.generator_ops.queue.queue_get import QueueGetGeneratorOp
from nr_ops.ops.ops.generator_ops.s3.get_key import S3GetKeyOp
from nr_ops.ops.ops.generator_ops.s3.list_keys import S3ListKeysOp
from nr_ops.ops.ops.generator_ops.skip_empty_input import SkipEmptyInputGeneratorOp
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
    PythonFileHookConnOp,
    PythonListHookConnOp,
    PythonCounterHookConnOp,
    PythonQueueHookConnOp,
    # connector_ops (Interfaces)
    PostgresConnOp,
    MysqlConnOp,
    S3ConnOp,
    HTTPConnOp,
    FileConnOp,
    ListConnOp,
    CounterConnOp,
    QueueConnOp,
    GCPGCSConnOp,
    GCPBigQueryConnOp,
    GoogleAdsConnectorOp,
    GoogleAnalyticsConnOp,
    GoogleAnalyticsGA4ConnOp,
    GoogleSheetsConnOp,
    # consumer_ops
    MockConsumerOp,
    PangresDFToSQLDBOp,
    SQLQueryConsumerOp,
    S3PutKeyOp,
    PutListConsumerOp,
    GCPBigQueryToGCSOp,
    GCPGCSPutKeyOp,
    GCPGCSDeleteKeyOp,
    GoogleAdsUploadOfflineConversionOp,
    ShellRunConsumerOp,
    DBTRunConsumerOp,
    FilePutConsumerOp,
    ListPutConsumerOp,
    QueuePutConsumerOp,
    CounterPutConsumerOp,
    NROpsPostgresCreateETLTableConsumerOp,
    BrazeGenericPostJsonDataAsyncOp,
    # generator_ops
    AirflowDagRunTriggerDagRunOp,
    AirflowDagRunGetDagRunOp,
    AirflowDagRunClearDagRunOp,
    AirflowDagClearTaskInstancesOp,
    AirflowDagRunTriggerDagAndWaitUntilCompletionOp,
    EvalExprOp,
    EvalExprAsMetadataOp,
    EvalExprConditionalOp,
    MockGeneratorOp,
    SleepGeneratorOp,
    SkipEmptyInputGeneratorOp,
    S3ListKeysOp,
    S3GetKeyOp,
    CompressOp,
    DecompressOp,
    DecompressNewlineSeparatedStreamsWithZlibOp,
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
    BigComCouponsGetAllCouponsOp,
    BigcomGenericComputeEmailHashesOp,
    BigcomGenericRemovePiiOp,
    MailchimpCampaignsGetCampaignsOp,
    MailchimpCampaignsGetCampaignInfoOp,
    MailchimpCampaignsGetSentToOp,
    MailchimpCampaignsGetUnsubscribedMembersOp,
    MailchimpCampaignsGetLinksClickedOp,
    MailchimpCampaignsGetLinksClickedMembersOp,
    MailchimpListsGetListsOp,
    MailchimpListsGetListMembersOp,
    MailchimpCampaignsGetCampaignReportsOp,
    MailchimpCampaignsGetEmailActivityOp,
    MailchimpGenericComputeEmailHashesOp,
    MailchimpGenericRemovePiiOp,
    MailchimpGenericRemoveLinksOp,
    OngoingOrdersSOAPGetAllOrdersOp,
    OngoingOrdersRESTGetWayBillRowsOp,
    OngoingOrdersRESTGetAllOrdersOp,
    OngoingOrdersRESTGetOrderOp,
    OngoingGenericRemovePiiOp,
    PriceindxDownloadExportPipelineOp,
    PandasReadGenericOp,
    PandasTrainTestSplit,
    GCPGCSGetKeyOp,
    GCPGCSListKeysOp,
    GCPGCSIsKeyExistsOp,
    GetGAReportsOp,
    GetGAReportsGA4Op,
    GoogleAdsSearchQueryGeneratorOp,
    GoogleSheetsGetCellRangeOp,
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
