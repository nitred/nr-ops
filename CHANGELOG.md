# About
We follow semantic versioning.

# Template
```
# Release 0.0.0 (YYYY-MM-DD)
* **New Ops**
  * Connector Hooks
    * ...
  * Connector Interfaces
    * ...
  * Generators
    * ...
  * Consumers
    * ...
  * Groups
    * ...
  * TimeSteps
    * ...
* **Breaking Changes**
  * ...
* **Non-breaking Changes**
  * ...
* **Notes from future**
  * ...
```

# Release 0.23.1.0 (2023-11-20)
* **Non-breaking Changes**
  * Added new arg `trigger_accepted_status_codes` for `AirflowDagRunTriggerDagAndWaitUntilCompletionOp` to allow 409 status codes as well.


# Release 0.23.0.0 (2023-11-20)
* **New Ops**
  * Generators
    * Added `AirflowDagRunTriggerDagAndWaitUntilCompletionOp`
* **Non-breaking Changes**
  * Added `pkg-config` apt package to docker image (prod and lambda).


# Release 0.22.0.0 (2023-11-17)
* **Breaking Changes**
  * Upgraded dependency to `pydantic==1.10.13`


# Release 0.21.0 (2023-11-17)
* **Non-breaking Changes**
  * Updated `GoogleSheetsGetCellRangeOp` op with better logging.
  * Added new function `custom_raise_exception` to EvalGlobals to be available to Eval Ops.


# Release 0.20.0 (2023-11-11)
* **New Ops**
  * Connector Interfaces
    * Added `GoogleSheetsConnectorOp`
  * Generators
    * Added `GoogleSheetsGetCellRangeOp`

# Release 0.19.0 (2023-10-20)
* **New Ops**
  * Generators
    * Added `GoogleAdsSearchQueryGeneratorOp`


# Release 0.18.0 (2023-10-11)
* **New Ops**
  * Generators
    * Added `OngoingOrdersRESTGetAllOrdersOp`
    * Added `OngoingOrdersRESTGetOrderOp`
* **Non-breaking Changes**
  * Renames Ongoing's op filenames to include whether they use REST or SOAP.


# Release 0.17.0 (2023-09-27)
* **New Ops**
  * Generators
    * Added `MailchimpCampaignsGetEmailActivityOp`
* **Non-breaking Changes**
  * Updated `MailchimpGenericRemovePiiOp` to remove email_address and ip from email activity records.
  * Updated `EvalExprOp` with new arguments `msg_data_var_name` and `msg_metadata_var_name`.


# Release 0.16.0 (2023-09-12)
* **New Ops**
  * Generators
    * Added `OngoingOrdersSOAPGetAllOrdersOp` which uses SOAP
    * Added `OngoingOrdersRESTGetWayBillRowsOp` which uses REST
    * Added `OngoingGenericRemovePiiOp`
    * Added `SkipEmptyInputGeneratorOp`
* **Breaking Changes**
  * Changed all occurrences of `str(pd.Timestamp.now(tz="UTC"))` to `pd.Timestamp.now(tz="UTC").isoformat()`
  * Added `zeep==4.2.1` dependency to handle SOAP requests. Updated poetry.lock accordingly.


# Release 0.15.0 (2023-08-30)
* **New Ops**
  * Generators
    * Added `BigcomGenericComputeEmailHashesOp` to compute hashes of different types on email address received from the Bigcom API. These hashes can  can be used as identifiers without storing pii.
    * Added `BigcomGenericRemovePiiOp` which is a generic op that removes PII from records for all related Bigcom ops
* **Breaking Changes**
  * Removed `remove_pii` as config params across all existing Bigcom ops and moved functionality to the generic ops i.e. `BigcomGenericRemovePiiOp`
  * Removed `yield_only_email_data` as config params from MailchimpGenericComputeEmailHashesOp.
* **Non-breaking Changes**
  * Optimized and removed unnecessary imports.

# Release 0.14.1 (2023-08-24)
* **Breaking Changes**
  * Bugfix: Implemented `MailchimpListsGetListMembersOp` correctly. The previous version was completely incorrect.
* **Non-breaking Changes**
  * Removed unused argument `include_total_contacts` from `MailchimpListsGetListsOp


# Release 0.14.0 (2023-08-23)
* **New Ops**
  * Generators
    * Added `MailchimpGenericComputeEmailHashesOp` to compute hashes of different types on email address received from the mailchimp API. These hashes can  can be used as identifiers without storing pii.
    * Added `MailchimpGenericRemovePiiOp` which is a generic op that removes PII from records for all related Mailchimp ops
    * Added `MailchimpGenericRemoveLinksOp` which is a generic op that removes links/urls from records for all Mailchimp ops
* **Breaking Changes**
  * Removed `remove_links` and `remove_pii` as config params across all existing Mailchimp ops and moved functionality to the generic ops i.e. `MailchimpGenericRemovePiiOp` and `MailchimpGenericRemoveLinksOp`

# Release 0.13.0 (2023-08-22)
* **New Ops**
  * Generators
    * Added `PriceindxDownloadExportPipelineOp`


# Release 0.12.0 (2023-08-21)
* **New Ops**
  * Generators
    * Added `MailchimpCampaignsGetCampaignReportsOp`
* **Non-Breaking Changes**
  * Updated FilePutConsumerOp, FileConnOp and PythonFileHookConnOp with new put types `write_line` and `write_line_and_flush`
  * Updated all Mailchimp Ops, `remove_links` now also removes `subscribe_url_short` and `subscribe_url_long` fields


# Release 0.11.0 (2023-08-18)
* **New Ops**
  * Generators
    * Added MailchimpCampaignsGetCampaignsOp
    * Added MailchimpCampaignsGetCampaignInfoOp
    * Added MailchimpCampaignsGetSentToOp
    * Added MailchimpCampaignsGetUnsubscribedMembersOp
    * Added MailchimpCampaignsGetLinksClickedOp
    * Added MailchimpCampaignsGetLinksClickedMembersOp
    * Added MailchimpListsGetListsOp
    * ~~Added MailchimpListsGetListMembersOp~~ (2023-08-24: Do not use this Op before release 0.14.1)
  * Consumers
    * Added FilePutConsumerOp
    * Added CounterPutConsumerOp
  * Interfaces
    * Added FileConnOp
    * Added CounterConnOp
  * Hooks
    * Added PythonFileHookConnOp
    * Added PythonCounterHookConnOp
* **Non-Breaking Changes**
  * Updated ListPutConsumerOp to use underlying Interface (ListConnOp) or Hook (PythonListHookConnOp) instead of directly using the Python list object.
  * Updated ListPutConsumerOp, ListConnOp and PythonListHookConnOp with a new method `.size()` to get the length of the list. 
* **Notes from future**
  * **2023-08-24: Bugfix**: Implemented `MailchimpListsGetListMembersOp` correctly. Do not use this op before release 0.14.1. This op in this version is merely a copy-pasted version of another Op.

# Release 0.10.0 (2023-08-01)
* **New Ops**
  * Generators
    * BigComCouponsGetAllCouponsOp
* **Breaking Changes**
  * bugfix: removed airflow imports from global namespace

# Release 0.9.1 (2023-06-09)
* **Breaking Changes**
  * Bugfix: Added `page >= total_pages` check to `BigComCatalogGetAllBrandsOp`, `BigComCatalogGetAllCategoriesOp`, `BigComCustomersGetAllCustomersOp` to prevent infinite loops.

# Release 0.9.0 (2023-06-09)
* **New Ops**
  * Generators
    * BigComOrdersGetAllOrdersOp
    * BigComOrdersGetAllOrderProductsOp
    * BigComOrdersGetAllOrderCouponsOp
    * BigComOrdersGetAllOrderShippingAddressesOp
    * BigComCustomersGetAllCustomersOp
    * BigComCatalogGetAllBrandsOp
    * BigComCatalogGetAllCategoriesOp


# Release 0.8.0 (2023-05-22)
* **New Ops**
  * Connector Hooks
    * PythonQueueHookConnOp
  * Connector Interfaces
    * QueueConnOp
  * Generators
    * BigComOrdersGetAllOrdersOp
    * BigComProductsGetAllProductsOp
    * BigComProductsGetAllProductVariantsOp
    * ListGetGeneratorOp
    * QueueGetGeneratorOp
  * Consumers
    * NROpsPostgresCreateETLTableConsumerOp
    * ListPutConsumerOp
    * QueuePutConsumerOp
* **Non-breaking Changes**
  * Added `apply_extras_as_headers` arguments to HTTPRequestsHookFromEnvConnOp hook and HTTPConnOp interface


# Release 0.7.0 (2023-05-09)
* **New Ops**
  * Generators
    * Added AirflowDagRunClearDagRunOp
    * Added `AirflowDagClearTaskInstancesOp`


# Release 0.6.2 (2023-05-08)
* **Breaking Changes**
  * bugfix: templated_fields now handles non-str data types correctly when using recursive templating
  * bugfix: dbt_kwargs_model in dbt.run is now correctly initialized after fields have been rendered
  * bugfix: s3.list_keys now correctly allows an empty prefix '' 
  * bugfix: msg obj is now correctly passed to render_fields instead of None in some cases


# Release 0.6.1 (2023-05-05)
* **Non-breaking Changes**
  * `vim git net-tools iputils-ping curl wget` are now installed in the docker image.


# Release 0.6.0 (2023-04-26)
* **New Ops**
  * Connector Interfaces
    * Added GoogleAdsConnectorOp (does not require any hooks and only relies on environment variables)
  * Generators
    * Added SQLQueryGeneratorOp
    * Added PandasTrainTestSplit
    * Added PickleGeneratorOp
    * Added UnPickleGeneratorOp
    * Added AirflowDagRunGetDagRunOp
    * Added AirflowDagRunTriggerDagRunOp
  * Consumers
    * Added ShellRunConsumerOp
    * Added DBTRunConsumerOp
    * Added GoogleAdsUploadOfflineConversionOp
* **Breaking Changes**
  * Added dependencies for dbt: `dbt-core==1.4.5` and `dbt-postgres==1.4.5`
  * Added dependencies for google-ads: `google-ads==21.0.0`
  * IMPORTANT: Using `poetry==1.4.2` and poetry has been removed from dev dependencies and instead has been installed globally in a separate environment. Some dependency conflicts have been resolved by doing this.
* **Non-breaking Changes**
  * Added `set_env_vars` to MainConfigModel
  * Added `read_sql` as a read_type for PandasReadGenericOp
  * Added the following sklearn imports to EVAL_GLOBALS
    * `sklearn`
    * `sklearn.datasets`
    * `sklearn.linear_model`
    * `sklearn.metrics`
    * `sklearn.model_selection`
    * `sklearn.pipeline`
    * `sklearn.preprocessing`
    * `sklearn.svm`
 

# Release 0.5.2 (2023-03-24)
* **Non-breaking Changes**
  * Added entire process memory usage stats in logs after each op message yield.


# Release 0.5.1 (2023-03-23)
* **Non-breaking Changes**
  * Op now logs time taken for each message to be yielded.
  * Added `get_metadata` method to `OpManager`
  * Added a large list of standard libraries to `EVALS_GLOBAL` 
  * Added more logging in general


# Release 0.5.0 (2023-03-22)
* **New Ops**
  * Connector Hooks
    * Added `connector.hooks.airflow_gcp_bigquery_hook`
    * Added `connector.hooks.airflow_gcp_gcs_hook`
  * Connector Interfaces
    * Added `connector.gcp_bigquery`
    * Added `connector.gcp_gcs`
  * Generators
    * Added `generator.gcp.gcs.get_key`
    * Added `generator.gcp.gcs.list_keys`
    * Added `generator.gcp.gcs.is_key_exists`
  * Consumers
    * Added `consumer.gcp.bigquery.extract_table`
    * Added `consumer.gcp.gcs.put_key`
    * Added `consumer.gcp.gcs.delete_key`
  * Groups
    * Added `group.op_chain_branch`
* **Non-breaking Changes**
  * Removed `op_depth` from all ops. It was not useful or was poorly implemented.
  * `root_msg` is now no longer Optional, it cannot just be `None`, it must be an `OpMsg` with `OpMsg.data = None`.
  * `generator.eval_expr_conditional`: Modified the op such that `"yield_input", "yield_output", "consume"` are valid options for both `on_true_behavior` and `on_false_behavior`. 

# Release 0.4.0 (2023-03-10)
* **New Ops**
  * Group
    * Added `OpFanInGroupOpConfigModel` which is a like a group of independent generators or group of independent sources.
  * Generators
    * Added `PandasReadGenericOp` that is a wrapper around `pandas.read_*` functions.
  * Consumers
    * ...
* **Breaking Changes**
  * Update `TimeStep`:
    * `to_json_dict` method no longer contains `start_isoformat` and `end_isoformat` attributes. The `start` and `end` use isoformat timestamp strings instead.
  * Updated `OpManager`:
    * Deprecated `OpSubManager` and instead `OpManager` maintains `data`, `metadata` and `ops` dicts where the keys are `op_ids`.
* **Non-breaking Changes**
  * Updated `TimeStep`
    * Default metadata along with a MetadataModel is introduced with `created_at` timestamp as a default attribute.
  * Updated `Op`:
    * Introduced `store_metadata` and `store_data` arguments to the OpModel.
  * Updated `PangresDFToSQLDBOp`:
    * Added `create_schema`, `create_table`, `chunksize`  and `add_new_columns` arguments.
  * Updated `S3ListKeysOp`:
    * Added additional metadata fields.


# Release 0.3.1 (2023-03-07)
* **Breaking Changes**
  * Bugfix/Improvement: Improved backoff in HTTPConnOp. `backoff_config: {}` must be added to config file to enable backoff with default values.

# Release 0.3.0 (2023-03-07)
* **New Ops**
  * Generators
    * `EvalExprConditionalOp`
* **Non-breaking Changes**
  * Added `time` and `logger` to the EVAL_GLOBALS

# Release 0.2.0 (2023-03-06)
* **New Ops**
  * Connector Hooks
    * `PythonListHookConnOp`
  * Connector Interfaces
    * `ListConnOp`
  * Generators
    * `BladeProductsListVariationsOp`
    * `BladeProductsViewVariationsOp`
    * `GetListGeneratorOp`
  * Consumers
    * `PutListConsumerOp`
* **Non-breaking Changes**
  * Added `.gitignore` in config folder

# Release 0.1.0 (2023-02-28)
* **New Ops**
  * Connector Hooks
    * ...
  * Connector Interfaces
    * ...
  * Generators
    * ...
  * Consumers
    * ...
