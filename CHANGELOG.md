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
