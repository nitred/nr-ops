import logging
import time
from typing import Any, Dict, Literal

from google.cloud import bigquery
from pydantic import StrictStr, root_validator

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.ops.base import BaseConnectorOp, BaseOpConfigModel
from nr_ops.ops.ops.connector_ops.interfaces.base import validate_hook_type_and_config

logger = logging.getLogger(__name__)


class GCPBigQueryConnOpConfigModel(BaseOpConfigModel):
    hook_type: Literal[
        "connector.hooks.airflow_gcp_bigquery_hook",
    ]
    hook_config: Dict[StrictStr, Any]

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False

    _validate_hook_type_and_config = root_validator(allow_reuse=True)(
        validate_hook_type_and_config
    )


class GCPBigQueryConnOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPBigQueryConnOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPBigQueryConnOp(BaseConnectorOp):
    OP_TYPE = "connector.gcp.bigquery"
    OP_CONFIG_MODEL = GCPBigQueryConnOpConfigModel
    OP_METADATA_MODEL = GCPBigQueryConnOpMetadataModel
    OP_AUDIT_MODEL = GCPBigQueryConnOpAuditModel

    templated_fields = None

    def __init__(self, hook_type: str, hook_config: Dict[str, Any], **kwargs):
        self.hook_type = hook_type
        self.hook_config = hook_config
        self.templated_fields = kwargs.get("templated_fields", [])

        from nr_ops.ops.op_collection import OP_COLLECTION

        self.hook_op = OP_COLLECTION[self.hook_type](**self.hook_config)
        self.hook_data = self.hook_op.run().data

        # Will be populated in run()
        self.bigquery_client: Any = None
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        self.bigquery_hook: BigQueryHook = None
        self.bigquery_client: bigquery.Client = None

    def extract_table(
        self,
        project: str,
        dataset_id: str,
        table_id: str,
        destination_uri: str,
        location: str,
        extract_job_config: Dict[str, Any],
    ):
        """."""
        logger.info(
            f"GCPBigQueryConnOp.extract_table: Extracting BigQuery table. "
            f"project={project}, dataset_id={dataset_id}, table_id={table_id}, "
            f"location={location}, destination_uri={destination_uri}"
        )
        start = time.time()
        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_ref = dataset_ref.table(table_id)
        extract_job = self.bigquery_client.extract_table(
            table_ref,
            destination_uri,
            # Location must match that of the source table.
            location=location,
            job_config=bigquery.job.ExtractJobConfig(**extract_job_config),
        )
        extract_job.result()  # Waits for job to complete.
        end = time.time()
        logger.info(
            f"GCPBigQueryConnOp.extract_table: Done uploading BigQuery table "
            f"{dataset_id}.{table_id} to {destination_uri=}. "
            f"Time taken: {end - start:0.2f} seconds"
        )

    def get_tables(self, project: str, dataset_id: str):
        logger.info(
            f"GCPBigQueryConnOp.get_tables: Getting tables for "
            f"project={project}, dataset_id={dataset_id}"
        )
        tables = self.bigquery_hook.get_dataset_tables(
            project_id=project, dataset_id=dataset_id
        )
        logger.info(
            f"GCPBigQueryConnOp.get_tables: Done getting tables for "
            f"project={project}, dataset_id={dataset_id}. Fetched {len(tables)} tables"
        )

    def run(self) -> OpMsg:
        """."""
        logger.info(f"GCPBigQueryConnOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=None, msg=None, log_prefix="GCPBigQueryConnOp.run:"
        )

        if self.hook_type == "connector.hooks.airflow_gcp_bigquery_hook":
            from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

            self.hook_data: BigQueryHook

            self.bigquery_hook = self.hook_data
            self.bigquery_client = self.bigquery_hook.get_client()
        else:
            raise NotImplementedError()

        return OpMsg(
            data=self,
            metadata=GCPBigQueryConnOpMetadataModel(),
            audit=GCPBigQueryConnOpAuditModel(),
        )
