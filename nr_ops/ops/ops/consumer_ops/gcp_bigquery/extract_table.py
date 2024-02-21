import logging
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, StrictStr

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseConsumerOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.gcp_bigquery import GCPBigQueryConnOp

logger = logging.getLogger(__name__)


class ExtractJobConfigModel(BaseModel):
    destination_format: Literal["CSV", "NEWLINE_DELIMITED_JSON", "AVRO", "PARQUET"]
    # https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationExtract.FIELDS.compression
    compression: Literal["NONE", "DEFLATE", "GZIP", "SNAPPY", "ZSTD"]
    field_delimiter: Optional[StrictStr] = None

    class Config:
        # NOTE WE ALLOW ADDITIONAL FIELDS HERE IN CASE WE MISSED SOMETHING
        extra = "allow"
        arbitrary_types_allowed = False


class GCPBigQueryToGCSOpConfigModel(BaseOpConfigModel):
    bigquery_conn_id: StrictStr
    project: StrictStr
    dataset_id: StrictStr
    table_id: StrictStr
    destination_uri: StrictStr
    location: StrictStr
    extract_job_config: ExtractJobConfigModel

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPBigQueryToGCSOpMetadataModel(BaseOpMetadataModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPBigQueryToGCSOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GCPBigQueryToGCSOp(BaseConsumerOp):
    OP_TYPE = "consumer.gcp.bigquery.extract_table"
    OP_CONFIG_MODEL = GCPBigQueryToGCSOpConfigModel
    OP_METADATA_MODEL = GCPBigQueryToGCSOpMetadataModel
    OP_AUDIT_MODEL = GCPBigQueryToGCSOpAuditModel

    templated_fields = None

    def __init__(
        self,
        bigquery_conn_id: str,
        project: str,
        dataset_id: str,
        table_id: str,
        destination_uri: str,
        location: str,
        extract_job_config: Dict[str, Any],
        **kwargs,
    ):
        """."""
        self.bigquery_conn_id = bigquery_conn_id
        self.project = project
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.destination_uri = destination_uri
        self.location = location
        self.extract_job_config = extract_job_config

        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.conn: GCPBigQueryConnOp = op_manager.get_connector(
            op_id=self.bigquery_conn_id
        )

    def run(self, time_step: TimeStep, msg: OpMsg) -> OpMsg:
        """."""
        logger.info("GCPBigQueryToGCSOp: Running.")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="PangresDFToSQLDBOpOp.run:"
        )

        logger.info(f"")
        self.conn.extract_table(
            project=self.project,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            destination_uri=self.destination_uri,
            location=self.location,
            extract_job_config=self.extract_job_config,
        )

        return OpMsg(
            data=None,
            metadata=GCPBigQueryToGCSOpMetadataModel(),
            audit=GCPBigQueryToGCSOpAuditModel(),
        )
