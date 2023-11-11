import logging
from typing import Any, Dict, Generator, List, Optional

from googleapiclient.discovery import build
from pydantic import StrictStr, conlist

from nr_ops.messages.op_audit import BaseOpAuditModel
from nr_ops.messages.op_metadata import BaseOpMetadataModel
from nr_ops.messages.op_msg import OpMsg
from nr_ops.messages.time_step import TimeStep
from nr_ops.ops.base import BaseGeneratorOp, BaseOpConfigModel
from nr_ops.ops.op_manager import get_global_op_manager
from nr_ops.ops.ops.connector_ops.interfaces.google_sheets import GoogleSheetsConnOp

logger = logging.getLogger(__name__)


class GoogleSheetsGetCellRangeOpConfigModel(BaseOpConfigModel):
    google_sheets_conn_id: StrictStr
    spreadsheet_id: StrictStr
    sheet_name: StrictStr
    cell_range: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleSheetsGetCellRangeOpMetadataModel(BaseOpMetadataModel):
    spreadsheet_id: StrictStr
    sheet_name: StrictStr
    cell_range: StrictStr

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleSheetsGetCellRangeOpAuditModel(BaseOpAuditModel):
    pass

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = False


class GoogleSheetsGetCellRangeOp(BaseGeneratorOp):
    OP_TYPE = "generator.google_sheets.get_cell_range"
    OP_CONFIG_MODEL = GoogleSheetsGetCellRangeOpConfigModel
    OP_METADATA_MODEL = GoogleSheetsGetCellRangeOpMetadataModel
    OP_AUDIT_MODEL = GoogleSheetsGetCellRangeOpAuditModel

    templated_fields = None

    def __init__(
        self,
        google_sheets_conn_id: str,
        spreadsheet_id: str,
        sheet_name: str,
        cell_range: str,
        **kwargs,
    ):
        """."""
        self.google_sheets_conn_id = google_sheets_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.cell_range = cell_range
        self.templated_fields = kwargs.get("templated_fields", [])

        op_manager = get_global_op_manager()

        self.conn: GoogleSheetsConnOp = op_manager.get_connector(
            op_id=self.google_sheets_conn_id
        )
        self.sheet_service = self.conn.sheet_service

    def run(
        self, time_step: TimeStep, msg: Optional[OpMsg] = None
    ) -> Generator[OpMsg, None, None]:
        """."""
        logger.info(f"GoogleSheetsGetCellRangeOp.run: Running")

        # RENDERS AND UPDATES THE TEMPLATED FIELDS INPLACE
        self.render_fields(
            time_step=time_step, msg=msg, log_prefix="GoogleSheetsGetCellRangeOp.run:"
        )

        range_ = f"{self.sheet_name}!{self.cell_range}"

        logger.info(
            f"GoogleSheetsGetCellRangeOp.run: Fetching cell range: {self.cell_range} "
            f"from {self.sheet_name=} and {self.spreadsheet_id}. Merged {range_=}"
        )

        result = (
            self.sheet_service.values()
            .get(spreadsheetId=self.spreadsheet_id, range=range_)
            .execute()
        )

        rows: List[List] = result.get("values", [])

        if not rows:
            logger.info(f"GoogleSheetsGetCellRangeOp.run: No data. {rows=}. Consuming.")
            return

        logger.info(
            f"GoogleSheetsGetCellRangeOp.run: Data exists. "
            f"{len(rows)=}. first_row={rows[0]}. Yielding."
        )

        yield OpMsg(
            data=rows,
            metadata=GoogleSheetsGetCellRangeOpMetadataModel(
                spreadsheet_id=self.spreadsheet_id,
                sheet_name=self.sheet_name,
                cell_range=self.cell_range,
            ),
            audit=GoogleSheetsGetCellRangeOpAuditModel(),
        )
