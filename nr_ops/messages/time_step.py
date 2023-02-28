import json
from typing import Any, Dict, Optional

import pandas as pd
from pydantic import BaseModel, StrictStr, conint


class TimeStep(BaseModel):
    index: Optional[conint(strict=True, ge=0)] = None
    start: pd.Timestamp
    end: pd.Timestamp
    tz: StrictStr
    metadata: Optional[Dict[str, Any]] = None

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = True

    def __repr__(self):
        """Overwrite the default __repr__ with something more human-readable.

        This is useful when logging an initialized object.
        """
        return (
            f"TimeStep: "
            f"index={self.index} | "
            f"start='{str(self.start)}' | "
            f"end='{str(self.end)}' | "
            f"tz='{self.tz}'"
            # json.dumps also handles None and converts it to "null"
            f"metadata='{json.dumps(self.metadata)}'"
        )

    def to_json_dict(self) -> dict:
        return {
            "index": self.index,
            "start": str(self.start).replace(" ", "T"),
            "end": str(self.end).replace(" ", "T"),
            "tz": self.tz,
            "metadata": self.metadata,
        }

    @classmethod
    def mock_init(cls):
        return cls(
            index=0,
            start=pd.Timestamp(0, tz="UTC"),
            end=pd.Timestamp(0, tz="UTC") + pd.Timedelta(days=1),
            tz="UTC",
            metadata=None,
        )
