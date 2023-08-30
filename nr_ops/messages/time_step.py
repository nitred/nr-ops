from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field, StrictStr, conint


class MetadataModel(BaseModel):
    created_at: StrictStr = Field(
        default_factory=lambda: pd.Timestamp.now(tz="UTC").isoformat()
    )

    class Config:
        # NOTE!!! This is required to allow NEW fields to be added to the model
        # by the user. This is useful for adding metadata to the model.
        extra = "allow"
        # NOTE!!! We don't allow any arbitrary types in the model.
        arbitrary_types_allowed = False


class TimeStep(BaseModel):
    index: Optional[conint(strict=True, ge=0)] = None
    start: pd.Timestamp
    end: pd.Timestamp
    tz: StrictStr
    metadata: MetadataModel = Field(default_factory=MetadataModel)

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
            f"start='{self.start.isoformat()}' | "
            f"end='{self.end.isoformat()}' | "
            f"tz='{self.tz}'"
            # json.dumps also handles None and converts it to "null"
            f"metadata='{self.metadata.json()}'"
        )

    def to_json_dict(self) -> dict:
        return {
            "index": self.index,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "tz": self.tz,
            "metadata": self.metadata.dict(),
        }

    @classmethod
    def mock_init(cls):
        return cls(
            index=0,
            start=pd.Timestamp(0, tz="UTC"),
            end=pd.Timestamp(0, tz="UTC") + pd.Timedelta(days=1),
            tz="UTC",
            metadata=MetadataModel(),
        )
