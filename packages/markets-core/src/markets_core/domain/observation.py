from datetime import datetime, date
from decimal import Decimal
from pydantic import BaseModel, ConfigDict, model_validator
from typing import Optional


class Observation(BaseModel):
    model_config = ConfigDict(frozen=True)

    series_id: str
    observation_date: date
    as_of_date: date
    value: Optional[Decimal] = None
    ingested_at: datetime
    source_revision: Optional[str] = None

    @model_validator(mode="after")
    def check_dates(self):
        if self.observation_date > self.as_of_date:
            raise ValueError("observation_date cannot exceed as_of_date")
        return self