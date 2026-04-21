from pydantic import BaseModel, field_validator


class SeriesId(BaseModel):
    source: str
    native_id: str

    @field_validator("source")
    @classmethod
    def lowercase_source(cls, v):
        return v.lower()

    def __str__(self) -> str:
        return f"{self.source}:{self.native_id}"