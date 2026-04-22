from pydantic import BaseModel
from typing import Optional


class Variable(BaseModel):
    id: int
    name: str
    display_name: str
    description: Optional[str] = None
    tier: int
    primary_series: Optional[str] = None
