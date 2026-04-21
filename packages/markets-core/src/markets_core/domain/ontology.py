from pydantic import BaseModel
from typing import Optional
from decimal import Decimal


class Variable(BaseModel):
    id: int
    name: str
    display_name: str
    description: Optional[str] = None
    tier: int
    primary_series: Optional[str] = None


class Regime(BaseModel):
    id: int
    name: str
    display_name: str
    description: str
    tier: int


class RegimeTrigger(BaseModel):
    id: int
    regime_id: int
    variable_id: int
    condition: str
    weight: Decimal
    description: Optional[str] = None