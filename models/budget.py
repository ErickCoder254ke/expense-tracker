from pydantic import BaseModel, Field
from typing import Literal, Optional
from datetime import datetime
import uuid

class Budget(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    category_id: str
    amount: float
    period: Literal["monthly"] = "monthly"
    month: int
    year: int
    created_at: datetime = Field(default_factory=datetime.utcnow)

class BudgetCreate(BaseModel):
    category_id: str
    amount: float
    month: int
    year: int

class BudgetUpdate(BaseModel):
    amount: Optional[float] = None