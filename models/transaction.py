from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime
import uuid

class MPesaDetails(BaseModel):
    recipient: Optional[str] = None
    reference: Optional[str] = None
    transaction_id: Optional[str] = None

class Transaction(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    amount: float
    type: Literal["expense", "income"]
    category_id: str
    description: str
    date: datetime
    source: Literal["manual", "sms"] = "manual"
    mpesa_details: Optional[MPesaDetails] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

class TransactionCreate(BaseModel):
    amount: float
    type: Literal["expense", "income"]
    category_id: str
    description: str
    date: datetime
    mpesa_details: Optional[MPesaDetails] = None

class TransactionUpdate(BaseModel):
    amount: Optional[float] = None
    type: Optional[Literal["expense", "income"]] = None
    category_id: Optional[str] = None
    description: Optional[str] = None
    date: Optional[datetime] = None