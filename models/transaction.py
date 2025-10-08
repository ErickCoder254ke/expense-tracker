from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime
import uuid

class MPesaDetails(BaseModel):
    recipient: Optional[str] = None
    reference: Optional[str] = None
    transaction_id: Optional[str] = None
    phone_number: Optional[str] = None
    balance_after: Optional[float] = None
    message_type: Optional[str] = None  # received, sent, withdrawal, airtime, paybill, till, fuliza_loan, fuliza_repayment
    transaction_fee: Optional[float] = None
    access_fee: Optional[float] = None
    fuliza_limit: Optional[float] = None
    fuliza_outstanding: Optional[float] = None
    due_date: Optional[str] = None

class SMSMetadata(BaseModel):
    original_message_hash: Optional[str] = None
    parsing_confidence: Optional[float] = None
    parsed_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    requires_review: Optional[bool] = False
    suggested_category: Optional[str] = None
    total_fees: Optional[float] = None
    fee_breakdown: Optional[dict] = None

class Transaction(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    amount: float
    type: Literal["expense", "income"]
    category_id: str
    description: str
    date: datetime
    source: Literal["manual", "sms", "api"] = "manual"
    mpesa_details: Optional[MPesaDetails] = None
    sms_metadata: Optional[SMSMetadata] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    # Transaction grouping for multi-transaction SMS messages
    transaction_group_id: Optional[str] = None  # Links related transactions from same SMS
    transaction_role: Optional[Literal["primary", "fee", "fuliza_deduction", "access_fee"]] = "primary"
    parent_transaction_id: Optional[str] = None  # For fee/deduction transactions, reference to main transaction

class TransactionCreate(BaseModel):
    amount: float
    type: Literal["expense", "income"]
    category_id: str
    description: str
    date: datetime
    source: Literal["manual", "sms", "api"] = "manual"
    mpesa_details: Optional[MPesaDetails] = None
    sms_metadata: Optional[SMSMetadata] = None
    # Transaction grouping for multi-transaction SMS messages
    transaction_group_id: Optional[str] = None
    transaction_role: Optional[Literal["primary", "fee", "fuliza_deduction", "access_fee"]] = "primary"
    parent_transaction_id: Optional[str] = None

class TransactionUpdate(BaseModel):
    amount: Optional[float] = None
    type: Optional[Literal["expense", "income"]] = None
    category_id: Optional[str] = None
    description: Optional[str] = None
    date: Optional[datetime] = None
    mpesa_details: Optional[MPesaDetails] = None
    sms_metadata: Optional[SMSMetadata] = None

class SMSParseRequest(BaseModel):
    message: str

class SMSImportRequest(BaseModel):
    messages: list[str]
    auto_categorize: bool = True
    require_review: bool = False
    transaction_date: Optional[str] = None  # ISO format date string

class SMSImportResponse(BaseModel):
    total_messages: int
    successful_imports: int
    duplicates_found: int
    parsing_errors: int
    transactions_created: list[str]  # List of transaction IDs
    errors: list[str]
