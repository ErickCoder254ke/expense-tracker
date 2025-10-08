"""
Enhanced SMS Parser that creates separate transactions for each monetary movement
Ensures every shilling is accounted for by creating:
- Primary transaction (main send/receive)
- Fee transactions (M-Pesa transaction fees)
- Access fee transactions (Fuliza access fees)
- Fuliza deduction transactions (automatic repayments)
"""

import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime
from models.transaction import TransactionCreate, MPesaDetails, SMSMetadata
from services.mpesa_parser import MPesaParser


class EnhancedSMSParser:
    """
    Enhanced SMS parser that creates multiple linked transactions from a single SMS
    """
    
    @classmethod
    def parse_message_to_transactions(cls, message: str, user_id: str, 
                                    category_mapping: Dict[str, str] = None) -> List[TransactionCreate]:
        """
        Parse an SMS message and return a list of transactions for each monetary movement
        
        Returns:
            List[TransactionCreate]: List of transactions to be created
        """
        # First use the existing parser to get the basic transaction data
        parsed_data = MPesaParser.parse_message(message)
        
        if not parsed_data:
            return []
        
        # Generate a unique group ID for all transactions from this SMS
        transaction_group_id = str(uuid.uuid4())
        transaction_date = datetime.now()
        
        # Parse transaction date if available
        if parsed_data.get('transaction_date'):
            try:
                transaction_date = datetime.fromisoformat(parsed_data['transaction_date'].replace('Z', '+00:00'))
            except (ValueError, AttributeError, TypeError):
                transaction_date = datetime.now()
        
        # Get default category IDs
        default_categories = category_mapping or cls._get_default_categories()
        
        transactions = []
        
        # Create the primary transaction
        primary_transaction = cls._create_primary_transaction(
            parsed_data, transaction_group_id, transaction_date, default_categories
        )
        transactions.append(primary_transaction)
        primary_transaction_id = primary_transaction.mpesa_details.transaction_id or str(uuid.uuid4())
        
        # Create fee transactions if fees exist
        fee_transactions = cls._create_fee_transactions(
            parsed_data, transaction_group_id, primary_transaction_id, 
            transaction_date, default_categories
        )
        transactions.extend(fee_transactions)
        
        # Create Fuliza-related transactions
        fuliza_transactions = cls._create_fuliza_transactions(
            parsed_data, transaction_group_id, primary_transaction_id,
            transaction_date, default_categories
        )
        transactions.extend(fuliza_transactions)
        
        return transactions
    
    @classmethod
    def _create_primary_transaction(cls, parsed_data: Dict[str, Any], 
                                  transaction_group_id: str, transaction_date: datetime,
                                  default_categories: Dict[str, str]) -> TransactionCreate:
        """Create the primary transaction from parsed SMS data"""
        
        # Get appropriate category
        category_id = default_categories.get(parsed_data['suggested_category'], 
                                           default_categories.get('General', ''))
        
        # Create SMS metadata for primary transaction
        sms_metadata = SMSMetadata(
            original_message_hash=parsed_data['original_message_hash'],
            parsing_confidence=parsed_data['parsing_confidence'],
            requires_review=parsed_data['requires_review'],
            suggested_category=parsed_data['suggested_category'],
            total_fees=cls._calculate_total_fees(parsed_data),
            fee_breakdown=cls._extract_fee_breakdown(parsed_data)
        )
        
        return TransactionCreate(
            amount=parsed_data['amount'],
            type=parsed_data['type'],
            category_id=category_id,
            description=parsed_data['description'],
            date=transaction_date,
            source="sms",
            mpesa_details=parsed_data['mpesa_details'],
            sms_metadata=sms_metadata,
            transaction_group_id=transaction_group_id,
            transaction_role="primary",
            parent_transaction_id=None
        )
    
    @classmethod
    def _create_fee_transactions(cls, parsed_data: Dict[str, Any], 
                               transaction_group_id: str, primary_transaction_id: str,
                               transaction_date: datetime, default_categories: Dict[str, str]) -> List[TransactionCreate]:
        """Create separate transactions for M-Pesa fees"""
        fee_transactions = []
        mpesa_details = parsed_data.get('mpesa_details', {})
        
        # Transaction fee
        transaction_fee = mpesa_details.get('transaction_fee', 0)
        if transaction_fee and transaction_fee > 0:
            fee_transaction = TransactionCreate(
                amount=transaction_fee,
                type="expense",
                category_id=default_categories.get('Transaction Fees', default_categories.get('General', '')),
                description=f"M-Pesa Transaction Fee - {parsed_data.get('description', 'Transaction')}",
                date=transaction_date,
                source="sms",
                mpesa_details=MPesaDetails(
                    transaction_id=primary_transaction_id,
                    message_type="transaction_fee",
                    recipient="M-Pesa Transaction Fee"
                ),
                sms_metadata=SMSMetadata(
                    original_message_hash=parsed_data['original_message_hash'],
                    parsing_confidence=1.0,  # Fees are usually clearly identified
                    requires_review=False,
                    suggested_category="Transaction Fees"
                ),
                transaction_group_id=transaction_group_id,
                transaction_role="fee",
                parent_transaction_id=primary_transaction_id
            )
            fee_transactions.append(fee_transaction)
        
        # Access fee (Fuliza)
        access_fee = mpesa_details.get('access_fee', 0)
        if access_fee and access_fee > 0:
            access_fee_transaction = TransactionCreate(
                amount=access_fee,
                type="expense", 
                category_id=default_categories.get('Loans & Credit', default_categories.get('General', '')),
                description=f"Fuliza Access Fee - {parsed_data.get('description', 'Fuliza Loan')}",
                date=transaction_date,
                source="sms",
                mpesa_details=MPesaDetails(
                    transaction_id=primary_transaction_id,
                    message_type="access_fee",
                    recipient="Fuliza Access Fee"
                ),
                sms_metadata=SMSMetadata(
                    original_message_hash=parsed_data['original_message_hash'],
                    parsing_confidence=1.0,
                    requires_review=False,
                    suggested_category="Loans & Credit"
                ),
                transaction_group_id=transaction_group_id,
                transaction_role="access_fee",
                parent_transaction_id=primary_transaction_id
            )
            fee_transactions.append(access_fee_transaction)
        
        return fee_transactions
    
    @classmethod
    def _create_fuliza_transactions(cls, parsed_data: Dict[str, Any],
                                  transaction_group_id: str, primary_transaction_id: str,
                                  transaction_date: datetime, default_categories: Dict[str, str]) -> List[TransactionCreate]:
        """Create separate transactions for Fuliza deductions"""
        fuliza_transactions = []
        mpesa_details = parsed_data.get('mpesa_details', {})
        message_type = mpesa_details.get('message_type', '')
        
        # Handle compound received + Fuliza deduction
        if message_type == 'compound_received_fuliza':
            # Look for Fuliza deduction amount in the original message
            fuliza_deduction = cls._extract_fuliza_deduction_amount(parsed_data)
            
            if fuliza_deduction and fuliza_deduction > 0:
                fuliza_deduction_transaction = TransactionCreate(
                    amount=fuliza_deduction,
                    type="expense",
                    category_id=default_categories.get('Loans & Credit', default_categories.get('General', '')),
                    description=f"Automatic Fuliza Repayment - {parsed_data.get('description', 'Received Money')}",
                    date=transaction_date,
                    source="sms",
                    mpesa_details=MPesaDetails(
                        transaction_id=primary_transaction_id,
                        message_type="fuliza_repayment",
                        recipient="Fuliza M-PESA Repayment",
                        fuliza_limit=mpesa_details.get('fuliza_limit'),
                        fuliza_outstanding=mpesa_details.get('fuliza_outstanding')
                    ),
                    sms_metadata=SMSMetadata(
                        original_message_hash=parsed_data['original_message_hash'],
                        parsing_confidence=0.9,  # High confidence for automatic deductions
                        requires_review=False,
                        suggested_category="Loans & Credit"
                    ),
                    transaction_group_id=transaction_group_id,
                    transaction_role="fuliza_deduction",
                    parent_transaction_id=primary_transaction_id
                )
                fuliza_transactions.append(fuliza_deduction_transaction)
        
        return fuliza_transactions
    
    @classmethod
    def _extract_fuliza_deduction_amount(cls, parsed_data: Dict[str, Any]) -> Optional[float]:
        """Extract Fuliza deduction amount from compound transaction"""
        message = parsed_data.get('original_message', '')
        if not message:
            return None
        
        # Pattern to find "Ksh X.XX has been used to pay/repay Fuliza"
        import re
        deduction_pattern = r'(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+.*?(?:used to|been used to).*?(?:pay|repay).*?fuliza'
        match = re.search(deduction_pattern, message.lower())
        
        if match:
            amount_str = match.group(1).replace(',', '')
            try:
                return float(amount_str)
            except ValueError:
                pass
        
        return None
    
    @classmethod
    def _calculate_total_fees(cls, parsed_data: Dict[str, Any]) -> float:
        """Calculate total fees from parsed data"""
        mpesa_details = parsed_data.get('mpesa_details', {})
        total_fees = 0.0
        
        transaction_fee = mpesa_details.get('transaction_fee', 0) or 0
        access_fee = mpesa_details.get('access_fee', 0) or 0
        
        total_fees = transaction_fee + access_fee
        return total_fees
    
    @classmethod
    def _extract_fee_breakdown(cls, parsed_data: Dict[str, Any]) -> Dict[str, float]:
        """Extract detailed fee breakdown"""
        mpesa_details = parsed_data.get('mpesa_details', {})
        fee_breakdown = {}
        
        transaction_fee = mpesa_details.get('transaction_fee', 0)
        if transaction_fee and transaction_fee > 0:
            fee_breakdown['transaction_fee'] = transaction_fee
        
        access_fee = mpesa_details.get('access_fee', 0)
        if access_fee and access_fee > 0:
            fee_breakdown['access_fee'] = access_fee
        
        return fee_breakdown
    
    @classmethod
    def _get_default_categories(cls) -> Dict[str, str]:
        """Get default category mappings - this should be replaced with actual category lookup"""
        return {
            'General': 'general_category_id',
            'Transaction Fees': 'fees_category_id', 
            'Loans & Credit': 'loans_category_id',
            'Transport': 'transport_category_id',
            'Food & Dining': 'food_category_id',
            'Utilities': 'utilities_category_id',
            'Shopping': 'shopping_category_id',
            'Healthcare': 'healthcare_category_id',
            'Education': 'education_category_id',
            'Entertainment': 'entertainment_category_id',
            'Business': 'business_category_id',
            'Personal Care': 'personal_care_category_id',
            'Family & Friends': 'family_category_id',
            'Savings & Investment': 'savings_category_id',
            'Income': 'income_category_id'
        }
    
    @classmethod
    def analyze_transaction_completeness(cls, transactions: List[TransactionCreate]) -> Dict[str, Any]:
        """
        Analyze a group of transactions to ensure all monetary movements are accounted for
        Returns a report showing the breakdown of amounts
        """
        if not transactions:
            return {"error": "No transactions provided"}
        
        primary_transactions = [t for t in transactions if t.transaction_role == "primary"]
        fee_transactions = [t for t in transactions if t.transaction_role == "fee"]
        access_fee_transactions = [t for t in transactions if t.transaction_role == "access_fee"]
        fuliza_deduction_transactions = [t for t in transactions if t.transaction_role == "fuliza_deduction"]
        
        analysis = {
            "transaction_group_id": transactions[0].transaction_group_id,
            "total_transactions": len(transactions),
            "breakdown": {
                "primary_amount": sum(t.amount for t in primary_transactions),
                "total_fees": sum(t.amount for t in fee_transactions + access_fee_transactions),
                "fuliza_deductions": sum(t.amount for t in fuliza_deduction_transactions),
                "net_effect": sum(t.amount if t.type == "income" else -t.amount for t in transactions)
            },
            "transaction_roles": {
                "primary": len(primary_transactions),
                "fees": len(fee_transactions + access_fee_transactions), 
                "fuliza_deductions": len(fuliza_deduction_transactions)
            }
        }
        
        return analysis
