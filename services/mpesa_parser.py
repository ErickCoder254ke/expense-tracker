import re
import hashlib
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from models.transaction import TransactionCreate, MPesaDetails
import phonenumbers
from phonenumbers import NumberParseException

class MPesaParser:
    """
    Robust M-Pesa SMS message parser that handles various Kenyan Safaricom M-Pesa formats
    """
    
    # Common M-Pesa keywords that indicate transaction messages
    MPESA_KEYWORDS = [
        'mpesa', 'm-pesa', 'safaricom', 'paybill', 'till', 'lipa na mpesa',
        'transaction id', 'receipt', 'confirmed', 'sent to', 'received from',
        'withdrawn', 'deposited', 'balance', 'ksh', 'kes'
    ]
    
    # Regex patterns for different M-Pesa message types
    PATTERNS = {
        # Money received pattern
        'received': [
            r'(?:you have )?received?\s+(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+from\s+(.+?)\s+([0-9+\-\s]+)\.?\s*.*?(?:new m-?pesa balance (?:is\s*)?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction (?:id|cost)[:\s]*([a-z0-9\-]{6,}))?',
            r'mpesa.*?confirmed.*?(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+received from\s+([0-9+\-\s]+)\s+(.+?)\..*?(?:new m-?pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction[:\s]*([a-z0-9\-]{6,}))?'
        ],
        
        # Money sent/payment pattern
        'sent': [
            r'(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+sent to\s+(.+?)(?:\s+on\s+[0-9/]+\s+at\s+[0-9:]+)?\.?\s*.*?(?:new m-?pesa balance (?:is\s*)?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction (?:id)[:\s]*([a-z0-9\-]{6,}))?',
            r'you have paid\s+(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+to\s+(.+?)\..*?(?:account number[:\s]*([a-z0-9\-]+))?.*?(?:new m-?pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction[:\s]*([a-z0-9\-]{6,}))?'
        ],
        
        # Withdrawal pattern
        'withdrawal': [
            r'(?:you have )?withdrawn?\s+(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+from\s+(.+?)\.?\s*.*?(?:new m-?pesa balance (?:is\s*)?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction (?:id)[:\s]*([a-z0-9\-]{6,}))?'
        ],
        
        # Airtime purchase pattern
        'airtime': [
            r'(?:you have )?purchased airtime\s+(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+for\s+([0-9+\-\s]+)\.?\s*.*?(?:new m-?pesa balance (?:is\s*)?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction[:\s]*([a-z0-9\-]{6,}))?'
        ],
        
        # Paybill/Till number pattern
        'paybill': [
            r'(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+(?:sent to|paid to)\s+(.+?)\s+paybill\s+([0-9]+).*?(?:account(?:\s+number)?[:\s]*([a-z0-9\-]+))?.*?(?:new m-?pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction[:\s]*([a-z0-9\-]{6,}))?'
        ],
        
        # Till number pattern
        'till': [
            r'(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+(?:sent to|paid to)\s+(.+?)\s+till\s+([0-9]+).*?(?:new m-?pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction[:\s]*([a-z0-9\-]{6,}))?'
        ]
    }
    
    @classmethod
    def is_mpesa_message(cls, message: str) -> bool:
        """
        Check if the message is likely an M-Pesa transaction message
        """
        message_lower = message.lower()
        return any(keyword in message_lower for keyword in cls.MPESA_KEYWORDS)
    
    @classmethod
    def normalize_message(cls, message: str) -> str:
        """
        Normalize the message text for consistent parsing
        """
        # Convert to lowercase
        message = message.lower()
        
        # Remove extra whitespace and line breaks
        message = re.sub(r'\s+', ' ', message).strip()
        
        # Normalize currency symbols
        message = re.sub(r'ksh\.?s?', 'ksh', message)
        message = re.sub(r'kes\.?', 'kes', message)
        
        # Normalize punctuation
        message = re.sub(r'[.,;!]+\s*$', '', message)
        
        return message
    
    @classmethod
    def extract_amount(cls, amount_str: str) -> Optional[float]:
        """
        Extract and parse amount from string
        """
        if not amount_str:
            return None
            
        # Remove commas and whitespace
        amount_str = re.sub(r'[,\s]', '', amount_str)
        
        try:
            return float(amount_str)
        except ValueError:
            return None
    
    @classmethod
    def extract_phone_number(cls, phone_str: str) -> Optional[str]:
        """
        Extract and format phone number from string
        """
        if not phone_str:
            return None
            
        # Clean up the phone number string
        phone_str = re.sub(r'[^\d+]', '', phone_str)
        
        try:
            # Parse with Kenya country code
            parsed = phonenumbers.parse(phone_str, "KE")
            if phonenumbers.is_valid_number(parsed):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except NumberParseException:
            pass
        
        # Fallback: return cleaned number if it looks reasonable
        if re.match(r'^(\+254|254|0)\d{9}$', phone_str):
            return phone_str
            
        return None
    
    @classmethod
    def determine_transaction_type(cls, message: str, pattern_type: str) -> str:
        """
        Determine if transaction is income or expense based on message content
        """
        message_lower = message.lower()
        
        # Income indicators
        if pattern_type == 'received' or any(word in message_lower for word in ['received', 'deposited', 'refund']):
            return 'income'
        
        # Expense indicators
        if any(word in message_lower for word in ['sent', 'paid', 'withdrawn', 'purchased', 'bought']):
            return 'expense'
        
        # Default to expense for ambiguous cases
        return 'expense'
    
    @classmethod
    def categorize_mpesa_transaction(cls, message: str, recipient: str = None) -> str:
        """
        Auto-categorize M-Pesa transaction based on message content
        """
        message_lower = message.lower()
        recipient_lower = (recipient or "").lower()
        
        # Transport
        if any(word in message_lower for word in ['uber', 'taxi', 'matatu', 'fuel', 'parking', 'transport']):
            return 'Transport'
        
        # Utilities
        if any(word in message_lower + recipient_lower for word in ['kplc', 'electricity', 'water', 'safaricom', 'airtel', 'telkom', 'airtime']):
            return 'Utilities'
        
        # Food
        if any(word in message_lower + recipient_lower for word in ['restaurant', 'hotel', 'food', 'cafe', 'kitchen']):
            return 'Food & Dining'
        
        # Shopping
        if any(word in message_lower + recipient_lower for word in ['shop', 'store', 'market', 'supermarket']):
            return 'Shopping'
        
        # Health
        if any(word in message_lower + recipient_lower for word in ['hospital', 'clinic', 'pharmacy', 'medical']):
            return 'Health'
        
        # Education
        if any(word in message_lower + recipient_lower for word in ['school', 'university', 'college', 'education']):
            return 'Education'
        
        # Entertainment
        if any(word in message_lower + recipient_lower for word in ['cinema', 'movie', 'game', 'sport']):
            return 'Entertainment'
        
        # Default to Bills & Fees for paybill/till transactions
        if any(word in message_lower for word in ['paybill', 'till']):
            return 'Bills & Fees'
        
        # Default category
        return 'Other'
    
    @classmethod
    def parse_message(cls, message: str) -> Optional[Dict[str, Any]]:
        """
        Parse M-Pesa SMS message and extract transaction details
        """
        if not cls.is_mpesa_message(message):
            return None
        
        original_message = message
        normalized_message = cls.normalize_message(message)
        
        # Try each pattern type
        for pattern_type, patterns in cls.PATTERNS.items():
            for pattern in patterns:
                match = re.search(pattern, normalized_message, re.IGNORECASE)
                if match:
                    return cls._extract_transaction_details(
                        original_message, normalized_message, match, pattern_type
                    )
        
        # If no pattern matches but it's clearly an M-Pesa message, try generic extraction
        return cls._generic_extraction(original_message, normalized_message)
    
    @classmethod
    def _extract_transaction_details(cls, original_message: str, normalized_message: str, 
                                   match: re.Match, pattern_type: str) -> Dict[str, Any]:
        """
        Extract transaction details from regex match
        """
        groups = match.groups()
        
        # Basic extraction based on pattern type
        amount = cls.extract_amount(groups[0]) if len(groups) > 0 else None
        
        # Extract recipient/sender info
        recipient = None
        phone_number = None
        reference = None
        
        if pattern_type == 'received':
            recipient = groups[1].strip() if len(groups) > 1 else None
            phone_number = cls.extract_phone_number(groups[2]) if len(groups) > 2 else None
            balance_after = cls.extract_amount(groups[3]) if len(groups) > 3 else None
            transaction_id = groups[4].strip() if len(groups) > 4 else None
        elif pattern_type in ['sent', 'paybill', 'till']:
            recipient = groups[1].strip() if len(groups) > 1 else None
            if pattern_type == 'paybill':
                reference = groups[2] if len(groups) > 2 else None  # Paybill number
                balance_after = cls.extract_amount(groups[4]) if len(groups) > 4 else None
                transaction_id = groups[5].strip() if len(groups) > 5 else None
            else:
                balance_after = cls.extract_amount(groups[2]) if len(groups) > 2 else None
                transaction_id = groups[3].strip() if len(groups) > 3 else None
        elif pattern_type == 'withdrawal':
            recipient = groups[1].strip() if len(groups) > 1 else None
            balance_after = cls.extract_amount(groups[2]) if len(groups) > 2 else None
            transaction_id = groups[3].strip() if len(groups) > 3 else None
        elif pattern_type == 'airtime':
            phone_number = cls.extract_phone_number(groups[1]) if len(groups) > 1 else None
            recipient = f"Airtime for {phone_number}" if phone_number else "Airtime Purchase"
            balance_after = cls.extract_amount(groups[2]) if len(groups) > 2 else None
            transaction_id = groups[3].strip() if len(groups) > 3 else None
        
        # Determine transaction type
        transaction_type = cls.determine_transaction_type(original_message, pattern_type)
        
        # Generate description
        description = cls._generate_description(pattern_type, recipient, amount, reference)
        
        # Auto-categorize
        suggested_category = cls.categorize_mpesa_transaction(original_message, recipient)
        
        # Calculate parsing confidence
        confidence = cls._calculate_confidence(original_message, amount, recipient, transaction_id)
        
        return {
            'amount': amount,
            'type': transaction_type,
            'description': description,
            'suggested_category': suggested_category,
            'mpesa_details': {
                'recipient': recipient,
                'reference': reference,
                'transaction_id': transaction_id,
                'phone_number': phone_number,
                'balance_after': balance_after,
                'message_type': pattern_type
            },
            'parsing_confidence': confidence,
            'original_message_hash': cls._hash_message(original_message),
            'requires_review': confidence < 0.8
        }
    
    @classmethod
    def _generic_extraction(cls, original_message: str, normalized_message: str) -> Optional[Dict[str, Any]]:
        """
        Generic extraction for messages that don't match specific patterns
        """
        # Try to extract amount
        amount_match = re.search(r'(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)', normalized_message)
        if not amount_match:
            return None
        
        amount = cls.extract_amount(amount_match.group(1))
        if not amount:
            return None
        
        # Try to extract transaction ID
        transaction_id_match = re.search(r'(?:transaction|receipt|ref)[:\s]*([a-z0-9\-]{6,})', normalized_message)
        transaction_id = transaction_id_match.group(1) if transaction_id_match else None
        
        # Determine transaction type
        transaction_type = cls.determine_transaction_type(original_message, 'generic')
        
        return {
            'amount': amount,
            'type': transaction_type,
            'description': f"M-Pesa Transaction - {amount}",
            'suggested_category': 'Other',
            'mpesa_details': {
                'recipient': None,
                'reference': None,
                'transaction_id': transaction_id,
                'phone_number': None,
                'balance_after': None,
                'message_type': 'generic'
            },
            'parsing_confidence': 0.4,  # Low confidence for generic extraction
            'original_message_hash': cls._hash_message(original_message),
            'requires_review': True
        }
    
    @classmethod
    def _generate_description(cls, pattern_type: str, recipient: str, amount: float, reference: str = None) -> str:
        """
        Generate a human-readable description for the transaction
        """
        if pattern_type == 'received':
            return f"Received from {recipient}" if recipient else "Money Received"
        elif pattern_type == 'sent':
            return f"Sent to {recipient}" if recipient else "Money Sent"
        elif pattern_type == 'withdrawal':
            return f"Withdrawal from {recipient}" if recipient else "Cash Withdrawal"
        elif pattern_type == 'airtime':
            return f"Airtime Purchase"
        elif pattern_type == 'paybill':
            desc = f"Payment to {recipient}" if recipient else "Paybill Payment"
            if reference:
                desc += f" (Ref: {reference})"
            return desc
        elif pattern_type == 'till':
            return f"Payment to {recipient}" if recipient else "Till Payment"
        else:
            return f"M-Pesa Transaction"
    
    @classmethod
    def _calculate_confidence(cls, message: str, amount: float, recipient: str, transaction_id: str) -> float:
        """
        Calculate parsing confidence score (0.0 - 1.0)
        """
        confidence = 0.0
        
        # Base confidence for M-Pesa message
        if cls.is_mpesa_message(message):
            confidence += 0.3
        
        # Amount extracted
        if amount and amount > 0:
            confidence += 0.3
        
        # Recipient/description available
        if recipient and len(recipient.strip()) > 2:
            confidence += 0.2
        
        # Transaction ID available
        if transaction_id and len(transaction_id.strip()) >= 6:
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    @classmethod
    def _hash_message(cls, message: str) -> str:
        """
        Generate a hash of the message for duplicate detection
        """
        return hashlib.md5(message.encode('utf-8')).hexdigest()
    
    @classmethod
    def create_transaction_from_sms(cls, message: str, user_id: str, category_id: str = None) -> Optional[TransactionCreate]:
        """
        Parse SMS message and create a TransactionCreate object
        """
        parsed_data = cls.parse_message(message)
        if not parsed_data:
            return None
        
        # Create M-Pesa details
        mpesa_details = MPesaDetails(
            recipient=parsed_data['mpesa_details'].get('recipient'),
            reference=parsed_data['mpesa_details'].get('reference'),
            transaction_id=parsed_data['mpesa_details'].get('transaction_id')
        )
        
        # Use provided category or suggest one
        final_category_id = category_id or "auto"  # Will be resolved by categorization service
        
        return TransactionCreate(
            amount=parsed_data['amount'],
            type=parsed_data['type'],
            category_id=final_category_id,
            description=parsed_data['description'],
            date=datetime.now(),  # SMS doesn't usually contain full date, use current time
            mpesa_details=mpesa_details
        )
