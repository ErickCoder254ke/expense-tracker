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
    
    # Enhanced regex patterns for M-Pesa message types (handling newer formats)
    PATTERNS = {
        # Enhanced patterns for newer M-Pesa message formats with transaction ID at start
        'modern_sent': [
            # Pattern: "TJ6CF6NDST Confirmed.Ksh30.00 sent to SIMON  NDERITU on 6/10/25 at 7:43 AM. New M-PESA balance is Ksh21.73. Transaction cost, Ksh0.00."
            r'([A-Z0-9]{6,12})\s+confirmed\.\s*(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+sent to\s+(.+?)(?:\s+(?:for account\s+(.+?))?(?:\s+on\s+([0-9/]+)\s+at\s+([0-9:]+\s*(?:AM|PM)?)))?.*?(?:new m-?pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction cost[:\s,]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?',
            # Fallback pattern for variations
            r'([A-Z0-9]{6,12})\s+confirmed\.\s*(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+sent to\s+(.+?)\s+.*?(?:new m-?pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?'
        ],

        'modern_received': [
            # Pattern: "TJ6CF6OS29 Confirmed.You have received Ksh100.00 from Equity Bulk Account 300600 on 6/10/25 at 5:19 PM New M-PESA balance is Ksh116.73."
            r'([A-Z0-9]{6,12})\s+confirmed\.\s*you have received\s+(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+from\s+(.+?)(?:\s+(?:on\s+([0-9/]+)\s+at\s+([0-9:]+\s*(?:AM|PM)?)))?.*?(?:new m-?pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?',
            # Pattern for received with phone number
            r'([A-Z0-9]{6,12})\s+confirmed\.\s*(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+received from\s+(.+?)\s+([0-9+\-\s]+).*?(?:new m-?pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?'
        ],

        # Fuliza loan pattern
        'fuliza_loan': [
            r'([A-Z0-9]{6,12})\s+confirmed\.\s*fuliza m-pesa amount is\s+(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\.*\s*access fee charged\s+(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\.*\s*total fuliza m-pesa outstanding amount is\s+(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+due on\s+([0-9/]+).*?m-pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?)'
        ],

        # Fuliza repayment pattern
        'fuliza_repayment': [
            r'([A-Z0-9]{6,12})\s+confirmed\.\s*(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+from your m-pesa has been used to.*?pay.*?fuliza.*?available fuliza m-pesa limit is\s+(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?).*?m-pesa balance is\s+(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?)'
        ],

        # Legacy received pattern (for older message formats)
        'received': [
            r'(?:you have )?received?\s+(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+from\s+(.+?)\s+([0-9+\-\s]+)\.?\s*.*?(?:new m-?pesa balance (?:is\s*)?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction (?:id|cost)[:\s]*([a-z0-9\-]{6,}))?',
            r'mpesa.*?confirmed.*?(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+received from\s+([0-9+\-\s]+)\s+(.+?)\..*?(?:new m-?pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction[:\s]*([a-z0-9\-]{6,}))?'
        ],

        # Legacy sent pattern (for older message formats)
        'sent': [
            r'(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+sent to\s+(.+?)(?:\s+on\s+[0-9/]+\s+at\s+[0-9:]+)?\.?\s*.*?(?:new m-?pesa balance (?:is\s*)?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction (?:cost|id)[:\s,]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?|[a-z0-9\-]{6,}))?',
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
        Enhanced to handle newer M-Pesa message formats
        """
        message_lower = message.lower()

        # Primary indicators
        primary_keywords = ['confirmed', 'mpesa', 'm-pesa', 'safaricom', 'fuliza']
        has_primary = any(keyword in message_lower for keyword in primary_keywords)

        # Enhanced transaction ID pattern for newer formats (letters and numbers at start)
        has_modern_transaction_id = bool(re.search(r'^[A-Z0-9]{6,12}\s+confirmed', message, re.IGNORECASE))
        has_legacy_transaction_id = bool(re.search(r'[A-Z0-9]{6,12}\s+confirmed', message, re.IGNORECASE))

        # Currency pattern (enhanced to handle variations)
        has_currency = bool(re.search(r'ksh?\.?\s*[0-9,]+(?:\.[0-9]{1,2})?', message, re.IGNORECASE))

        # Transaction action indicators
        action_keywords = ['sent to', 'received from', 'withdrawn', 'deposited', 'paid to', 'purchased']
        has_action = any(keyword in message_lower for keyword in action_keywords)

        # Balance indicator
        has_balance = 'balance' in message_lower

        # Must have primary indicator or transaction ID pattern, plus currency and action
        return (has_primary or has_modern_transaction_id or has_legacy_transaction_id) and has_currency and (has_action or has_balance)
    
    @classmethod
    def normalize_message(cls, message: str) -> str:
        """
        Normalize the message text for consistent parsing
        """
        if not message:
            return ""

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
    def clean_recipient_name(cls, recipient: str) -> Optional[str]:
        """
        Clean and format recipient name for better display
        """
        if not recipient:
            return None

        # Remove extra whitespace
        recipient = re.sub(r'\s+', ' ', recipient.strip())

        # Handle common business name patterns
        if 'SAFARICOM' in recipient.upper():
            if 'DATA BUNDLES' in recipient.upper():
                return 'Safaricom Data Bundles'
            elif 'AIRTIME' in recipient.upper():
                return 'Safaricom Airtime'
            else:
                return 'Safaricom'

        # Capitalize names properly
        words = recipient.split()
        cleaned_words = []
        for word in words:
            if word.isupper() and len(word) > 2:
                # Convert all caps to title case
                cleaned_words.append(word.title())
            elif word.islower() and len(word) > 2:
                # Convert all lowercase to title case
                cleaned_words.append(word.title())
            else:
                # Keep mixed case or short words as is
                cleaned_words.append(word)

        return ' '.join(cleaned_words)

    @classmethod
    def parse_transaction_date(cls, date_str: str, time_str: str) -> Optional[str]:
        """
        Parse transaction date and time from M-Pesa message format
        Example: "6/10/25" and "7:43 AM" -> "2025-10-06 07:43:00"
        """
        if not date_str or not time_str:
            return None

        try:
            # Handle date format: "6/10/25" (M/D/YY)
            date_parts = date_str.split('/')
            if len(date_parts) == 3:
                month, day, year = date_parts

                # Convert 2-digit year to 4-digit
                year = int(year)
                if year >= 0 and year <= 30:  # Assume 00-30 is 2000-2030
                    year += 2000
                elif year >= 70 and year <= 99:  # Assume 70-99 is 1970-1999
                    year += 1900
                else:
                    year += 2000  # Default to 2000s

                # Handle time format: "7:43 AM" or "11:51 PM"
                time_clean = time_str.strip().upper()
                if 'AM' in time_clean or 'PM' in time_clean:
                    from datetime import datetime
                    try:
                        time_obj = datetime.strptime(time_clean, '%I:%M %p').time()
                        date_obj = datetime(year, int(month), int(day), time_obj.hour, time_obj.minute)
                        return date_obj.isoformat()
                    except ValueError:
                        pass

        except (ValueError, IndexError):
            pass

        return None
    
    @classmethod
    def determine_transaction_type(cls, message: str, pattern_type: str) -> str:
        """
        Determine if transaction is income or expense based on message content
        """
        message_lower = message.lower()

        # Income indicators
        if pattern_type in ['received', 'fuliza_loan'] or any(word in message_lower for word in ['received', 'deposited', 'refund']):
            return 'income'

        # Expense indicators
        if pattern_type in ['fuliza_repayment'] or any(word in message_lower for word in ['sent', 'paid', 'withdrawn', 'purchased', 'bought', 'repay']):
            return 'expense'

        # Default to expense for ambiguous cases
        return 'expense'
    
    @classmethod
    def categorize_mpesa_transaction(cls, message: str, recipient: str = None) -> str:
        """
        Enhanced auto-categorization based on message content and recipient
        """
        message_lower = message.lower()
        recipient_lower = (recipient or "").lower()
        combined_text = message_lower + " " + recipient_lower

        # Fuliza (Loans & Credit)
        if 'fuliza' in combined_text:
            return 'Loans & Credit'

        # Utilities (Enhanced to catch data bundles, airtime, etc.)
        utilities_keywords = [
            'safaricom', 'airtel', 'telkom', 'data bundles', 'airtime', 'bundles',
            'kplc', 'electricity', 'water', 'nairobi water', 'kenya power',
            'internet', 'wifi', 'broadband'
        ]
        if any(keyword in combined_text for keyword in utilities_keywords):
            return 'Utilities'

        # Transport
        transport_keywords = [
            'uber', 'bolt', 'taxi', 'matatu', 'boda', 'fuel', 'petrol',
            'parking', 'transport', 'bus', 'travel', 'fare'
        ]
        if any(keyword in combined_text for keyword in transport_keywords):
            return 'Transport'

        # Food & Dining
        food_keywords = [
            'restaurant', 'hotel', 'food', 'cafe', 'kitchen', 'meal',
            'lunch', 'dinner', 'breakfast', 'snack', 'delivery', 'takeaway'
        ]
        if any(keyword in combined_text for keyword in food_keywords):
            return 'Food & Dining'

        # Shopping
        shopping_keywords = [
            'shop', 'store', 'market', 'supermarket', 'mall', 'outlet',
            'retail', 'purchase', 'buy', 'nakumatt', 'tuskys', 'carrefour'
        ]
        if any(keyword in combined_text for keyword in shopping_keywords):
            return 'Shopping'

        # Health
        health_keywords = [
            'hospital', 'clinic', 'pharmacy', 'medical', 'doctor', 'health',
            'medicine', 'treatment', 'consultation'
        ]
        if any(keyword in combined_text for keyword in health_keywords):
            return 'Health'

        # Education
        education_keywords = [
            'school', 'university', 'college', 'education', 'tuition',
            'fees', 'academic', 'learning', 'course'
        ]
        if any(keyword in combined_text for keyword in education_keywords):
            return 'Education'

        # Entertainment
        entertainment_keywords = [
            'cinema', 'movie', 'game', 'sport', 'entertainment', 'music',
            'concert', 'show', 'theatre', 'fun'
        ]
        if any(keyword in combined_text for keyword in entertainment_keywords):
            return 'Entertainment'

        # Banks & Financial Services
        financial_keywords = [
            'bank', 'equity', 'kcb', 'cooperative', 'barclays', 'standard chartered',
            'family bank', 'gt bank', 'loan', 'credit', 'savings', 'account'
        ]
        if any(keyword in combined_text for keyword in financial_keywords):
            return 'Financial Services'

        # Government & Official Services
        government_keywords = [
            'government', 'ministry', 'county', 'kra', 'nhif', 'nssf',
            'huduma', 'license', 'permit', 'registration'
        ]
        if any(keyword in combined_text for keyword in government_keywords):
            return 'Government & Services'

        # Personal transfers (common names pattern)
        if recipient and len(recipient.split()) >= 2:
            # Check if recipient looks like a person's name (two or more capitalized words)
            name_words = recipient.split()
            if all(word[0].isupper() for word in name_words if len(word) > 1):
                return 'Personal Transfer'

        # Bills & Fees for paybill/till transactions
        if any(keyword in combined_text for keyword in ['paybill', 'till', 'bill payment']):
            return 'Bills & Fees'

        # Default category
        return 'Other'
    
    @classmethod
    def parse_message(cls, message: str) -> Optional[Dict[str, Any]]:
        """
        Parse M-Pesa SMS message and extract transaction details
        """
        if not message or not message.strip():
            return None

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

        # Initialize variables
        amount = None
        recipient = None
        phone_number = None
        reference = None
        balance_after = None
        transaction_id = None
        transaction_fee = None
        access_fee = None
        fuliza_limit = None
        fuliza_outstanding = None
        due_date = None

        # Extract data based on pattern type
        if pattern_type == 'modern_sent':
            transaction_id = groups[0].strip() if len(groups) > 0 and groups[0] else None
            amount = cls.extract_amount(groups[1]) if len(groups) > 1 and groups[1] else None
            recipient = cls.clean_recipient_name(groups[2]) if len(groups) > 2 and groups[2] else None
            reference = groups[3] if len(groups) > 3 and groups[3] else None
            transaction_date = cls.parse_transaction_date(groups[4], groups[5]) if len(groups) > 5 and groups[4] and groups[5] else None
            balance_after = cls.extract_amount(groups[6]) if len(groups) > 6 and groups[6] else None
            transaction_fee = cls.extract_amount(groups[7]) if len(groups) > 7 and groups[7] else None

        elif pattern_type == 'modern_received':
            transaction_id = groups[0].strip() if len(groups) > 0 and groups[0] else None
            amount = cls.extract_amount(groups[1]) if len(groups) > 1 and groups[1] else None
            recipient = cls.clean_recipient_name(groups[2]) if len(groups) > 2 and groups[2] else None
            transaction_date = cls.parse_transaction_date(groups[3], groups[4]) if len(groups) > 4 and groups[3] and groups[4] else None
            balance_after = cls.extract_amount(groups[5]) if len(groups) > 5 and groups[5] else None

        elif pattern_type == 'fuliza_loan':
            transaction_id = groups[0].strip() if len(groups) > 0 and groups[0] else None
            amount = cls.extract_amount(groups[1]) if len(groups) > 1 and groups[1] else None
            access_fee = cls.extract_amount(groups[2]) if len(groups) > 2 and groups[2] else None
            fuliza_outstanding = cls.extract_amount(groups[3]) if len(groups) > 3 and groups[3] else None
            due_date = groups[4].strip() if len(groups) > 4 and groups[4] else None
            balance_after = cls.extract_amount(groups[5]) if len(groups) > 5 and groups[5] else None
            recipient = "Fuliza M-PESA Loan"

        elif pattern_type == 'fuliza_repayment':
            transaction_id = groups[0].strip() if len(groups) > 0 and groups[0] else None
            amount = cls.extract_amount(groups[1]) if len(groups) > 1 and groups[1] else None
            fuliza_limit = cls.extract_amount(groups[2]) if len(groups) > 2 and groups[2] else None
            balance_after = cls.extract_amount(groups[3]) if len(groups) > 3 and groups[3] else None
            recipient = "Fuliza M-PESA Repayment"

        elif pattern_type == 'received':
            amount = cls.extract_amount(groups[0]) if len(groups) > 0 and groups[0] else None
            recipient = cls.clean_recipient_name(groups[1]) if len(groups) > 1 and groups[1] else None
            phone_number = cls.extract_phone_number(groups[2]) if len(groups) > 2 and groups[2] else None
            balance_after = cls.extract_amount(groups[3]) if len(groups) > 3 and groups[3] else None
            transaction_id = groups[4].strip() if len(groups) > 4 and groups[4] else None

        elif pattern_type in ['sent', 'paybill', 'till']:
            amount = cls.extract_amount(groups[0]) if len(groups) > 0 and groups[0] else None
            recipient = cls.clean_recipient_name(groups[1]) if len(groups) > 1 and groups[1] else None
            if pattern_type == 'paybill':
                reference = groups[2] if len(groups) > 2 and groups[2] else None
                balance_after = cls.extract_amount(groups[4]) if len(groups) > 4 and groups[4] else None
                transaction_id = groups[5].strip() if len(groups) > 5 and groups[5] else None
            else:
                balance_after = cls.extract_amount(groups[2]) if len(groups) > 2 and groups[2] else None
                # Try to extract transaction cost/fee from the message
                fee_match = re.search(r'transaction cost[:\s,]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?)', original_message, re.IGNORECASE)
                if fee_match:
                    transaction_fee = cls.extract_amount(fee_match.group(1))
                transaction_id = groups[3].strip() if len(groups) > 3 and groups[3] else None

        elif pattern_type == 'withdrawal':
            amount = cls.extract_amount(groups[0]) if len(groups) > 0 and groups[0] else None
            recipient = cls.clean_recipient_name(groups[1]) if len(groups) > 1 and groups[1] else None
            balance_after = cls.extract_amount(groups[2]) if len(groups) > 2 and groups[2] else None
            transaction_id = groups[3].strip() if len(groups) > 3 and groups[3] else None

        elif pattern_type == 'airtime':
            amount = cls.extract_amount(groups[0]) if len(groups) > 0 and groups[0] else None
            phone_number = cls.extract_phone_number(groups[1]) if len(groups) > 1 and groups[1] else None
            recipient = f"Airtime for {phone_number}" if phone_number else "Airtime Purchase"
            balance_after = cls.extract_amount(groups[2]) if len(groups) > 2 and groups[2] else None
            transaction_id = groups[3].strip() if len(groups) > 3 and groups[3] else None

        # Determine transaction type
        transaction_type = cls.determine_transaction_type(original_message, pattern_type)

        # Generate description
        description = cls._generate_description(pattern_type, recipient, amount, reference)

        # Auto-categorize
        suggested_category = cls.categorize_mpesa_transaction(original_message, recipient)

        # Enhanced fee extraction from the original message
        enhanced_fees = cls._extract_all_fees(original_message)

        # Merge extracted fees with pattern-based fees
        if transaction_fee is None and enhanced_fees.get('transaction_fee'):
            transaction_fee = enhanced_fees['transaction_fee']
        if access_fee is None and enhanced_fees.get('access_fee'):
            access_fee = enhanced_fees['access_fee']

        # Calculate total fees
        total_fees = 0
        fee_breakdown = {}

        if transaction_fee:
            total_fees += transaction_fee
            fee_breakdown['transaction_fee'] = transaction_fee

        if access_fee:
            total_fees += access_fee
            fee_breakdown['access_fee'] = access_fee

        # Add any additional fees found
        for fee_type, fee_amount in enhanced_fees.items():
            if fee_type not in ['transaction_fee', 'access_fee'] and fee_amount > 0:
                total_fees += fee_amount
                fee_breakdown[fee_type] = fee_amount

        # Calculate parsing confidence with pattern type
        confidence = cls._calculate_confidence(original_message, amount, recipient, transaction_id, pattern_type)

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
                'message_type': pattern_type,
                'transaction_fee': transaction_fee,
                'access_fee': access_fee,
                'fuliza_limit': fuliza_limit,
                'fuliza_outstanding': fuliza_outstanding,
                'due_date': due_date
            },
            'parsing_confidence': confidence,
            'original_message_hash': cls._hash_message(original_message),
            'requires_review': confidence < 0.8,
            'sms_metadata': {
                'total_fees': total_fees if total_fees > 0 else None,
                'fee_breakdown': fee_breakdown if fee_breakdown else None,
                'parsing_confidence': confidence,
                'original_message_hash': cls._hash_message(original_message),
                'requires_review': confidence < 0.8,
                'suggested_category': suggested_category
            }
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
        transaction_id = transaction_id_match.group(1).strip() if transaction_id_match and transaction_id_match.group(1) else None
        
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
        if pattern_type == 'fuliza_loan':
            return f"Fuliza Loan - Ksh {amount}"
        elif pattern_type == 'fuliza_repayment':
            return f"Fuliza Repayment - Ksh {amount}"
        elif pattern_type == 'received':
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
    def _calculate_confidence(cls, message: str, amount: float, recipient: str, transaction_id: str, pattern_type: str = None) -> float:
        """
        Enhanced confidence calculation for better accuracy assessment
        """
        confidence = 0.0

        # Base confidence for M-Pesa message (higher for modern formats)
        if cls.is_mpesa_message(message):
            # Higher confidence for modern transaction ID formats
            if re.search(r'^[A-Z0-9]{6,12}\s+confirmed', message, re.IGNORECASE):
                confidence += 0.4  # Modern format
            else:
                confidence += 0.3  # Legacy format

        # Amount extracted and reasonable
        if amount and amount > 0:
            if amount >= 1 and amount <= 500000:  # Reasonable M-Pesa limits
                confidence += 0.3
            else:
                confidence += 0.2  # Amount seems unreasonable

        # Recipient/description quality
        if recipient and len(recipient.strip()) > 2:
            recipient_clean = recipient.strip()
            if len(recipient_clean) > 5 and not recipient_clean.isdigit():
                confidence += 0.2  # Good quality recipient name
            else:
                confidence += 0.1  # Basic recipient info

        # Transaction ID quality
        if transaction_id and len(transaction_id.strip()) >= 6:
            # Modern transaction IDs are more reliable
            if re.match(r'^[A-Z0-9]{8,12}$', transaction_id.strip()):
                confidence += 0.2  # Good quality transaction ID
            else:
                confidence += 0.1  # Basic transaction ID

        # Pattern type bonus (modern patterns are more reliable)
        if pattern_type in ['modern_sent', 'modern_received']:
            confidence += 0.1

        # Additional validation checks
        message_lower = message.lower()

        # Check for balance information (increases confidence)
        if 'new m-pesa balance' in message_lower or 'balance is' in message_lower:
            confidence += 0.05

        # Check for transaction cost information (increases confidence)
        if 'transaction cost' in message_lower:
            confidence += 0.05

        # Check for date/time information (increases confidence)
        if re.search(r'\d{1,2}/\d{1,2}/\d{2,4}', message) and re.search(r'\d{1,2}:\d{2}\s*(?:AM|PM)', message, re.IGNORECASE):
            confidence += 0.05

        return min(confidence, 1.0)
    
    @classmethod
    def _extract_all_fees(cls, message: str) -> Dict[str, float]:
        """
        Enhanced fee extraction to capture all possible fees from M-Pesa messages
        """
        fees = {}
        message_lower = message.lower()

        # Enhanced transaction cost patterns
        transaction_fee_patterns = [
            r'transaction cost[:\s,]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?)',
            r'transaction fee[:\s,]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?)',
            r'charge[:\s,]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?)',
            r'cost[:\s,]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?)',
        ]

        for pattern in transaction_fee_patterns:
            match = re.search(pattern, message_lower)
            if match:
                fee = cls.extract_amount(match.group(1))
                if fee and fee > 0:
                    fees['transaction_fee'] = fee
                    break

        # Enhanced access fee patterns (Fuliza)
        access_fee_patterns = [
            r'access fee charged[:\s]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?)',
            r'access fee[:\s]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?)',
            r'fuliza.*?fee[:\s]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?)',
        ]

        for pattern in access_fee_patterns:
            match = re.search(pattern, message_lower)
            if match:
                fee = cls.extract_amount(match.group(1))
                if fee and fee > 0:
                    fees['access_fee'] = fee
                    break

        # Additional fee types

        # Service fee pattern
        service_fee_match = re.search(r'service fee[:\s]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?)', message_lower)
        if service_fee_match:
            fee = cls.extract_amount(service_fee_match.group(1))
            if fee and fee > 0:
                fees['service_fee'] = fee

        # Processing fee pattern
        processing_fee_match = re.search(r'processing fee[:\s]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?)', message_lower)
        if processing_fee_match:
            fee = cls.extract_amount(processing_fee_match.group(1))
            if fee and fee > 0:
                fees['processing_fee'] = fee

        # ATM fee pattern
        atm_fee_match = re.search(r'atm fee[:\s]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?)', message_lower)
        if atm_fee_match:
            fee = cls.extract_amount(atm_fee_match.group(1))
            if fee and fee > 0:
                fees['atm_fee'] = fee

        # Bank charges pattern
        bank_charge_match = re.search(r'bank charge[s]?[:\s]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?)', message_lower)
        if bank_charge_match:
            fee = cls.extract_amount(bank_charge_match.group(1))
            if fee and fee > 0:
                fees['bank_charge'] = fee

        return fees

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
