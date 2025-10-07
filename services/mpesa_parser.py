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
            # Enhanced pattern for modern sent messages with transaction ID at start
            # Pattern: "TJ6CF6NDST Confirmed.Ksh30.00 sent to SIMON  NDERITU on 6/10/25 at 7:43 AM. New M-PESA balance is Ksh21.73. Transaction cost, Ksh0.00."
            r'([A-Z0-9]{6,12})\s+confirmed\.\s*(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+sent to\s+(.+?)(?:\s+for account\s+(.+?))?(?:\s+on\s+([0-9/\-]+)\s+at\s+([0-9:]+\s*(?:AM|PM)?)).*?(?:new m-?pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction cost[:\s,]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?',
            # Pattern for KPLC PREPAID and similar service payments
            # "TJ4CF6I7HN Confirmed. Ksh100.00 sent to KPLC PREPAID for account 54405080323 on 4/10/25 at 4:38 PM"
            r'([A-Z0-9]{6,12})\s+confirmed\.\s*(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+sent to\s+(.+?)\s+for account\s+(.+?)\s+on\s+([0-9/\-]+)\s+at\s+([0-9:]+\s*(?:AM|PM)?).*?(?:new m-?pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction cost[:\s,]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?',
            # Fallback pattern for variations without specific account
            r'([A-Z0-9]{6,12})\s+confirmed\.\s*(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+sent to\s+(.+?)\s+on\s+([0-9/\-]+)\s+at\s+([0-9:]+\s*(?:AM|PM)?).*?(?:new m-?pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?.*?(?:transaction cost[:\s,]*(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?'
        ],

        'modern_received': [
            # Enhanced pattern for modern received messages
            # "TJ3CF6GKC7 Confirmed.You have received Ksh100.00 from Equity Bulk Account 300600 on 3/10/25 at 10:55 PM New M-PESA balance is Ksh111.86."
            r'([A-Z0-9]{6,12})\s+confirmed\.\s*you have received\s+(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+from\s+(.+?)\s+(?:([0-9]+)\s+)?on\s+([0-9/\-]+)\s+at\s+([0-9:]+\s*(?:AM|PM)?).*?(?:new m-?pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?',
            # Alternative pattern without explicit date/time
            r'([A-Z0-9]{6,12})\s+confirmed\.\s*you have received\s+(?:ksh?|kes)\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+from\s+(.+?).*?(?:new m-?pesa balance.*?(?:ksh?|kes)?\s*([0-9,]+(?:\.[0-9]{1,2})?))?',
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
        Enhanced transaction date and time parsing from M-Pesa message format
        Handles multiple formats: "6/10/25" and "7:43 AM" -> "2025-10-06 07:43:00"
        Also handles "3/10/25" format and various time formats
        """
        if not date_str or not time_str:
            return None

        try:
            # Handle different date formats
            date_patterns = [
                r'(\d{1,2})/(\d{1,2})/(\d{2,4})',  # M/D/YY or MM/DD/YYYY
                r'(\d{1,2})-(\d{1,2})-(\d{2,4})',  # M-D-YY or MM-DD-YYYY
            ]

            date_match = None
            for pattern in date_patterns:
                match = re.search(pattern, date_str)
                if match:
                    date_match = match
                    break

            if date_match:
                month, day, year = date_match.groups()

                # Convert to integers
                month = int(month)
                day = int(day)
                year = int(year)

                # Convert 2-digit year to 4-digit (enhanced logic)
                if year < 100:
                    current_year = datetime.now().year
                    current_century = current_year // 100 * 100

                    # If year is within 10 years in the future, assume current century
                    if year <= (current_year % 100) + 10:
                        year += current_century
                    else:
                        year += current_century - 100

                # Enhanced time parsing
                time_patterns = [
                    r'(\d{1,2}):(\d{2})\s*(AM|PM)',  # 7:43 AM or 11:51 PM
                    r'(\d{1,2}):(\d{2})',  # 24-hour format 07:43
                ]

                time_match = None
                for pattern in time_patterns:
                    match = re.search(pattern, time_str.upper())
                    if match:
                        time_match = match
                        break

                if time_match:
                    hour = int(time_match.group(1))
                    minute = int(time_match.group(2))

                    # Handle AM/PM conversion
                    if len(time_match.groups()) >= 3 and time_match.group(3):
                        am_pm = time_match.group(3).upper()
                        if am_pm == 'PM' and hour != 12:
                            hour += 12
                        elif am_pm == 'AM' and hour == 12:
                            hour = 0

                    # Validate date and time
                    if 1 <= month <= 12 and 1 <= day <= 31 and 0 <= hour <= 23 and 0 <= minute <= 59:
                        date_obj = datetime(year, month, day, hour, minute)
                        return date_obj.isoformat()

        except (ValueError, IndexError) as e:
            # Log the error for debugging
            print(f"Date parsing error: {e} for date_str='{date_str}', time_str='{time_str}'")
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
        Includes comprehensive Kenyan service providers and paybill numbers
        """
        message_lower = message.lower()
        recipient_lower = (recipient or "").lower()
        combined_text = message_lower + " " + recipient_lower

        # Extract paybill number for specific categorization
        paybill_match = re.search(r'paybill\s+(\d+)', combined_text)
        paybill_number = paybill_match.group(1) if paybill_match else None

        # Known Kenyan Utility Paybill Numbers
        utility_paybills = {
            '888880': 'Utilities',  # KPLC Prepaid
            '888888': 'Utilities',  # KPLC Postpaid
            '444400': 'Utilities',  # Nairobi Water
            '895500': 'Utilities',  # Mombasa Water
            '517000': 'Utilities',  # Kisumu Water
            '111444': 'Utilities',  # Nakuru Water
            '511000': 'Utilities',  # Eldoret Water
            '885100': 'Utilities',  # Kiambu Water
            '880600': 'Utilities',  # Garissa Water
            '363100': 'Utilities',  # Mavoko Water
            '200200': 'Telecommunications',  # Safaricom
        }

        # Check paybill number first for exact matches
        if paybill_number and paybill_number in utility_paybills:
            return utility_paybills[paybill_number]

        # Fuliza (Loans & Credit)
        if 'fuliza' in combined_text:
            return 'Loans & Credit'

        # Enhanced Utilities categorization
        utility_keywords = [
            # Electricity
            'kplc', 'kenya power', 'electricity', 'prepaid', 'postpaid', 'power',
            # Water
            'water', 'nairobi water', 'mombasa water', 'kisumu water', 'nakuru water',
            'eldoret water', 'kiambu water', 'garissa water', 'mavoko water', 'ncwsc',
            # Telecommunications & Internet
            'safaricom', 'airtel', 'telkom', 'data bundles', 'airtime', 'bundles',
            'internet', 'wifi', 'broadband', 'faiba', 'zuku', 'wananchi',
            # Gas
            'gas', 'lpg', 'cooking gas'
        ]
        if any(keyword in combined_text for keyword in utility_keywords):
            return 'Utilities'

        # Enhanced Transportation
        transport_keywords = [
            'uber', 'bolt', 'taxi', 'matatu', 'boda', 'boda boda', 'fuel', 'petrol',
            'parking', 'transport', 'bus', 'travel', 'fare', 'sgr', 'railway',
            'kenya airways', 'jambojet', 'fly540', 'flight', 'airline'
        ]
        if any(keyword in combined_text for keyword in transport_keywords):
            return 'Transport'

        # Food & Dining
        food_keywords = [
            'restaurant', 'hotel', 'food', 'cafe', 'kitchen', 'meal',
            'lunch', 'dinner', 'breakfast', 'snack', 'delivery', 'takeaway',
            'kfc', 'pizza', 'subway', 'java', 'artcaffe', 'chicken inn'
        ]
        if any(keyword in combined_text for keyword in food_keywords):
            return 'Food & Dining'

        # Enhanced Shopping
        shopping_keywords = [
            'shop', 'store', 'market', 'supermarket', 'mall', 'outlet',
            'retail', 'purchase', 'buy', 'nakumatt', 'tuskys', 'carrefour',
            'naivas', 'chandarana', 'quickmart', 'cleanshelf', 'eastmatt'
        ]
        if any(keyword in combined_text for keyword in shopping_keywords):
            return 'Shopping'

        # Health & Medical
        health_keywords = [
            'hospital', 'clinic', 'pharmacy', 'medical', 'doctor', 'health',
            'medicine', 'treatment', 'consultation', 'nhif', 'aga khan',
            'nairobi hospital', 'kenyatta hospital', 'mater hospital'
        ]
        if any(keyword in combined_text for keyword in health_keywords):
            return 'Health'

        # Education
        education_keywords = [
            'school', 'university', 'college', 'education', 'tuition',
            'fees', 'academic', 'learning', 'course', 'uon', 'ku', 'mku',
            'strathmore', 'usiu', 'kabarak'
        ]
        if any(keyword in combined_text for keyword in education_keywords):
            return 'Education'

        # Entertainment & Recreation
        entertainment_keywords = [
            'cinema', 'movie', 'game', 'sport', 'entertainment', 'music',
            'concert', 'show', 'theatre', 'fun', 'betting', 'sportpesa',
            'betin', 'mcheza', 'club', 'disco'
        ]
        if any(keyword in combined_text for keyword in entertainment_keywords):
            return 'Entertainment'

        # Enhanced Banks & Financial Services
        financial_keywords = [
            'bank', 'equity', 'kcb', 'cooperative', 'barclays', 'standard chartered',
            'family bank', 'gt bank', 'loan', 'credit', 'savings', 'account',
            'ncba', 'diamond trust', 'i&m bank', 'housing finance', 'sidian bank',
            'centum', 'sacco'
        ]
        if any(keyword in combined_text for keyword in financial_keywords):
            return 'Financial Services'

        # Government & Official Services
        government_keywords = [
            'government', 'ministry', 'county', 'kra', 'nhif', 'nssf',
            'huduma', 'license', 'permit', 'registration', 'ntsa', 'lands',
            'attorney general', 'court', 'police', 'immigration'
        ]
        if any(keyword in combined_text for keyword in government_keywords):
            return 'Government & Services'

        # Personal transfers (enhanced detection)
        if recipient and len(recipient.split()) >= 2:
            # Check if recipient looks like a person's name
            name_words = recipient.split()
            # More sophisticated name detection
            if (len(name_words) >= 2 and
                all(word.isalpha() and len(word) > 1 for word in name_words) and
                all(word[0].isupper() for word in name_words)):
                return 'Personal Transfer'

        # Bills & Fees for paybill/till transactions (fallback)
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

        # Initialize transaction_date
        transaction_date = None

        # Extract data based on pattern type
        if pattern_type == 'modern_sent':
            transaction_id = groups[0].strip() if len(groups) > 0 and groups[0] else None
            amount = cls.extract_amount(groups[1]) if len(groups) > 1 and groups[1] else None
            recipient = cls.clean_recipient_name(groups[2]) if len(groups) > 2 and groups[2] else None

            # Handle different pattern variations
            if len(groups) >= 8:  # Full pattern with account and date
                reference = groups[3] if groups[3] else None
                transaction_date = cls.parse_transaction_date(groups[4], groups[5]) if groups[4] and groups[5] else None
                balance_after = cls.extract_amount(groups[6]) if groups[6] else None
                transaction_fee = cls.extract_amount(groups[7]) if groups[7] else None
            elif len(groups) >= 6:  # Pattern without account but with date
                reference = None
                transaction_date = cls.parse_transaction_date(groups[3], groups[4]) if groups[3] and groups[4] else None
                balance_after = cls.extract_amount(groups[5]) if groups[5] else None
                transaction_fee = None
            else:
                reference = None
                balance_after = cls.extract_amount(groups[3]) if len(groups) > 3 and groups[3] else None
                transaction_fee = None

        elif pattern_type == 'modern_received':
            transaction_id = groups[0].strip() if len(groups) > 0 and groups[0] else None
            amount = cls.extract_amount(groups[1]) if len(groups) > 1 and groups[1] else None
            recipient = cls.clean_recipient_name(groups[2]) if len(groups) > 2 and groups[2] else None

            # Handle different pattern variations for received messages
            if len(groups) >= 7:  # Full pattern with account number and date
                reference = groups[3] if groups[3] else None  # Account number
                transaction_date = cls.parse_transaction_date(groups[4], groups[5]) if groups[4] and groups[5] else None
                balance_after = cls.extract_amount(groups[6]) if groups[6] else None
            elif len(groups) >= 5:  # Pattern with date but no account
                reference = None
                transaction_date = cls.parse_transaction_date(groups[3], groups[4]) if groups[3] and groups[4] else None
                balance_after = cls.extract_amount(groups[5]) if len(groups) > 5 and groups[5] else None
            else:
                reference = None
                balance_after = cls.extract_amount(groups[3]) if len(groups) > 3 and groups[3] else None

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
            'transaction_date': transaction_date,  # Include extracted transaction date
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
                'suggested_category': suggested_category,
                'parsed_at': datetime.now().isoformat()
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
        Enhanced to provide more detailed descriptions
        """
        if pattern_type == 'fuliza_loan':
            return f"Fuliza Loan - KSh {amount:,.2f}"
        elif pattern_type == 'fuliza_repayment':
            return f"Fuliza Repayment - KSh {amount:,.2f}"
        elif pattern_type in ['received', 'modern_received']:
            if recipient:
                # Special handling for common senders
                if 'equity' in recipient.lower():
                    return f"Received from {recipient}"
                elif 'bulk account' in recipient.lower():
                    return f"Received from {recipient}"
                else:
                    return f"Received from {recipient}"
            return "Money Received"
        elif pattern_type in ['sent', 'modern_sent']:
            if recipient:
                # Enhanced descriptions for common recipients
                if 'kplc' in recipient.lower() or 'kenya power' in recipient.lower():
                    desc = f"Electricity Payment - {recipient}"
                    if reference:
                        desc += f" (Account: {reference})"
                    return desc
                elif 'safaricom' in recipient.lower():
                    if 'data' in recipient.lower():
                        return f"Data Bundle Purchase - {recipient}"
                    else:
                        return f"Airtime Purchase - {recipient}"
                elif 'water' in recipient.lower():
                    desc = f"Water Bill Payment - {recipient}"
                    if reference:
                        desc += f" (Account: {reference})"
                    return desc
                else:
                    desc = f"Payment to {recipient}"
                    if reference:
                        desc += f" (Ref: {reference})"
                    return desc
            return "Money Sent"
        elif pattern_type == 'withdrawal':
            return f"Cash Withdrawal - {recipient}" if recipient else "Cash Withdrawal"
        elif pattern_type == 'airtime':
            return "Airtime Purchase"
        elif pattern_type == 'paybill':
            desc = f"Paybill Payment - {recipient}" if recipient else "Paybill Payment"
            if reference:
                desc += f" (Account: {reference})"
            return desc
        elif pattern_type == 'till':
            return f"Till Payment - {recipient}" if recipient else "Till Payment"
        else:
            return "M-Pesa Transaction"
    
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
    def test_enhanced_parsing(cls) -> Dict[str, Any]:
        """
        Test the enhanced parsing with the provided user examples
        """
        test_messages = [
            "TJ3CF6GKC7 Confirmed.You have received Ksh100.00 from Equity Bulk Account 300600 on 3/10/25 at 10:55 PM New M-PESA balance is Ksh111.86.  Separate personal and business funds through Pochi la Biashara on *334#.",
            "TJ3CF6GITN Confirmed.You have received Ksh99.00 from Equity Bulk Account 300600 on 3/10/25 at 10:56 PM New M-PESA balance is Ksh210.86.  Separate personal and business funds through Pochi la Biashara on *334#.",
            "TJ4CF6I7HN Confirmed. Ksh100.00 sent to KPLC PREPAID for account 54405080323 on 4/10/25 at 4:38 PM New M-PESA balance is Ksh110.86. Transaction cost, Ksh0.00.Amount you can transact within the day is 499,900.00. Save frequent paybills for quick payment on M-PESA app https://bit.ly/mpesalnk"
        ]

        results = []
        for i, message in enumerate(test_messages, 1):
            print(f"\n=== Testing Message {i} ===")
            print(f"Original: {message[:100]}...")

            parsed = cls.parse_message(message)
            if parsed:
                results.append({
                    'message_number': i,
                    'success': True,
                    'amount': parsed['amount'],
                    'type': parsed['type'],
                    'description': parsed['description'],
                    'suggested_category': parsed['suggested_category'],
                    'transaction_date': parsed.get('transaction_date'),
                    'recipient': parsed['mpesa_details']['recipient'],
                    'transaction_id': parsed['mpesa_details']['transaction_id'],
                    'reference': parsed['mpesa_details']['reference'],
                    'balance_after': parsed['mpesa_details']['balance_after'],
                    'transaction_fee': parsed['mpesa_details']['transaction_fee'],
                    'confidence': parsed['parsing_confidence'],
                    'requires_review': parsed['requires_review']
                })

                print(f"✅ SUCCESS")
                print(f"Amount: KSh {parsed['amount']}")
                print(f"Type: {parsed['type']}")
                print(f"Description: {parsed['description']}")
                print(f"Category: {parsed['suggested_category']}")
                print(f"Date: {parsed.get('transaction_date', 'Not extracted')}")
                print(f"Recipient: {parsed['mpesa_details']['recipient']}")
                print(f"Transaction ID: {parsed['mpesa_details']['transaction_id']}")
                print(f"Reference: {parsed['mpesa_details']['reference']}")
                print(f"Balance After: {parsed['mpesa_details']['balance_after']}")
                print(f"Transaction Fee: {parsed['mpesa_details']['transaction_fee']}")
                print(f"Confidence: {parsed['parsing_confidence']:.2f}")
            else:
                results.append({
                    'message_number': i,
                    'success': False,
                    'error': 'Failed to parse message'
                })
                print(f"❌ FAILED to parse message")

        return {
            'total_tested': len(test_messages),
            'successful': len([r for r in results if r['success']]),
            'failed': len([r for r in results if not r['success']]),
            'results': results
        }
    
    @classmethod
    def create_transaction_from_sms(cls, message: str, user_id: str, category_id: str = None) -> Optional[TransactionCreate]:
        """
        Parse SMS message and create a TransactionCreate object
        Enhanced to use extracted transaction date when available
        """
        parsed_data = cls.parse_message(message)
        if not parsed_data:
            return None

        # Create enhanced M-Pesa details
        mpesa_details = MPesaDetails(
            recipient=parsed_data['mpesa_details'].get('recipient'),
            reference=parsed_data['mpesa_details'].get('reference'),
            transaction_id=parsed_data['mpesa_details'].get('transaction_id'),
            phone_number=parsed_data['mpesa_details'].get('phone_number'),
            balance_after=parsed_data['mpesa_details'].get('balance_after'),
            message_type=parsed_data['mpesa_details'].get('message_type'),
            transaction_fee=parsed_data['mpesa_details'].get('transaction_fee'),
            access_fee=parsed_data['mpesa_details'].get('access_fee'),
            fuliza_limit=parsed_data['mpesa_details'].get('fuliza_limit'),
            fuliza_outstanding=parsed_data['mpesa_details'].get('fuliza_outstanding'),
            due_date=parsed_data['mpesa_details'].get('due_date')
        )

        # Use provided category or suggest one
        final_category_id = category_id or "auto"  # Will be resolved by categorization service

        # Use extracted transaction date if available, otherwise use current time
        transaction_date = parsed_data.get('transaction_date')
        if transaction_date:
            try:
                # Parse the ISO date string
                from datetime import datetime
                date = datetime.fromisoformat(transaction_date)
            except (ValueError, TypeError):
                date = datetime.now()
        else:
            date = datetime.now()

        return TransactionCreate(
            amount=parsed_data['amount'],
            type=parsed_data['type'],
            category_id=final_category_id,
            description=parsed_data['description'],
            date=date,
            source='sms',
            mpesa_details=mpesa_details,
            sms_metadata=parsed_data.get('sms_metadata')
        )
