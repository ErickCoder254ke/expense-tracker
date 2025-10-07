#!/usr/bin/env python3

"""
Test script for enhanced M-Pesa SMS parsing and fee extraction
Tests the parser with real M-Pesa message formats to ensure proper extraction of fees and transaction details
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.mpesa_parser import MPesaParser
import json

# Real M-Pesa message examples for testing (including user-provided examples)
TEST_MESSAGES = [
    # User-provided examples (exact format)
    "TJ3CF6GKC7 Confirmed.You have received Ksh100.00 from Equity Bulk Account 300600 on 3/10/25 at 10:55 PM New M-PESA balance is Ksh111.86.  Separate personal and business funds through Pochi la Biashara on *334#.",

    "TJ3CF6GITN Confirmed.You have received Ksh99.00 from Equity Bulk Account 300600 on 3/10/25 at 10:56 PM New M-PESA balance is Ksh210.86.  Separate personal and business funds through Pochi la Biashara on *334#.",

    "TJ4CF6I7HN Confirmed. Ksh100.00 sent to KPLC PREPAID for account 54405080323 on 4/10/25 at 4:38 PM New M-PESA balance is Ksh110.86. Transaction cost, Ksh0.00.Amount you can transact within the day is 499,900.00. Save frequent paybills for quick payment on M-PESA app https://bit.ly/mpesalnk",

    # Additional test messages
    "TJ6CF6NDST Confirmed.Ksh30.00 sent to SIMON  NDERITU on 6/10/25 at 7:43 AM. New M-PESA balance is Ksh21.73. Transaction cost, Ksh0.00. Amount you can transact within the day is 499,970.00.",

    # M-Pesa data bundles purchase
    "TJ6CF6OZYR Confirmed. Ksh5.00 sent to SAFARICOM DATA BUNDLES for account SAFARICOM DATA BUNDLES on 6/10/25 at 5:14 PM. New M-PESA balance is Ksh16.73. Transaction cost, Ksh0.00.",

    # M-Pesa with transaction fee
    "TJ7CF6QJUV Confirmed. Ksh30.00 sent to SIMON  NDERITU on 7/10/25 at 8:00 AM. New M-PESA balance is Ksh71.73. Transaction cost, Ksh2.50. Amount you can transact within the day is 499,970.00.",

    # Fuliza loan message with access fee
    "TJ8CF6WXYZ Confirmed. Fuliza M-PESA amount is Ksh50.00. Access fee charged Ksh5.00. Total Fuliza M-PESA outstanding amount is Ksh55.00 due on 15/10/25. New M-PESA balance is Ksh50.00.",

    # Paybill transaction with fee
    "TJ9CF6ABCD Confirmed. Ksh150.00 sent to KENYA POWER for account 123456789 on 8/10/25 at 2:30 PM. New M-PESA balance is Ksh500.00. Transaction cost, Ksh15.00.",

    # ATM withdrawal
    "TK1CF6EFGH Confirmed. Ksh500.00 withdrawn from KCB ATM WESTLANDS on 9/10/25 at 10:15 AM. New M-PESA balance is Ksh1,200.00. Transaction cost, Ksh35.00.",

    # Multiple SMS messages in one paste (testing message splitting)
    """TJ6CF6NDST Confirmed.Ksh30.00 sent to SIMON  NDERITU on 6/10/25 at 7:43 AM. New M-PESA balance is Ksh21.73. Transaction cost, Ksh0.00. Amount you can       transact within the day is 499,970.00. Sign up for Lipa Na M-PESA Till online https://m-pesaforbusiness.co.keTJ6CF6OZYR Confirmed.     Ksh5.00 sent to SAFARICOM DATA BUNDLES for account SAFARICOM DATA BUNDLES on 6/10/25 at 5:14 PM. New M-PESA balance is Ksh16.73.       Transaction cost, Ksh0.00.TJ6CF6OS29 Confirmed.You have received Ksh100.00 from Equity Bulk Account 300600 on 6/10/25 at 5:19 PM New   M-PESA balance is Ksh116.73.  Separate personal and business funds through Pochi la Biashara on *334#.TJ6CF6QGF0 Confirmed. Ksh15.00   sent to SAFARICOM DATA BUNDLES for account SAFARICOM DATA BUNDLES on 6/10/25 at 11:51 PM. New M-PESA balance is Ksh101.73. Transaction cost, Ksh0.00.TJ7CF6QJUV Confirmed. Ksh30.00 sent to SIMON  NDERITU on 7/10/25 at 8:00 AM. New M-PESA balance is Ksh71.73. Transaction cost, Ksh0.00. Amount you can transact within the day is 499,970.00. Sign up for Lipa Na M-PESA Till onlinehttps://m-pesaforbusiness.co.ke"""
]

def test_message_parsing():
    """Test parsing of individual M-Pesa messages"""
    print("=" * 80)
    print("TESTING M-PESA MESSAGE PARSING")
    print("=" * 80)
    
    for i, message in enumerate(TEST_MESSAGES[:-1], 1):  # Skip the multi-message test for now
        print(f"\n--- Test Message {i} ---")
        print(f"Original: {message[:100]}..." if len(message) > 100 else f"Original: {message}")
        
        # Check if it's detected as M-Pesa message
        is_mpesa = MPesaParser.is_mpesa_message(message)
        print(f"Detected as M-Pesa: {is_mpesa}")
        
        if is_mpesa:
            # Parse the message
            parsed_data = MPesaParser.parse_message(message)
            
            if parsed_data:
                print(f"✅ PARSING SUCCESS")
                print(f"Amount: KSh {parsed_data['amount']}")
                print(f"Type: {parsed_data['type']}")
                print(f"Description: {parsed_data['description']}")
                print(f"Category: {parsed_data['suggested_category']}")
                print(f"Confidence: {parsed_data['parsing_confidence']:.2f}")

                # Print transaction date if extracted
                transaction_date = parsed_data.get('transaction_date')
                if transaction_date:
                    print(f"Transaction Date: {transaction_date}")
                else:
                    print(f"Transaction Date: Not extracted")

                # Print M-Pesa details
                mpesa_details = parsed_data['mpesa_details']
                if mpesa_details:
                    print(f"Recipient: {mpesa_details.get('recipient', 'N/A')}")
                    print(f"Transaction ID: {mpesa_details.get('transaction_id', 'N/A')}")
                    print(f"Reference/Account: {mpesa_details.get('reference', 'N/A')}")
                    print(f"Balance After: KSh {mpesa_details.get('balance_after', 'N/A')}")
                    print(f"Transaction Fee: KSh {mpesa_details.get('transaction_fee', 0)}")
                    print(f"Access Fee: KSh {mpesa_details.get('access_fee', 0)}")
                
                # Print SMS metadata
                sms_metadata = parsed_data.get('sms_metadata', {})
                if sms_metadata:
                    total_fees = sms_metadata.get('total_fees', 0)
                    fee_breakdown = sms_metadata.get('fee_breakdown', {})
                    print(f"Total Fees: KSh {total_fees}")
                    if fee_breakdown:
                        print(f"Fee Breakdown: {fee_breakdown}")
                
                print(f"Requires Review: {parsed_data['requires_review']}")
            else:
                print("❌ PARSING FAILED")
        else:
            print("❌ NOT DETECTED AS M-PESA MESSAGE")

def test_fee_extraction():
    """Test enhanced fee extraction specifically"""
    print("\n" + "=" * 80)
    print("TESTING ENHANCED FEE EXTRACTION")
    print("=" * 80)
    
    fee_test_messages = [
        ("Zero fee message", "TJ6CF6NDST Confirmed.Ksh30.00 sent to SIMON NDERITU on 6/10/25 at 7:43 AM. New M-PESA balance is Ksh21.73. Transaction cost, Ksh0.00."),
        ("Transaction fee message", "TJ7CF6QJUV Confirmed. Ksh30.00 sent to SIMON NDERITU on 7/10/25 at 8:00 AM. New M-PESA balance is Ksh71.73. Transaction cost, Ksh2.50."),
        ("Fuliza access fee", "TJ8CF6WXYZ Confirmed. Fuliza M-PESA amount is Ksh50.00. Access fee charged Ksh5.00. Total Fuliza M-PESA outstanding amount is Ksh55.00."),
        ("ATM withdrawal fee", "TK1CF6EFGH Confirmed. Ksh500.00 withdrawn from KCB ATM. New M-PESA balance is Ksh1,200.00. Transaction cost, Ksh35.00."),
        ("Paybill with fee", "TJ9CF6ABCD Confirmed. Ksh150.00 sent to KENYA POWER. New M-PESA balance is Ksh500.00. Transaction cost, Ksh15.00.")
    ]
    
    for test_name, message in fee_test_messages:
        print(f"\n--- {test_name} ---")
        
        # Test individual fee extraction
        enhanced_fees = MPesaParser._extract_all_fees(message)
        print(f"Enhanced fees extracted: {enhanced_fees}")
        
        # Test full parsing
        parsed_data = MPesaParser.parse_message(message)
        if parsed_data:
            sms_metadata = parsed_data.get('sms_metadata', {})
            total_fees = sms_metadata.get('total_fees', 0)
            fee_breakdown = sms_metadata.get('fee_breakdown', {})
            
            print(f"Total fees from parsing: KSh {total_fees}")
            print(f"Fee breakdown from parsing: {fee_breakdown}")
            
            # Calculate expected vs actual
            expected_total = sum(enhanced_fees.values())
            if abs(total_fees - expected_total) < 0.01:  # Account for floating point precision
                print("✅ Fee extraction matches")
            else:
                print(f"❌ Fee mismatch: Expected {expected_total}, Got {total_fees}")
        else:
            print("❌ Parsing failed")

def test_multi_message_splitting():
    """Test splitting of multiple messages pasted together"""
    print("\n" + "=" * 80)
    print("TESTING MULTI-MESSAGE SPLITTING")
    print("=" * 80)
    
    multi_message = TEST_MESSAGES[-1]  # The long multi-message string
    print(f"Input length: {len(multi_message)} characters")
    
    # This would normally be done in the frontend SMS parser service
    # For backend testing, we'll manually split and test each part
    
    # Simple splitting by transaction ID pattern for testing
    import re
    transaction_patterns = re.findall(r'([A-Z0-9]{6,12}\s+confirmed[^T]*)', multi_message, re.IGNORECASE)
    
    print(f"Found {len(transaction_patterns)} potential messages")
    
    for i, pattern in enumerate(transaction_patterns, 1):
        print(f"\n--- Split Message {i} ---")
        cleaned_message = pattern.strip()
        print(f"Length: {len(cleaned_message)}")
        print(f"Preview: {cleaned_message[:150]}...")
        
        is_mpesa = MPesaParser.is_mpesa_message(cleaned_message)
        print(f"Detected as M-Pesa: {is_mpesa}")
        
        if is_mpesa:
            parsed_data = MPesaParser.parse_message(cleaned_message)
            if parsed_data:
                print(f"✅ Parsed: KSh {parsed_data['amount']} - {parsed_data['description']}")
                sms_metadata = parsed_data.get('sms_metadata', {})
                total_fees = sms_metadata.get('total_fees', 0)
                if total_fees > 0:
                    print(f"   Fees: KSh {total_fees}")
            else:
                print("❌ Parsing failed")

def test_date_parsing():
    """Test enhanced date and time parsing from M-Pesa messages"""
    print("\n" + "=" * 80)
    print("TESTING DATE AND TIME PARSING")
    print("=" * 80)

    date_test_cases = [
        ("3/10/25", "10:55 PM", "Should parse to 2025-10-03 22:55:00"),
        ("4/10/25", "4:38 PM", "Should parse to 2025-10-04 16:38:00"),
        ("6/10/25", "7:43 AM", "Should parse to 2025-10-06 07:43:00"),
        ("12/1/24", "9:30 AM", "Should parse to 2024-01-12 09:30:00"),
        ("1/5/26", "11:59 PM", "Should parse to 2026-05-01 23:59:00"),
    ]

    print("Testing individual date parsing function:")
    for date_str, time_str, expected in date_test_cases:
        result = MPesaParser.parse_transaction_date(date_str, time_str)
        print(f"  {date_str} at {time_str} -> {result} ({expected})")

    print("\nTesting date extraction from full messages:")
    # Test with our user-provided examples that have dates
    date_messages = [
        "TJ3CF6GKC7 Confirmed.You have received Ksh100.00 from Equity Bulk Account 300600 on 3/10/25 at 10:55 PM New M-PESA balance is Ksh111.86.",
        "TJ4CF6I7HN Confirmed. Ksh100.00 sent to KPLC PREPAID for account 54405080323 on 4/10/25 at 4:38 PM New M-PESA balance is Ksh110.86.",
        "TJ6CF6NDST Confirmed.Ksh30.00 sent to SIMON NDERITU on 6/10/25 at 7:43 AM. New M-PESA balance is Ksh21.73."
    ]

    for message in date_messages:
        print(f"\nMessage: {message[:80]}...")
        parsed_data = MPesaParser.parse_message(message)
        if parsed_data:
            transaction_date = parsed_data.get('transaction_date')
            if transaction_date:
                print(f"✅ Extracted date: {transaction_date}")
            else:
                print(f"❌ No date extracted")
        else:
            print(f"❌ Parsing failed")

def test_categorization():
    """Test auto-categorization of different transaction types"""
    print("\n" + "=" * 80)
    print("TESTING AUTO-CATEGORIZATION")
    print("=" * 80)
    
    categorization_tests = [
        ("KPLC PREPAID (utilities)", "TJ4CF6I7HN Confirmed. Ksh100.00 sent to KPLC PREPAID for account 54405080323"),
        ("Data bundles (utilities)", "TJ6CF6OZYR Confirmed. Ksh5.00 sent to SAFARICOM DATA BUNDLES for account SAFARICOM DATA BUNDLES"),
        ("Personal transfer", "TJ6CF6NDST Confirmed.Ksh30.00 sent to SIMON NDERITU on 6/10/25 at 7:43 AM"),
        ("Bank account (financial)", "TJ6CF6OS29 Confirmed.You have received Ksh100.00 from Equity Bulk Account 300600"),
        ("Kenya Power (utilities)", "TJ9CF6ABCD Confirmed. Ksh150.00 sent to KENYA POWER for account 123456789"),
        ("ATM withdrawal", "TK1CF6EFGH Confirmed. Ksh500.00 withdrawn from KCB ATM WESTLANDS"),
        ("Fuliza loan (loans & credit)", "TJ8CF6WXYZ Confirmed. Fuliza M-PESA amount is Ksh50.00. Access fee charged Ksh5.00"),
        ("Nairobi Water (utilities)", "TX1234567 Confirmed. Ksh250.00 sent to NAIROBI WATER for account NCWSC123"),
        ("Safaricom airtime (utilities)", "TY1234567 Confirmed. Ksh50.00 sent to SAFARICOM for airtime purchase")
    ]
    
    for test_name, message in categorization_tests:
        print(f"\n--- {test_name} ---")
        print(f"Message: {message[:100]}...")
        
        parsed_data = MPesaParser.parse_message(message)
        if parsed_data:
            print(f"✅ Category: {parsed_data['suggested_category']}")
            print(f"   Type: {parsed_data['type']}")
            print(f"   Amount: KSh {parsed_data['amount']}")
            print(f"   Confidence: {parsed_data['parsing_confidence']:.2f}")
        else:
            print("❌ Parsing failed")

def main():
    """Run all tests"""
    print("M-PESA SMS PARSER ENHANCED TESTING")
    print("Testing enhanced fee extraction and transaction charges functionality")
    print("=" * 80)
    
    try:
        test_message_parsing()
        test_fee_extraction()
        test_date_parsing()
        test_multi_message_splitting()
        test_categorization()

        print("\n" + "=" * 80)
        print("✅ ALL TESTS COMPLETED")
        print("Review the output above to verify enhanced parsing features:")
        print("- Date and time extraction from messages")
        print("- Enhanced fee extraction and transaction charges")
        print("- Improved categorization for Kenyan services")
        print("- Better recipient name handling")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ TEST ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
