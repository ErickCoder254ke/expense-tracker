#!/usr/bin/env python3
"""
Test script for Fuliza automatic deduction scenarios
Tests various Fuliza patterns including automatic deductions when receiving money
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.mpesa_parser import MPesaParser
from datetime import datetime

def test_fuliza_scenarios():
    """Test various Fuliza transaction scenarios"""
    
    print("=== Testing Fuliza Transaction Scenarios ===\n")
    
    # Test scenarios for Fuliza transactions
    fuliza_test_messages = [
        # 1. Standard Fuliza loan
        {
            "description": "Standard Fuliza loan with access fee",
            "message": "TJ8CF6WXYZ Confirmed. Fuliza M-PESA amount is Ksh50.00. Access fee charged Ksh5.00. Total Fuliza M-PESA outstanding amount is Ksh55.00 due on 15/10/25. New M-PESA balance is Ksh50.00.",
            "expected": {
                "type": "income",
                "amount": 50.00,
                "pattern": "fuliza_loan",
                "access_fee": 5.00,
                "outstanding": 55.00
            }
        },
        
        # 2. Automatic deduction from received money (common scenario)
        {
            "description": "Automatic Fuliza deduction from received payment",
            "message": "TJ9CF6ABCD Confirmed. Ksh30.00 from your M-PESA has been used to pay your outstanding Fuliza M-PESA amount. Available Fuliza M-PESA limit is Ksh200.00. New M-PESA balance is Ksh70.00.",
            "expected": {
                "type": "expense",
                "amount": 30.00,
                "pattern": "fuliza_repayment",
                "fuliza_limit": 200.00
            }
        },
        
        # 3. Partial automatic deduction scenario
        {
            "description": "Partial Fuliza repayment from received amount",
            "message": "TK1CF6EFGH Confirmed. Ksh15.00 from your M-PESA has been used to repay Fuliza M-PESA. Outstanding Fuliza amount is Ksh25.00. Available Fuliza M-PESA limit is Ksh175.00. New M-PESA balance is Ksh85.00.",
            "expected": {
                "type": "expense", 
                "amount": 15.00,
                "pattern": "fuliza_repayment",
                "fuliza_limit": 175.00
            }
        },
        
        # 4. Manual Fuliza repayment
        {
            "description": "Manual Fuliza repayment",
            "message": "TL2CF6GHIJ Confirmed. Ksh100.00 sent to pay Fuliza M-PESA. Outstanding Fuliza amount is Ksh0.00. Available Fuliza M-PESA limit is Ksh300.00. New M-PESA balance is Ksh150.00.",
            "expected": {
                "type": "expense",
                "amount": 100.00,
                "pattern": "fuliza_repayment",
                "fuliza_limit": 300.00
            }
        },
        
        # 5. Combined scenario: receive money + automatic deduction
        {
            "description": "Received money with automatic Fuliza deduction",
            "message": "TM3CF6KLMN Confirmed. You have received Ksh200.00 from JOHN DOE 254722123456. Ksh50.00 from your M-PESA has been used to pay Fuliza M-PESA. Available Fuliza M-PESA limit is Ksh250.00. New M-PESA balance is Ksh350.00.",
            "expected": {
                "type": "income",  # This is tricky - could be income or a separate transaction
                "amount": 200.00,
                "pattern": "compound_received_fuliza",  # The main transaction is receiving money
                "fuliza_deduction": 50.00  # Additional info about deduction
            }
        },

        # 6. Real-world Fuliza loan example
        {
            "description": "Real Fuliza loan with enhanced pattern",
            "message": "TK9CF6WXYZ Confirmed. Fuliza M-PESA amount is Ksh150.00. Access fee charged Ksh10.00. Total Fuliza M-PESA outstanding amount is Ksh160.00 due on 20/11/25. Your new M-PESA balance is Ksh150.00. Repay on time to avoid extra charges.",
            "expected": {
                "type": "income",
                "amount": 150.00,
                "pattern": "fuliza_loan",
                "access_fee": 10.00,
                "outstanding": 160.00
            }
        },

        # 7. Enhanced automatic repayment from salary
        {
            "description": "Salary received with automatic Fuliza repayment",
            "message": "TL5CF6ABCD Confirmed. You have received Ksh15000.00 from COMPANY PAYROLL on 25/10/25 at 2:00 PM. Ksh160.00 from your M-PESA has been used to repay your outstanding Fuliza M-PESA amount. Available Fuliza M-PESA limit is Ksh300.00. New M-PESA balance is Ksh14840.00.",
            "expected": {
                "type": "income",
                "amount": 15000.00,
                "pattern": "compound_received_fuliza",
                "fuliza_deduction": 160.00,
                "fuliza_limit": 300.00
            }
        },

        # 8. Multiple fee types in one transaction
        {
            "description": "Transaction with multiple fees including Fuliza",
            "message": "TN7CF6EFGH Confirmed. Ksh500.00 sent to KPLC PREPAID for account 12345678 on 26/10/25 at 6:30 PM. Transaction cost, Ksh25.00. Service fee, Ksh5.00. New M-PESA balance is Ksh1470.00.",
            "expected": {
                "type": "expense",
                "amount": 500.00,
                "pattern": "modern_sent",
                "transaction_fee": 25.00,
                "service_fee": 5.00
            }
        },

        # 9. Enhanced date extraction test
        {
            "description": "Transaction with enhanced date format",
            "message": "TO8CF6GHIJ Confirmed. Ksh75.00 sent to UBER TRIP on 27/10/25 at 11:45 PM. New M-PESA balance is Ksh395.00. Transaction cost, Ksh0.00.",
            "expected": {
                "type": "expense",
                "amount": 75.00,
                "pattern": "modern_sent",
                "has_date": True,
                "transaction_fee": 0.00
            }
        }
    ]
    
    successful_parses = 0
    total_tests = len(fuliza_test_messages)
    
    for i, test_case in enumerate(fuliza_test_messages, 1):
        print(f"--- Test {i}: {test_case['description']} ---")
        print(f"Message: {test_case['message'][:80]}...")
        
        # Test if message is recognized as M-Pesa
        is_mpesa = MPesaParser.is_mpesa_message(test_case['message'])
        print(f"Is M-Pesa Message: {is_mpesa}")
        
        if not is_mpesa:
            print("❌ Message not recognized as M-Pesa\n")
            continue
        
        # Parse the message
        parsed = MPesaParser.parse_message(test_case['message'])
        
        if parsed:
            successful_parses += 1
            print("✅ Message parsed successfully")
            
            # Check basic parsing
            print(f"Amount: KSh {parsed['amount']}")
            print(f"Type: {parsed['type']}")
            print(f"Pattern: {parsed['mpesa_details']['message_type']}")
            print(f"Description: {parsed['description']}")
            print(f"Category: {parsed['suggested_category']}")
            
            # Check Fuliza-specific details
            mpesa_details = parsed['mpesa_details']
            if mpesa_details.get('access_fee'):
                print(f"Access Fee: KSh {mpesa_details['access_fee']}")
            if mpesa_details.get('fuliza_outstanding'):
                print(f"Fuliza Outstanding: KSh {mpesa_details['fuliza_outstanding']}")
            if mpesa_details.get('fuliza_limit'):
                print(f"Fuliza Limit: KSh {mpesa_details['fuliza_limit']}")
            if mpesa_details.get('due_date'):
                print(f"Due Date: {mpesa_details['due_date']}")
            
            print(f"Balance After: KSh {mpesa_details.get('balance_after', 'N/A')}")
            print(f"Confidence: {parsed['parsing_confidence']:.2f}")
            
            # Validate against expected results
            expected = test_case['expected']
            validation_passed = True
            
            if expected.get('type') and parsed['type'] != expected['type']:
                print(f"⚠️  Type mismatch: expected {expected['type']}, got {parsed['type']}")
                validation_passed = False
            
            if expected.get('amount') and abs(parsed['amount'] - expected['amount']) > 0.01:
                print(f"⚠️  Amount mismatch: expected {expected['amount']}, got {parsed['amount']}")
                validation_passed = False
            
            if expected.get('pattern') and mpesa_details.get('message_type') != expected['pattern']:
                print(f"⚠️  Pattern mismatch: expected {expected['pattern']}, got {mpesa_details.get('message_type')}")
                validation_passed = False
            
            if validation_passed:
                print("✅ Validation passed")
            else:
                print("⚠️  Some validations failed")
            
        else:
            print("❌ Message parsing failed")
        
        print()
    
    print(f"=== Summary ===")
    print(f"Total tests: {total_tests}")
    print(f"Successfully parsed: {successful_parses}")
    print(f"Success rate: {(successful_parses / total_tests) * 100:.1f}%")
    
    if successful_parses == total_tests:
        print("🎉 All Fuliza scenarios parsed successfully!")
    elif successful_parses > 0:
        print("⚠️  Some scenarios parsed, but improvements needed")
    else:
        print("❌ No scenarios were parsed successfully")

def test_fuliza_categorization():
    """Test that Fuliza transactions are properly categorized"""

    print("\n=== Testing Fuliza Categorization ===")

    test_messages = [
        "TJ8CF6WXYZ Confirmed. Fuliza M-PESA amount is Ksh50.00. Access fee charged Ksh5.00.",
        "TJ9CF6ABCD Confirmed. Ksh30.00 from your M-PESA has been used to pay Fuliza M-PESA.",
    ]

    for message in test_messages:
        category = MPesaParser.categorize_mpesa_transaction(message)
        print(f"Message: {message[:50]}...")
        print(f"Category: {category}")

        if 'fuliza' in message.lower():
            expected_category = 'Loans & Credit'
            if category == expected_category:
                print("✅ Correct category")
            else:
                print(f"⚠️  Expected '{expected_category}', got '{category}'")
        print()

def test_enhanced_fee_extraction():
    """Test enhanced fee extraction capabilities"""

    print("\n=== Testing Enhanced Fee Extraction ===")

    fee_test_messages = [
        {
            "description": "Zero transaction cost",
            "message": "TJ1CF6TEST Confirmed. Ksh100.00 sent to JOHN DOE. Transaction cost, Ksh0.00. New M-PESA balance is Ksh900.00.",
            "expected_fees": {"transaction_fee": 0.00}
        },
        {
            "description": "Multiple fee types",
            "message": "TJ2CF6TEST Confirmed. Ksh500.00 sent to BANK ACCOUNT. Transaction cost, Ksh25.00. Service fee, Ksh10.00. Bank charge, Ksh5.00.",
            "expected_fees": {"transaction_fee": 25.00, "service_fee": 10.00, "bank_charge": 5.00}
        },
        {
            "description": "Fuliza access fee",
            "message": "TJ3CF6TEST Confirmed. Fuliza M-PESA amount is Ksh200.00. Access fee charged Ksh15.00.",
            "expected_fees": {"access_fee": 15.00}
        },
        {
            "description": "ATM withdrawal fee",
            "message": "TJ4CF6TEST Confirmed. Ksh1000.00 withdrawn from Agent ATM. ATM fee, Ksh30.00. New balance Ksh2970.00.",
            "expected_fees": {"atm_fee": 30.00}
        }
    ]

    for test_case in fee_test_messages:
        print(f"--- {test_case['description']} ---")
        print(f"Message: {test_case['message'][:60]}...")

        extracted_fees = MPesaParser._extract_all_fees(test_case['message'])
        expected_fees = test_case['expected_fees']

        print(f"Extracted fees: {extracted_fees}")
        print(f"Expected fees: {expected_fees}")

        # Validate fees
        fees_match = True
        for fee_type, expected_amount in expected_fees.items():
            extracted_amount = extracted_fees.get(fee_type, 0)
            if abs(extracted_amount - expected_amount) > 0.01:
                print(f"⚠️  {fee_type}: expected {expected_amount}, got {extracted_amount}")
                fees_match = False

        if fees_match:
            print("✅ Fees extracted correctly")
        else:
            print("❌ Fee extraction failed")
        print()

def test_enhanced_date_extraction():
    """Test enhanced date extraction from various message formats"""

    print("\n=== Testing Enhanced Date Extraction ===")

    date_test_messages = [
        {
            "description": "Standard format with AM/PM",
            "message": "TJ1CF6TEST Confirmed. Ksh100.00 sent to JOHN on 6/10/25 at 7:43 AM. New balance Ksh900.00.",
            "expected_has_date": True
        },
        {
            "description": "PM time format",
            "message": "TJ2CF6TEST Confirmed. Ksh200.00 received on 15/11/25 at 11:30 PM. New balance Ksh1100.00.",
            "expected_has_date": True
        },
        {
            "description": "24-hour format",
            "message": "TJ3CF6TEST Confirmed. Ksh300.00 sent on 20/12/25 at 14:30. New balance Ksh800.00.",
            "expected_has_date": True
        },
        {
            "description": "No explicit date",
            "message": "TJ4CF6TEST Confirmed. Ksh400.00 sent to VENDOR. New balance Ksh400.00.",
            "expected_has_date": False
        }
    ]

    for test_case in date_test_messages:
        print(f"--- {test_case['description']} ---")
        print(f"Message: {test_case['message'][:60]}...")

        extracted_date = MPesaParser.extract_date_from_message(test_case['message'])
        has_date = extracted_date is not None
        expected_has_date = test_case['expected_has_date']

        print(f"Extracted date: {extracted_date or 'None'}")
        print(f"Expected to have date: {expected_has_date}")

        if has_date == expected_has_date:
            print("✅ Date extraction correct")
        else:
            print("❌ Date extraction failed")
        print()

def test_comprehensive_parsing():
    """Comprehensive test of all enhanced features together"""

    print("\n=== Comprehensive Enhanced Parsing Test ===")

    # Test with user's actual message examples
    user_messages = [
        "TJ6CF6NDST Confirmed.Ksh30.00 sent to SIMON  NDERITU on 6/10/25 at 7:43 AM. New M-PESA balance is Ksh21.73. Transaction cost, Ksh0.00. Amount you can transact within the day is 499,970.00.",
        "TJ3CF6GKC7 Confirmed.You have received Ksh100.00 from Equity Bulk Account 300600 on 3/10/25 at 10:55 PM New M-PESA balance is Ksh111.86.",
        "TJ4CF6I7HN Confirmed. Ksh100.00 sent to KPLC PREPAID for account 54405080323 on 4/10/25 at 4:38 PM New M-PESA balance is Ksh110.86. Transaction cost, Ksh0.00."
    ]

    print(f"Testing {len(user_messages)} real user messages...")

    for i, message in enumerate(user_messages, 1):
        print(f"\n--- Real Message {i} ---")
        print(f"Message: {message[:80]}...")

        parsed = MPesaParser.parse_message(message)

        if parsed:
            print("✅ Successfully parsed")
            print(f"   Amount: KSh {parsed['amount']}")
            print(f"   Type: {parsed['type']}")
            print(f"   Description: {parsed['description']}")
            print(f"   Category: {parsed['suggested_category']}")
            print(f"   Date: {parsed.get('transaction_date', 'Not extracted')}")
            print(f"   Transaction ID: {parsed['mpesa_details']['transaction_id']}")
            print(f"   Confidence: {parsed['parsing_confidence']:.2f}")

            # Check fees
            if parsed['sms_metadata'] and parsed['sms_metadata'].get('fee_breakdown'):
                print(f"   Fees: {parsed['sms_metadata']['fee_breakdown']}")

        else:
            print("❌ Failed to parse")

    print(f"\n🎉 Comprehensive test completed!")

if __name__ == "__main__":
    test_fuliza_scenarios()
    test_fuliza_categorization()
    test_enhanced_fee_extraction()
    test_enhanced_date_extraction()
    test_comprehensive_parsing()
