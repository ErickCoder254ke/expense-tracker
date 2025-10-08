#!/usr/bin/env python3
"""
Test script to verify enhanced date parsing functionality
Tests the user's specific M-Pesa SMS examples to ensure dates are extracted correctly
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.mpesa_parser import MPesaParser
from datetime import datetime

def test_user_examples():
    """Test the parser with the user's specific SMS examples"""
    
    # User's specific examples from their request
    user_examples = [
        "TJ3CF6GKC7 Confirmed.You have received Ksh100.00 from Equity Bulk Account 300600 on 3/10/25 at 10:55 PM New M-PESA balance is Ksh111.86.  Separate personal and business funds through Pochi la Biashara on *334#.",
        "TJ3CF6GITN Confirmed.You have received Ksh99.00 from Equity Bulk Account 300600 on 3/10/25 at 10:56 PM New M-PESA balance is Ksh210.86.  Separate personal and business funds through Pochi la Biashara on *334#.",
        "TJ4CF6I7HN Confirmed. Ksh100.00 sent to KPLC PREPAID for account 54405080323 on 4/10/25 at 4:38 PM New M-PESA balance is Ksh110.86. Transaction cost, Ksh0.00.Amount you can transact within the day is 499,900.00. Save frequent paybills for quick payment on M-PESA app https://bit.ly/mpesalnk"
    ]

    print("=== Testing Enhanced Date Parsing ===")
    print(f"Testing {len(user_examples)} user examples...\n")

    successful_parses = 0
    successful_date_extractions = 0

    for i, message in enumerate(user_examples, 1):
        print(f"--- Test {i} ---")
        print(f"Message: {message[:80]}...")
        
        # Test if message is recognized as M-Pesa
        is_mpesa = MPesaParser.is_mpesa_message(message)
        print(f"Is M-Pesa Message: {is_mpesa}")
        
        if not is_mpesa:
            print("❌ Message not recognized as M-Pesa\n")
            continue
        
        # Parse the message
        parsed = MPesaParser.parse_message(message)
        
        if parsed:
            successful_parses += 1
            print("✅ Message parsed successfully")
            print(f"Amount: KSh {parsed['amount']}")
            print(f"Type: {parsed['type']}")
            print(f"Recipient: {parsed['mpesa_details']['recipient']}")
            print(f"Transaction ID: {parsed['mpesa_details']['transaction_id']}")
            print(f"Pattern Type: {parsed['mpesa_details']['message_type']}")
            
            # Check if date was extracted
            transaction_date = parsed.get('transaction_date')
            if transaction_date:
                successful_date_extractions += 1
                print(f"✅ Date Extracted: {transaction_date}")
                
                # Parse and display in readable format
                try:
                    dt = datetime.fromisoformat(transaction_date.replace('Z', '+00:00'))
                    print(f"   Readable Date: {dt.strftime('%B %d, %Y at %I:%M %p')}")
                    print(f"   Year: {dt.year}, Month: {dt.month}, Day: {dt.day}")
                    print(f"   Hour: {dt.hour}, Minute: {dt.minute}")
                except Exception as e:
                    print(f"   ⚠️  Date parsing error: {e}")
            else:
                print("❌ Date NOT extracted")
            
            print(f"Confidence: {parsed['parsing_confidence']:.2f}")
            print(f"Suggested Category: {parsed['suggested_category']}")
        else:
            print("❌ Message parsing failed")
        
        print()

    print(f"=== Summary ===")
    print(f"Total messages: {len(user_examples)}")
    print(f"Successfully parsed: {successful_parses}")
    print(f"Date extraction success: {successful_date_extractions}")
    print(f"Parse success rate: {(successful_parses / len(user_examples)) * 100:.1f}%")
    print(f"Date extraction rate: {(successful_date_extractions / len(user_examples)) * 100:.1f}%")
    
    if successful_date_extractions == len(user_examples):
        print("🎉 All dates extracted successfully!")
    elif successful_date_extractions > 0:
        print("⚠️  Some dates extracted, but not all")
    else:
        print("❌ No dates were extracted")

def test_date_parsing_function():
    """Test the date parsing function directly"""
    
    print("\n=== Testing Date Parsing Function Directly ===")
    
    test_cases = [
        ("3/10/25", "10:55 PM"),
        ("4/10/25", "4:38 PM"),
        ("6/10/25", "7:43 AM"),
        ("6/10/25", "5:14 PM"),
        ("7/10/25", "8:00 AM"),
    ]
    
    for date_str, time_str in test_cases:
        print(f"Testing: '{date_str}' at '{time_str}'")
        parsed_date = MPesaParser.parse_transaction_date(date_str, time_str)
        
        if parsed_date:
            print(f"✅ Result: {parsed_date}")
            try:
                dt = datetime.fromisoformat(parsed_date)
                print(f"   Readable: {dt.strftime('%B %d, %Y at %I:%M %p')}")
            except Exception as e:
                print(f"   ⚠️  Error converting: {e}")
        else:
            print("❌ Failed to parse")
        print()

if __name__ == "__main__":
    test_user_examples()
    test_date_parsing_function()
