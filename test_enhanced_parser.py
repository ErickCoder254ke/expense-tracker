#!/usr/bin/env python3
"""
Test script for the enhanced M-Pesa parser
Tests the new features: date extraction, enhanced categorization, and better fee handling
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.mpesa_parser import MPesaParser

def main():
    print("🧪 Testing Enhanced M-Pesa Parser")
    print("=" * 60)
    
    # Run the enhanced parsing test
    test_results = MPesaParser.test_enhanced_parsing()
    
    print(f"\n📊 Test Summary:")
    print(f"Total Messages Tested: {test_results['total_tested']}")
    print(f"Successfully Parsed: {test_results['successful']}")
    print(f"Failed to Parse: {test_results['failed']}")
    print(f"Success Rate: {test_results['successful']/test_results['total_tested']*100:.1f}%")
    
    if test_results['failed'] > 0:
        print(f"\n❌ Failed Messages:")
        for result in test_results['results']:
            if not result['success']:
                print(f"Message {result['message_number']}: {result['error']}")
    
    print(f"\n✅ All tests completed!")
    
    # Test individual features
    print(f"\n🔧 Testing Individual Features:")
    
    # Test date parsing
    print(f"\n📅 Date Parsing Tests:")
    date_tests = [
        ("3/10/25", "10:55 PM"),
        ("4/10/25", "4:38 PM"),
        ("12/1/24", "9:30 AM"),
    ]
    
    for date_str, time_str in date_tests:
        parsed_date = MPesaParser.parse_transaction_date(date_str, time_str)
        print(f"  {date_str} at {time_str} -> {parsed_date}")
    
    # Test categorization
    print(f"\n📂 Categorization Tests:")
    categorization_tests = [
        ("KPLC PREPAID", "Should be Utilities"),
        ("SAFARICOM DATA BUNDLES", "Should be Utilities"),
        ("Equity Bulk Account", "Should be Financial Services"),
        ("SIMON NDERITU", "Should be Personal Transfer"),
    ]
    
    for recipient, expected in categorization_tests:
        category = MPesaParser.categorize_mpesa_transaction("", recipient)
        print(f"  {recipient} -> {category} ({expected})")

if __name__ == "__main__":
    main()
