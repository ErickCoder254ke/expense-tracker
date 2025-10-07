from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from motor.motor_asyncio import AsyncIOMotorDatabase
from models.transaction import Transaction, TransactionCreate, SMSParseRequest, SMSImportRequest, SMSImportResponse, SMSMetadata
from models.user import Category
from services.mpesa_parser import MPesaParser
from services.duplicate_detector import DuplicateDetector
from services.categorization import CategorizationService
from typing import List, Dict, Any
import uuid
from datetime import datetime
from bson import ObjectId

router = APIRouter(prefix="/sms", tags=["sms-integration"])

async def get_db():
    from server import db
    return db

@router.post("/parse", response_model=Dict[str, Any])
async def parse_single_sms(
    request: SMSParseRequest,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Parse a single SMS message and return transaction details"""
    try:
        print(f"DEBUG: Received message: {repr(request.message)}")

        if not request.message:
            raise HTTPException(status_code=400, detail="Message cannot be empty")

        # Parse the message
        parsed_data = MPesaParser.parse_message(request.message)
        if not parsed_data:
            raise HTTPException(status_code=400, detail="Message could not be parsed as M-Pesa transaction")
        
        # Get categories for auto-categorization
        categories_docs = await db.categories.find().to_list(100)
        categories = [Category(**{**doc, "id": str(doc["_id"])}) for doc in categories_docs]
        
        # Auto-categorize if needed
        if parsed_data['suggested_category']:
            category_match = next((c for c in categories if c.name == parsed_data['suggested_category']), None)
            if category_match:
                parsed_data['suggested_category_id'] = category_match.id
        
        return {
            "success": True,
            "parsed_data": parsed_data,
            "available_categories": [{"id": c.id, "name": c.name, "icon": c.icon, "color": c.color} for c in categories]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error parsing SMS: {str(e)}")

@router.post("/import", response_model=SMSImportResponse)
async def import_sms_messages(
    import_request: SMSImportRequest,
    background_tasks: BackgroundTasks,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Import multiple SMS messages as transactions"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")
        
        user_id = str(user_doc["_id"])
        
        # Get categories
        categories_docs = await db.categories.find().to_list(100)
        categories = [Category(**{**doc, "id": str(doc["_id"])}) for doc in categories_docs]
        
        # Track import results
        import_session_id = str(uuid.uuid4())
        successful_imports = 0
        duplicates_found = 0
        parsing_errors = 0
        transactions_created = []
        errors = []
        
        for message in import_request.messages:
            try:
                # Parse the message
                parsed_data = MPesaParser.parse_message(message)
                if not parsed_data:
                    parsing_errors += 1
                    errors.append(f"Could not parse message: {message[:50]}...")
                    continue
                
                # Check for duplicates
                duplicate_check = await DuplicateDetector.check_comprehensive_duplicate(
                    db=db,
                    user_id=user_id,
                    amount=parsed_data['amount'],
                    transaction_id=parsed_data['mpesa_details'].get('transaction_id'),
                    message_hash=parsed_data['original_message_hash'],
                    recipient=parsed_data['mpesa_details'].get('recipient')
                )
                
                if duplicate_check['is_duplicate']:
                    duplicates_found += 1
                    await DuplicateDetector.log_duplicate_attempt(db, user_id, parsed_data['original_message_hash'], duplicate_check)
                    continue
                
                # Auto-categorize
                category_id = None
                if import_request.auto_categorize:
                    suggested_category = parsed_data.get('suggested_category')
                    if suggested_category:
                        category_match = next((c for c in categories if c.name == suggested_category), None)
                        if category_match:
                            category_id = category_match.id
                
                # Use default category if not found
                if not category_id:
                    default_category = next((c for c in categories if c.name == "Other"), None)
                    category_id = default_category.id if default_category else categories[0].id
                
                # Create SMS metadata
                sms_metadata = SMSMetadata(
                    original_message_hash=parsed_data['original_message_hash'],
                    parsing_confidence=parsed_data['parsing_confidence'],
                    requires_review=parsed_data['requires_review'] or import_request.require_review,
                    suggested_category=parsed_data['suggested_category']
                )
                
                # Use provided transaction date or current time
                transaction_date = datetime.now()
                if hasattr(import_request, 'transaction_date') and import_request.transaction_date:
                    try:
                        # Parse the ISO format date string
                        transaction_date = datetime.fromisoformat(import_request.transaction_date.replace('Z', '+00:00'))
                    except (ValueError, AttributeError):
                        # If parsing fails, use current time
                        transaction_date = datetime.now()

                # Create transaction
                transaction_data = TransactionCreate(
                    amount=parsed_data['amount'],
                    type=parsed_data['type'],
                    category_id=category_id,
                    description=parsed_data['description'],
                    date=transaction_date,
                    source="sms",
                    mpesa_details=parsed_data['mpesa_details'],
                    sms_metadata=sms_metadata
                )
                
                transaction = Transaction(**transaction_data.dict(), user_id=user_id)
                result = await db.transactions.insert_one(transaction.dict())
                
                transactions_created.append(str(result.inserted_id))
                successful_imports += 1
                
            except Exception as e:
                parsing_errors += 1
                errors.append(f"Error processing message: {str(e)}")
        
        # Log import session
        import_log = {
            "import_session_id": import_session_id,
            "user_id": user_id,
            "messages_processed": len(import_request.messages),
            "transactions_created": successful_imports,
            "duplicates_found": duplicates_found,
            "parsing_errors": parsing_errors,
            "errors": errors,
            "completed_at": datetime.utcnow(),
            "auto_categorize": import_request.auto_categorize,
            "require_review": import_request.require_review
        }
        
        await db.sms_import_logs.insert_one(import_log)
        
        return SMSImportResponse(
            total_messages=len(import_request.messages),
            successful_imports=successful_imports,
            duplicates_found=duplicates_found,
            parsing_errors=parsing_errors,
            transactions_created=transactions_created,
            errors=errors
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error importing SMS messages: {str(e)}")

@router.get("/import-status/{import_session_id}")
async def get_import_status(
    import_session_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get the status of an SMS import session"""
    try:
        import_log = await db.sms_import_logs.find_one({"import_session_id": import_session_id})
        if not import_log:
            raise HTTPException(status_code=404, detail="Import session not found")
        
        # Remove ObjectId for JSON serialization
        import_log["id"] = str(import_log["_id"])
        del import_log["_id"]
        
        return import_log
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting import status: {str(e)}")

@router.get("/duplicate-stats")
async def get_duplicate_statistics(
    days: int = 30,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get duplicate detection statistics"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")
        
        user_id = str(user_doc["_id"])
        
        stats = await DuplicateDetector.get_duplicate_statistics(db, user_id, days)
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting duplicate statistics: {str(e)}")

@router.post("/create-transaction")
async def create_transaction_from_parsed_sms(
    parsed_data: Dict[str, Any],
    category_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Create a transaction from already parsed SMS data"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")
        
        user_id = str(user_doc["_id"])
        
        # Verify category exists
        category_doc = await db.categories.find_one({"_id": ObjectId(category_id) if ObjectId.is_valid(category_id) else category_id})
        if not category_doc:
            raise HTTPException(status_code=404, detail="Category not found")
        
        # Check for duplicates one more time
        duplicate_check = await DuplicateDetector.check_comprehensive_duplicate(
            db=db,
            user_id=user_id,
            amount=parsed_data['amount'],
            transaction_id=parsed_data.get('mpesa_details', {}).get('transaction_id'),
            message_hash=parsed_data.get('original_message_hash'),
            recipient=parsed_data.get('mpesa_details', {}).get('recipient')
        )
        
        if duplicate_check['is_duplicate']:
            raise HTTPException(status_code=400, detail="Duplicate transaction detected")
        
        # Create SMS metadata
        sms_metadata = SMSMetadata(
            original_message_hash=parsed_data.get('original_message_hash'),
            parsing_confidence=parsed_data.get('parsing_confidence', 0.5),
            requires_review=False,  # User has reviewed and confirmed
            suggested_category=parsed_data.get('suggested_category')
        )
        
        # Create transaction
        transaction_data = TransactionCreate(
            amount=parsed_data['amount'],
            type=parsed_data['type'],
            category_id=category_id,
            description=parsed_data['description'],
            date=datetime.now(),
            source="sms",
            mpesa_details=parsed_data.get('mpesa_details'),
            sms_metadata=sms_metadata
        )
        
        transaction = Transaction(**transaction_data.dict(), user_id=user_id)
        result = await db.transactions.insert_one(transaction.dict())
        transaction.id = str(result.inserted_id)
        
        return transaction
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating transaction: {str(e)}")

@router.get("/test-parser")
async def test_parser_with_sample_messages():
    """Test the parser with sample M-Pesa messages including user's specific examples"""
    # User's specific examples from their request
    user_examples = [
        "TJ6CF6NDST Confirmed.Ksh30.00 sent to SIMON  NDERITU on 6/10/25 at 7:43 AM. New M-PESA balance is Ksh21.73. Transaction cost, Ksh0.00. Amount you can transact within the day is 499,970.00. Sign up for Lipa Na M-PESA Till online https://m-pesaforbusiness.co.ke",
        "TJ6CF6OZYR Confirmed.     Ksh5.00 sent to SAFARICOM DATA BUNDLES for account SAFARICOM DATA BUNDLES on 6/10/25 at 5:14 PM. New M-PESA balance is Ksh16.73.       Transaction cost, Ksh0.00.",
        "TJ6CF6OS29 Confirmed.You have received Ksh100.00 from Equity Bulk Account 300600 on 6/10/25 at 5:19 PM New M-PESA balance is Ksh116.73.  Separate personal and business funds through Pochi la Biashara on *334#.",
        "TJ6CF6QGF0 Confirmed. Ksh15.00   sent to SAFARICOM DATA BUNDLES for account SAFARICOM DATA BUNDLES on 6/10/25 at 11:51 PM. New M-PESA balance is Ksh101.73. Transaction cost, Ksh0.00.",
        "TJ7CF6QJUV Confirmed. Ksh30.00 sent to SIMON  NDERITU on 7/10/25 at 8:00 AM. New M-PESA balance is Ksh71.73. Transaction cost, Ksh0.00. Amount you can transact within the day is 499,970.00. Sign up for Lipa Na M-PESA Till online https://m-pesaforbusiness.co.ke"
    ]

    # Additional legacy examples for comparison
    legacy_examples = [
        "You have received Ksh 1,250.00 from JOHN DOE 254722123456. New M-PESA balance is Ksh 3,450.00. Transaction cost, if any, is Ksh 0.00. Transaction ID ABC1DE2FG3. Confirmed.",
        "Ksh 200.00 sent to Safaricom Paybill 123456 on 12/01/2024 at 14:00. New M-PESA balance is Ksh 500.00. Transaction ID: XYZ123ABC.",
        "You have withdrawn Ksh 1,000.00 from 0722123456 - AGENT NAME. New M-PESA balance is Ksh 2,000.00. Transaction ID 98765ABC.",
        "You have purchased airtime Ksh 50.00 for 254700123456. New M-PESA balance is Ksh 450.00. Transaction ID: A1B2C3D.",
        "This is not an M-Pesa message"
    ]

    all_messages = user_examples + legacy_examples

    results = {
        "user_examples": [],
        "legacy_examples": [],
        "summary": {}
    }

    # Test user examples
    for message in user_examples:
        parsed = MPesaParser.parse_message(message)
        results["user_examples"].append({
            "original_message": message,
            "parsed_successfully": parsed is not None,
            "parsed_data": parsed,
            "confidence": parsed.get('parsing_confidence', 0) if parsed else 0,
            "pattern_type": parsed.get('mpesa_details', {}).get('message_type') if parsed else None,
            "transaction_id": parsed.get('mpesa_details', {}).get('transaction_id') if parsed else None,
            "amount": parsed.get('amount') if parsed else None,
            "recipient": parsed.get('mpesa_details', {}).get('recipient') if parsed else None,
            "category": parsed.get('suggested_category') if parsed else None
        })

    # Test legacy examples
    for message in legacy_examples:
        parsed = MPesaParser.parse_message(message)
        results["legacy_examples"].append({
            "original_message": message,
            "parsed_successfully": parsed is not None,
            "parsed_data": parsed,
            "confidence": parsed.get('parsing_confidence', 0) if parsed else 0
        })

    # Calculate summary statistics
    user_success_count = sum(1 for r in results["user_examples"] if r["parsed_successfully"])
    legacy_success_count = sum(1 for r in results["legacy_examples"] if r["parsed_successfully"])

    results["summary"] = {
        "total_messages": len(all_messages),
        "user_examples_count": len(user_examples),
        "user_examples_parsed": user_success_count,
        "user_examples_success_rate": user_success_count / len(user_examples) * 100,
        "legacy_examples_count": len(legacy_examples),
        "legacy_examples_parsed": legacy_success_count,
        "legacy_examples_success_rate": legacy_success_count / len(legacy_examples) * 100,
        "overall_success_rate": (user_success_count + legacy_success_count) / len(all_messages) * 100
    }

    return results

@router.post("/test-user-examples")
async def test_user_specific_examples():
    """Test the parser specifically with the user's provided SMS examples"""
    user_examples = [
        "TJ6CF6NDST Confirmed.Ksh30.00 sent to SIMON  NDERITU on 6/10/25 at 7:43 AM. New M-PESA balance is Ksh21.73. Transaction cost, Ksh0.00. Amount you can       transact within the day is 499,970.00. Sign up for Lipa Na M-PESA Till online https://m-pesaforbusiness.co.ke",
        "TJ6CF6OZYR Confirmed.     Ksh5.00 sent to SAFARICOM DATA BUNDLES for account SAFARICOM DATA BUNDLES on 6/10/25 at 5:14 PM. New M-PESA balance is Ksh16.73.       Transaction cost, Ksh0.00.",
        "TJ6CF6OS29 Confirmed.You have received Ksh100.00 from Equity Bulk Account 300600 on 6/10/25 at 5:19 PM New   M-PESA balance is Ksh116.73.  Separate personal and business funds through Pochi la Biashara on *334#.",
        "TJ6CF6QGF0 Confirmed. Ksh15.00   sent to SAFARICOM DATA BUNDLES for account SAFARICOM DATA BUNDLES on 6/10/25 at 11:51 PM. New M-PESA balance is Ksh101.73. Transaction cost, Ksh0.00.",
        "TJ7CF6QJUV Confirmed. Ksh30.00 sent to SIMON  NDERITU on 7/10/25 at 8:00 AM. New M-PESA balance is Ksh71.73. Transaction cost, Ksh0.00. Amount you can transact within the day is 499,970.00. Sign up for Lipa Na M-PESA Till onlinehttps://m-pesaforbusiness.co.ke"
    ]

    results = []
    for i, message in enumerate(user_examples, 1):
        parsed = MPesaParser.parse_message(message)

        # Detailed analysis for each message
        analysis = {
            "message_number": i,
            "original_message": message[:100] + "..." if len(message) > 100 else message,
            "is_mpesa_message": MPesaParser.is_mpesa_message(message),
            "parsed_successfully": parsed is not None,
        }

        if parsed:
            analysis.update({
                "transaction_id": parsed.get('mpesa_details', {}).get('transaction_id'),
                "amount": parsed.get('amount'),
                "type": parsed.get('type'),
                "recipient": parsed.get('mpesa_details', {}).get('recipient'),
                "balance_after": parsed.get('mpesa_details', {}).get('balance_after'),
                "transaction_fee": parsed.get('mpesa_details', {}).get('transaction_fee'),
                "confidence": parsed.get('parsing_confidence'),
                "pattern_type": parsed.get('mpesa_details', {}).get('message_type'),
                "suggested_category": parsed.get('suggested_category'),
                "requires_review": parsed.get('requires_review'),
                "description": parsed.get('description')
            })
        else:
            analysis["error"] = "Failed to parse message"

        results.append(analysis)

    # Summary statistics
    successful_parses = sum(1 for r in results if r["parsed_successfully"])

    return {
        "test_description": "Testing parser with user's specific M-PESA SMS examples",
        "total_messages": len(user_examples),
        "successful_parses": successful_parses,
        "success_rate": (successful_parses / len(user_examples)) * 100,
        "results": results,
        "expected_outcomes": {
            "message_1": "Send to Simon Nderitu (Personal Transfer)",
            "message_2": "Send to Safaricom Data Bundles (Utilities)",
            "message_3": "Received from Equity Bulk Account (Financial Services/Income)",
            "message_4": "Send to Safaricom Data Bundles (Utilities)",
            "message_5": "Send to Simon Nderitu (Personal Transfer)"
        }
    }
