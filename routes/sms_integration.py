from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from motor.motor_asyncio import AsyncIOMotorDatabase
from models.transaction import Transaction, TransactionCreate, SMSImportRequest, SMSImportResponse, SMSMetadata
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
    message: str,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Parse a single SMS message and return transaction details"""
    try:
        # Parse the message
        parsed_data = MPesaParser.parse_message(message)
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
                
                # Create transaction
                transaction_data = TransactionCreate(
                    amount=parsed_data['amount'],
                    type=parsed_data['type'],
                    category_id=category_id,
                    description=parsed_data['description'],
                    date=datetime.now(),  # Use current time since SMS doesn't contain full timestamp
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
    """Test the parser with sample M-Pesa messages"""
    sample_messages = [
        "You have received Ksh 1,250.00 from JOHN DOE 254722123456. New M-PESA balance is Ksh 3,450.00. Transaction cost, if any, is Ksh 0.00. Transaction ID ABC1DE2FG3. Confirmed.",
        "Ksh 200.00 sent to Safaricom Paybill 123456 on 12/01/2024 at 14:00. New M-PESA balance is Ksh 500.00. Transaction ID: XYZ123ABC.",
        "You have withdrawn Ksh 1,000.00 from 0722123456 - AGENT NAME. New M-PESA balance is Ksh 2,000.00. Transaction ID 98765ABC.",
        "You have purchased airtime Ksh 50.00 for 254700123456. New M-PESA balance is Ksh 450.00. Transaction ID: A1B2C3D.",
        "You have paid Ksh 1,000.00 to COMPANY NAME PAYBILL 543210. Account number: INV12345. New M-PESA balance Ksh 2,500.00. Transaction: 1A2B3C4D.",
        "This is not an M-Pesa message"
    ]
    
    results = []
    for message in sample_messages:
        parsed = MPesaParser.parse_message(message)
        results.append({
            "original_message": message,
            "parsed_successfully": parsed is not None,
            "parsed_data": parsed
        })
    
    return {
        "total_messages": len(sample_messages),
        "successfully_parsed": sum(1 for r in results if r["parsed_successfully"]),
        "results": results
    }
