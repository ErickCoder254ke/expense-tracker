from fastapi import APIRouter, HTTPException, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase
from models.transaction import Transaction, TransactionCreate, TransactionUpdate
from models.user import Category
from services.categorization import CategorizationService
from typing import List, Optional, Literal
from datetime import datetime, timedelta
from bson import ObjectId

router = APIRouter(prefix="/transactions", tags=["transactions"])

async def get_db():
    from server import db
    return db

@router.get("/", response_model=List[Transaction])
async def get_transactions(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    type_filter: Optional[Literal["expense", "income"]] = None,
    category_id: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get transactions with optional filters"""
    try:
        # Build filter query
        filter_query = {}
        if type_filter:
            filter_query["type"] = type_filter
        if category_id:
            filter_query["category_id"] = category_id
        if start_date or end_date:
            date_filter = {}
            if start_date:
                date_filter["$gte"] = start_date
            if end_date:
                date_filter["$lte"] = end_date
            filter_query["date"] = date_filter
        
        # Get transactions
        transactions_docs = await db.transactions.find(filter_query).sort("date", -1).skip(offset).limit(limit).to_list(limit)
        transactions = []
        
        for doc in transactions_docs:
            doc["id"] = str(doc["_id"])
            del doc["_id"]
            transactions.append(Transaction(**doc))
        
        return transactions
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching transactions: {str(e)}")

@router.post("/", response_model=Transaction)
async def create_transaction(transaction_data: TransactionCreate, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Create a new transaction"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")
        
        user_id = str(user_doc["_id"])
        
        # Auto-categorize if category_id is "auto"
        category_id = transaction_data.category_id
        if category_id == "auto":
            categories_docs = await db.categories.find().to_list(100)
            categories = [Category(**{**doc, "id": str(doc["_id"])}) for doc in categories_docs]
            category_id = CategorizationService.auto_categorize(transaction_data.description, categories)
        
        # Verify category exists
        category_doc = await db.categories.find_one({"_id": category_id})
        if not category_doc:
            raise HTTPException(status_code=404, detail="Category not found")
        
        # Create transaction
        transaction_dict = transaction_data.dict()
        transaction_dict['user_id'] = user_id
        transaction_dict['category_id'] = category_id
        transaction = Transaction(**transaction_dict)
        
        result = await db.transactions.insert_one(transaction.dict())
        transaction.id = str(result.inserted_id)
        
        return transaction
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating transaction: {str(e)}")

@router.get("/{transaction_id}", response_model=Transaction)
async def get_transaction(transaction_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Get a specific transaction"""
    try:
        doc = await db.transactions.find_one({"_id": transaction_id})
        if not doc:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        doc["id"] = str(doc["_id"])
        del doc["_id"]
        return Transaction(**doc)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching transaction: {str(e)}")

@router.put("/{transaction_id}", response_model=Transaction)
async def update_transaction(transaction_id: str, update_data: TransactionUpdate, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Update a transaction"""
    try:
        # Get existing transaction
        existing_doc = await db.transactions.find_one({"_id": ObjectId(transaction_id) if ObjectId.is_valid(transaction_id) else transaction_id})
        if not existing_doc:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        # Prepare update data
        update_dict = {k: v for k, v in update_data.dict().items() if v is not None}
        
        if update_dict:
            await db.transactions.update_one(
                {"_id": ObjectId(transaction_id) if ObjectId.is_valid(transaction_id) else transaction_id},
                {"$set": update_dict}
            )
        
        # Return updated transaction
        updated_doc = await db.transactions.find_one({"_id": ObjectId(transaction_id) if ObjectId.is_valid(transaction_id) else transaction_id})
        updated_doc["id"] = str(updated_doc["_id"])
        del updated_doc["_id"]
        
        return Transaction(**updated_doc)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating transaction: {str(e)}")

@router.delete("/{transaction_id}")
async def delete_transaction(transaction_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Delete a transaction"""
    try:
        result = await db.transactions.delete_one({"_id": ObjectId(transaction_id) if ObjectId.is_valid(transaction_id) else transaction_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        return {"message": "Transaction deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting transaction: {str(e)}")

@router.get("/analytics/summary")
async def get_analytics_summary(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get analytics summary for dashboard"""
    try:
        # Default to current month if no dates provided
        if not start_date:
            now = datetime.now()
            start_date = datetime(now.year, now.month, 1)
        if not end_date:
            now = datetime.now()
            if now.month == 12:
                end_date = datetime(now.year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(now.year, now.month + 1, 1) - timedelta(days=1)
        
        # Build date filter
        date_filter = {"date": {"$gte": start_date, "$lte": end_date}}
        
        # Get total income and expenses
        income_pipeline = [
            {"$match": {**date_filter, "type": "income"}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ]
        expense_pipeline = [
            {"$match": {**date_filter, "type": "expense"}},
            {"$group": {"_id": None, "total": {"$sum": "$amount"}}}
        ]
        
        income_result = await db.transactions.aggregate(income_pipeline).to_list(1)
        expense_result = await db.transactions.aggregate(expense_pipeline).to_list(1)
        
        total_income = income_result[0]["total"] if income_result else 0
        total_expenses = expense_result[0]["total"] if expense_result else 0
        
        # Get expenses by category
        category_pipeline = [
            {"$match": {**date_filter, "type": "expense"}},
            {"$group": {"_id": "$category_id", "total": {"$sum": "$amount"}, "count": {"$sum": 1}}},
            {"$sort": {"total": -1}}
        ]
        
        category_results = await db.transactions.aggregate(category_pipeline).to_list(20)
        
        # Get category names
        categories_by_category = {}
        for result in category_results:
            category_doc = await db.categories.find_one({"_id": ObjectId(result["_id"]) if ObjectId.is_valid(result["_id"]) else result["_id"]})
            if category_doc:
                categories_by_category[result["_id"]] = {
                    "name": category_doc["name"],
                    "color": category_doc["color"],
                    "icon": category_doc["icon"],
                    "amount": result["total"],
                    "count": result["count"]
                }
        
        # Get recent transactions
        recent_transactions_docs = await db.transactions.find(date_filter).sort("date", -1).limit(5).to_list(5)
        recent_transactions = []
        
        for doc in recent_transactions_docs:
            doc["id"] = str(doc["_id"])
            del doc["_id"]
            recent_transactions.append(Transaction(**doc))
        
        return {
            "period": {"start_date": start_date, "end_date": end_date},
            "totals": {
                "income": total_income,
                "expenses": total_expenses,
                "balance": total_income - total_expenses
            },
            "categories": categories_by_category,
            "recent_transactions": recent_transactions
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting analytics: {str(e)}")