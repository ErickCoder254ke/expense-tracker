from fastapi import APIRouter, HTTPException, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase
from models.transaction import Transaction, TransactionCreate, TransactionUpdate
from models.user import Category
from services.categorization import CategorizationService
from services.frequency_analyzer import TransactionFrequencyAnalyzer, FrequentTransaction
from typing import List, Optional, Literal
from datetime import datetime, timedelta
from bson import ObjectId
from pydantic import BaseModel

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
        category_doc = await db.categories.find_one({"_id": ObjectId(category_id) if ObjectId.is_valid(category_id) else category_id})
        if not category_doc:
            raise HTTPException(status_code=404, detail="Category not found")
        
        # Create transaction
        transaction_dict = transaction_data.dict()
        transaction_dict['user_id'] = user_id
        transaction_dict['category_id'] = category_id
        transaction = Transaction(**transaction_dict)

        print(f"Creating transaction: {transaction.dict()}")
        result = await db.transactions.insert_one(transaction.dict())
        transaction.id = str(result.inserted_id)
        print(f"Transaction created with ID: {transaction.id}")

        # Verify it was saved
        saved_transaction = await db.transactions.find_one({"_id": result.inserted_id})
        print(f"Saved transaction verification: {saved_transaction}")

        return transaction
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating transaction: {str(e)}")

@router.get("/{transaction_id}", response_model=Transaction)
async def get_transaction(transaction_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Get a specific transaction"""
    try:
        doc = await db.transactions.find_one({"_id": ObjectId(transaction_id) if ObjectId.is_valid(transaction_id) else transaction_id})
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

        print(f"Analytics query date range: {start_date} to {end_date}")

        # First, let's check if we have any transactions at all
        total_transactions = await db.transactions.count_documents({})
        print(f"Total transactions in database: {total_transactions}")

        # Let's also see what transactions exist without date filter
        all_transactions = await db.transactions.find({}).to_list(100)
        print(f"Sample transactions: {all_transactions[:3] if all_transactions else 'None'}")

        # Build date filter
        date_filter = {"date": {"$gte": start_date, "$lte": end_date}}
        print(f"Date filter: {date_filter}")

        # Check transactions in date range
        date_filtered_count = await db.transactions.count_documents(date_filter)
        print(f"Transactions in date range: {date_filtered_count}")
        
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

        # Enhanced transaction fees summary - capture all fee types
        fees_pipeline = [
            {"$match": date_filter},
            {
                "$group": {
                    "_id": None,
                    "total_transaction_fees": {
                        "$sum": {
                            "$ifNull": ["$mpesa_details.transaction_fee", 0]
                        }
                    },
                    "total_access_fees": {
                        "$sum": {
                            "$ifNull": ["$mpesa_details.access_fee", 0]
                        }
                    },
                    "total_service_fees": {
                        "$sum": {
                            "$ifNull": ["$mpesa_details.service_fee", 0]
                        }
                    },
                    "total_from_sms_metadata": {
                        "$sum": {
                            "$ifNull": ["$sms_metadata.total_fees", 0]
                        }
                    },
                    "fee_transactions_count": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$or": [
                                        {"$gt": [{"$ifNull": ["$mpesa_details.transaction_fee", 0]}, 0]},
                                        {"$gt": [{"$ifNull": ["$mpesa_details.access_fee", 0]}, 0]},
                                        {"$gt": [{"$ifNull": ["$mpesa_details.service_fee", 0]}, 0]},
                                        {"$gt": [{"$ifNull": ["$sms_metadata.total_fees", 0]}, 0]}
                                    ]
                                },
                                1,
                                0
                            ]
                        }
                    },
                    "transactions_with_parsed_fees": {
                        "$sum": {
                            "$cond": [
                                {"$gt": [{"$ifNull": ["$sms_metadata.total_fees", 0]}, 0]},
                                1,
                                0
                            ]
                        }
                    }
                }
            }
        ]

        fees_result = await db.transactions.aggregate(fees_pipeline).to_list(1)
        fees_data = fees_result[0] if fees_result else {
            "total_transaction_fees": 0,
            "total_access_fees": 0,
            "total_service_fees": 0,
            "total_from_sms_metadata": 0,
            "fee_transactions_count": 0,
            "transactions_with_parsed_fees": 0
        }

        # Calculate total fees prioritizing SMS metadata if available, otherwise sum individual fees
        total_fees = max(
            fees_data["total_from_sms_metadata"],
            fees_data["total_transaction_fees"] + fees_data["total_access_fees"] + fees_data["total_service_fees"]
        )

        # Enhanced Fuliza summary for comprehensive tracking
        fuliza_pipeline = [
            {"$match": {**date_filter, "$or": [
                {"description": {"$regex": "fuliza", "$options": "i"}},
                {"mpesa_details.message_type": {"$in": ["fuliza_loan", "fuliza_repayment", "compound_received_fuliza"]}}
            ]}},
            {
                "$group": {
                    "_id": None,
                    "total_loans": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$or": [
                                        {"$eq": ["$mpesa_details.message_type", "fuliza_loan"]},
                                        {"$and": [
                                            {"$eq": ["$type", "income"]},
                                            {"$regexMatch": {"input": "$description", "regex": "fuliza", "options": "i"}}
                                        ]}
                                    ]
                                },
                                "$amount",
                                0
                            ]
                        }
                    },
                    "total_repayments": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$or": [
                                        {"$eq": ["$mpesa_details.message_type", "fuliza_repayment"]},
                                        {"$and": [
                                            {"$eq": ["$type", "expense"]},
                                            {"$regexMatch": {"input": "$description", "regex": "fuliza.*repay", "options": "i"}}
                                        ]}
                                    ]
                                },
                                "$amount",
                                0
                            ]
                        }
                    },
                    "loan_count": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$or": [
                                        {"$eq": ["$mpesa_details.message_type", "fuliza_loan"]},
                                        {"$and": [
                                            {"$eq": ["$type", "income"]},
                                            {"$regexMatch": {"input": "$description", "regex": "fuliza", "options": "i"}}
                                        ]}
                                    ]
                                },
                                1,
                                0
                            ]
                        }
                    },
                    "repayment_count": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$or": [
                                        {"$eq": ["$mpesa_details.message_type", "fuliza_repayment"]},
                                        {"$and": [
                                            {"$eq": ["$type", "expense"]},
                                            {"$regexMatch": {"input": "$description", "regex": "fuliza.*repay", "options": "i"}}
                                        ]}
                                    ]
                                },
                                1,
                                0
                            ]
                        }
                    },
                    "current_outstanding": {
                        "$last": {"$ifNull": ["$mpesa_details.fuliza_outstanding", 0]}
                    }
                }
            }
        ]

        fuliza_result = await db.transactions.aggregate(fuliza_pipeline).to_list(1)
        fuliza_data = fuliza_result[0] if fuliza_result else {
            "total_loans": 0,
            "total_repayments": 0,
            "loan_count": 0,
            "repayment_count": 0,
            "current_outstanding": 0
        }

        # Add fuliza summary to response if there's activity
        response_data = {
            "period": {"start_date": start_date, "end_date": end_date},
            "totals": {
                "income": total_income,
                "expenses": total_expenses,
                "balance": total_income - total_expenses,
                "fees": {
                    "total_fees": total_fees,
                    "transaction_fees": fees_data["total_transaction_fees"],
                    "access_fees": fees_data["total_access_fees"],
                    "fee_transactions_count": fees_data["fee_transactions_count"],
                    "service_fees": fees_data.get("total_service_fees", 0),
                    "transactions_with_parsed_fees": fees_data.get("transactions_with_parsed_fees", 0)
                }
            },
            "categories": categories_by_category,
            "recent_transactions": recent_transactions
        }

        # Add Fuliza data if there's any activity
        if (fuliza_data["total_loans"] > 0 or fuliza_data["total_repayments"] > 0 or fuliza_data["current_outstanding"] > 0):
            response_data["fuliza_summary"] = fuliza_data

        return response_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting analytics: {str(e)}")

@router.get("/charges/analytics")
async def get_transaction_charges_analytics(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    period: str = Query("month", regex="^(week|month|quarter|year)$"),
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get comprehensive transaction charges analytics for every shilling tracking"""
    try:
        # Default to current period if no dates provided
        if not start_date or not end_date:
            now = datetime.now()
            if period == "week":
                start_date = now - timedelta(days=7)
                end_date = now
            elif period == "month":
                start_date = datetime(now.year, now.month, 1)
                if now.month == 12:
                    end_date = datetime(now.year + 1, 1, 1) - timedelta(days=1)
                else:
                    end_date = datetime(now.year, now.month + 1, 1) - timedelta(days=1)
            elif period == "quarter":
                quarter = (now.month - 1) // 3 + 1
                start_month = (quarter - 1) * 3 + 1
                start_date = datetime(now.year, start_month, 1)
                if quarter == 4:
                    end_date = datetime(now.year + 1, 1, 1) - timedelta(days=1)
                else:
                    end_date = datetime(now.year, start_month + 3, 1) - timedelta(days=1)
            elif period == "year":
                start_date = datetime(now.year, 1, 1)
                end_date = datetime(now.year, 12, 31)

        date_filter = {"date": {"$gte": start_date, "$lte": end_date}}

        # Comprehensive fee analysis pipeline
        charges_pipeline = [
            {"$match": date_filter},
            {
                "$group": {
                    "_id": None,
                    # M-Pesa transaction fees
                    "total_transaction_fees": {
                        "$sum": {"$ifNull": ["$mpesa_details.transaction_fee", 0]}
                    },
                    "transaction_fee_count": {
                        "$sum": {
                            "$cond": [
                                {"$gt": [{"$ifNull": ["$mpesa_details.transaction_fee", 0]}, 0]},
                                1, 0
                            ]
                        }
                    },
                    # Fuliza access fees
                    "total_access_fees": {
                        "$sum": {"$ifNull": ["$mpesa_details.access_fee", 0]}
                    },
                    "access_fee_count": {
                        "$sum": {
                            "$cond": [
                                {"$gt": [{"$ifNull": ["$mpesa_details.access_fee", 0]}, 0]},
                                1, 0
                            ]
                        }
                    },
                    # Service fees
                    "total_service_fees": {
                        "$sum": {"$ifNull": ["$mpesa_details.service_fee", 0]}
                    },
                    "service_fee_count": {
                        "$sum": {
                            "$cond": [
                                {"$gt": [{"$ifNull": ["$mpesa_details.service_fee", 0]}, 0]},
                                1, 0
                            ]
                        }
                    },
                    # SMS metadata fees (comprehensive fee tracking)
                    "total_sms_fees": {
                        "$sum": {"$ifNull": ["$sms_metadata.total_fees", 0]}
                    },
                    "sms_fee_count": {
                        "$sum": {
                            "$cond": [
                                {"$gt": [{"$ifNull": ["$sms_metadata.total_fees", 0]}, 0]},
                                1, 0
                            ]
                        }
                    },
                    # Fee transactions (role-based)
                    "fee_role_transactions": {
                        "$sum": {
                            "$cond": [
                                {"$in": ["$transaction_role", ["fee", "access_fee"]]},
                                "$amount", 0
                            ]
                        }
                    },
                    "fee_role_count": {
                        "$sum": {
                            "$cond": [
                                {"$in": ["$transaction_role", ["fee", "access_fee"]]},
                                1, 0
                            ]
                        }
                    },
                    # Total transactions for context
                    "total_transactions": {"$sum": 1},
                    "total_amount_transacted": {"$sum": "$amount"},
                    "expense_transactions": {
                        "$sum": {
                            "$cond": [{"$eq": ["$type", "expense"]}, 1, 0]
                        }
                    },
                    "expense_amount": {
                        "$sum": {
                            "$cond": [{"$eq": ["$type", "expense"]}, "$amount", 0]
                        }
                    }
                }
            }
        ]

        charges_result = await db.transactions.aggregate(charges_pipeline).to_list(1)
        charges_data = charges_result[0] if charges_result else {}

        # Calculate comprehensive fee totals
        transaction_fees = charges_data.get("total_transaction_fees", 0)
        access_fees = charges_data.get("total_access_fees", 0)
        service_fees = charges_data.get("total_service_fees", 0)
        sms_fees = charges_data.get("total_sms_fees", 0)
        fee_role_fees = charges_data.get("fee_role_transactions", 0)

        # Use the highest fee total (SMS metadata is most comprehensive)
        total_fees = max(sms_fees, transaction_fees + access_fees + service_fees, fee_role_fees)

        # Fee breakdown by type
        fee_breakdown = {
            "transaction_fees": {
                "amount": transaction_fees,
                "count": charges_data.get("transaction_fee_count", 0),
                "description": "M-Pesa transaction charges"
            },
            "access_fees": {
                "amount": access_fees,
                "count": charges_data.get("access_fee_count", 0),
                "description": "Fuliza access fees"
            },
            "service_fees": {
                "amount": service_fees,
                "count": charges_data.get("service_fee_count", 0),
                "description": "Bank and service charges"
            },
            "role_based_fees": {
                "amount": fee_role_fees,
                "count": charges_data.get("fee_role_count", 0),
                "description": "Dedicated fee transactions"
            }
        }

        # Fee efficiency metrics
        total_transactions = charges_data.get("total_transactions", 0)
        expense_amount = charges_data.get("expense_amount", 0)
        expense_transactions = charges_data.get("expense_transactions", 0)

        efficiency_metrics = {
            "average_fee_per_transaction": total_fees / total_transactions if total_transactions > 0 else 0,
            "fee_percentage_of_expenses": (total_fees / expense_amount * 100) if expense_amount > 0 else 0,
            "fee_efficiency_rating": "excellent" if (total_fees / expense_amount * 100) < 2 else "good" if (total_fees / expense_amount * 100) < 5 else "fair" if (total_fees / expense_amount * 100) < 10 else "poor",
            "transactions_with_fees": sum([
                charges_data.get("transaction_fee_count", 0),
                charges_data.get("access_fee_count", 0),
                charges_data.get("service_fee_count", 0),
                charges_data.get("fee_role_count", 0)
            ]),
            "fee_free_transactions": total_transactions - sum([
                charges_data.get("transaction_fee_count", 0),
                charges_data.get("access_fee_count", 0),
                charges_data.get("service_fee_count", 0),
                charges_data.get("fee_role_count", 0)
            ])
        }

        # Monthly fee trend (if period is longer than a month)
        fee_trend = []
        if period in ["quarter", "year"]:
            # Get monthly breakdown
            trend_pipeline = [
                {"$match": date_filter},
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$date"},
                            "month": {"$month": "$date"}
                        },
                        "monthly_fees": {
                            "$sum": {"$ifNull": ["$sms_metadata.total_fees",
                                {"$add": [
                                    {"$ifNull": ["$mpesa_details.transaction_fee", 0]},
                                    {"$ifNull": ["$mpesa_details.access_fee", 0]},
                                    {"$ifNull": ["$mpesa_details.service_fee", 0]}
                                ]}
                            ]}
                        },
                        "transaction_count": {"$sum": 1}
                    }
                },
                {"$sort": {"_id.year": 1, "_id.month": 1}}
            ]

            trend_result = await db.transactions.aggregate(trend_pipeline).to_list(12)
            fee_trend = [
                {
                    "period": f"{item['_id']['year']}-{item['_id']['month']:02d}",
                    "fees": item["monthly_fees"],
                    "transactions": item["transaction_count"]
                } for item in trend_result
            ]

        # Fee optimization suggestions
        suggestions = []
        fee_percentage = efficiency_metrics["fee_percentage_of_expenses"]

        if fee_percentage > 10:
            suggestions.append("Consider consolidating smaller transactions to reduce per-transaction fees")
        if charges_data.get("access_fee_count", 0) > 5:
            suggestions.append("Frequent Fuliza usage detected - consider maintaining higher M-Pesa balance")
        if fee_percentage < 2:
            suggestions.append("Excellent fee efficiency! You're using M-Pesa very cost-effectively")
        elif fee_percentage < 5:
            suggestions.append("Good fee management. Minor optimizations possible")

        return {
            "period": {"start_date": start_date, "end_date": end_date, "type": period},
            "summary": {
                "total_fees": total_fees,
                "total_transactions": total_transactions,
                "expense_amount": expense_amount,
                "fee_source": "enhanced_parsing" if sms_fees > (transaction_fees + access_fees) else "mpesa_details"
            },
            "fee_breakdown": fee_breakdown,
            "efficiency_metrics": efficiency_metrics,
            "fee_trend": fee_trend,
            "optimization_suggestions": suggestions,
            "data_quality": {
                "sms_parsed_fees": charges_data.get("sms_fee_count", 0),
                "mpesa_detail_fees": charges_data.get("transaction_fee_count", 0) + charges_data.get("access_fee_count", 0),
                "dedicated_fee_transactions": charges_data.get("fee_role_count", 0),
                "completeness_score": min(100, (charges_data.get("sms_fee_count", 0) / max(total_transactions, 1) * 100))
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting charges analytics: {str(e)}")
        
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
        
        return response_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting analytics: {str(e)}")

@router.get("/debug/database")
async def debug_database(db: AsyncIOMotorDatabase = Depends(get_db)):
    """Debug endpoint to check database contents"""
    try:
        # Get all transactions
        all_transactions = await db.transactions.find({}).to_list(100)

        # Get all categories
        all_categories = await db.categories.find({}).to_list(100)

        # Get all users
        all_users = await db.users.find({}).to_list(100)

        return {
            "transactions_count": len(all_transactions),
            "categories_count": len(all_categories),
            "users_count": len(all_users),
            "sample_transactions": all_transactions[:5],
            "sample_categories": all_categories[:3],
            "sample_users": all_users[:1] if all_users else []
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting debug info: {str(e)}")

# Frequency Analysis Endpoints

class CategoryUpdateRequest(BaseModel):
    category_id: str
    transaction_ids: List[str]
    pattern: str

class PatternReviewRequest(BaseModel):
    pattern: str
    transaction_ids: List[str]
    action: Literal["categorize", "dismiss"]

@router.get("/frequency-analysis")
async def get_frequent_transactions(
    min_frequency: int = Query(3, ge=2, le=10),
    days_back: int = Query(90, ge=7, le=365),
    uncategorized_only: bool = Query(True),
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get frequently occurring transactions that may need categorization"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")

        user_id = str(user_doc["_id"])

        # Initialize frequency analyzer
        analyzer = TransactionFrequencyAnalyzer(db)

        if uncategorized_only:
            frequent_transactions = await analyzer.get_uncategorized_frequent_transactions(
                user_id, min_frequency
            )
        else:
            frequent_transactions = await analyzer.analyze_frequent_transactions(
                user_id, min_frequency, days_back
            )

        # Convert to dict format for JSON response
        result = []
        for ft in frequent_transactions:
            result.append({
                "pattern": ft.pattern,
                "description_samples": ft.description_samples,
                "count": ft.count,
                "total_amount": ft.total_amount,
                "avg_amount": ft.avg_amount,
                "category_id": ft.category_id,
                "category_name": ft.category_name,
                "first_seen": ft.first_seen,
                "last_seen": ft.last_seen,
                "transaction_ids": ft.transaction_ids,
                "confidence_score": ft.confidence_score,
                "suggested_category": ft.suggested_category,
                "needs_attention": not ft.category_id or ft.category_name == "Other" or ft.confidence_score < 0.7
            })

        return {
            "frequent_transactions": result,
            "summary": {
                "total_patterns": len(result),
                "needs_categorization": len([r for r in result if r["needs_attention"]]),
                "analysis_period_days": days_back,
                "min_frequency_threshold": min_frequency
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing transaction frequency: {str(e)}")

@router.post("/frequency-analysis/categorize")
async def categorize_frequent_pattern(
    request: CategoryUpdateRequest,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Apply a category to all transactions matching a frequent pattern"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")

        user_id = str(user_doc["_id"])

        # Verify category exists
        category_doc = await db.categories.find_one({
            "_id": ObjectId(request.category_id) if ObjectId.is_valid(request.category_id) else request.category_id
        })
        if not category_doc:
            raise HTTPException(status_code=404, detail="Category not found")

        # Initialize frequency analyzer
        analyzer = TransactionFrequencyAnalyzer(db)

        # Apply category to pattern
        updated_count = await analyzer.apply_category_to_pattern(
            user_id,
            request.pattern,
            request.category_id,
            request.transaction_ids
        )

        return {
            "message": f"Successfully categorized {updated_count} transactions",
            "updated_count": updated_count,
            "category_name": category_doc["name"],
            "pattern": request.pattern
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error categorizing pattern: {str(e)}")

@router.post("/frequency-analysis/review")
async def review_frequent_pattern(
    request: PatternReviewRequest,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Mark a frequent transaction pattern as reviewed"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")

        user_id = str(user_doc["_id"])

        # Initialize frequency analyzer
        analyzer = TransactionFrequencyAnalyzer(db)

        if request.action == "dismiss":
            # Mark pattern as reviewed to avoid future prompts
            updated_count = await analyzer.mark_pattern_as_reviewed(
                user_id,
                request.pattern,
                request.transaction_ids
            )

            return {
                "message": f"Pattern dismissed. {updated_count} transactions marked as reviewed",
                "updated_count": updated_count,
                "action": "dismissed"
            }

        return {"message": "Invalid action", "action": request.action}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reviewing pattern: {str(e)}")

@router.get("/categorization-suggestions")
async def get_categorization_suggestions(
    limit: int = Query(10, ge=1, le=50),
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get smart categorization suggestions based on frequent patterns"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")

        user_id = str(user_doc["_id"])

        # Initialize frequency analyzer
        analyzer = TransactionFrequencyAnalyzer(db)

        # Get uncategorized frequent transactions
        frequent_transactions = await analyzer.get_uncategorized_frequent_transactions(user_id)

        # Create suggestions
        suggestions = []
        for ft in frequent_transactions[:limit]:
            suggestion = {
                "pattern": ft.pattern,
                "description_sample": ft.description_samples[0] if ft.description_samples else "No description",
                "count": ft.count,
                "total_amount": ft.total_amount,
                "suggested_category": ft.suggested_category,
                "confidence_score": ft.confidence_score,
                "transaction_ids": ft.transaction_ids,
                "priority": "high" if ft.count >= 5 and ft.total_amount > 1000 else "medium" if ft.count >= 3 else "low",
                "potential_savings": f"Could save time on {ft.count} future similar transactions"
            }
            suggestions.append(suggestion)

        # Sort by priority and impact
        suggestions.sort(key=lambda x: (x["count"], x["total_amount"]), reverse=True)

        return {
            "suggestions": suggestions,
            "summary": {
                "total_suggestions": len(suggestions),
                "high_priority": len([s for s in suggestions if s["priority"] == "high"]),
                "potential_time_saved": f"{sum(s['count'] for s in suggestions)} future categorizations"
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting categorization suggestions: {str(e)}")
