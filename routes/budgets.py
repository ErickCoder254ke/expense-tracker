from fastapi import APIRouter, HTTPException, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase
from models.budget import Budget, BudgetCreate, BudgetUpdate
from models.user import Category
from services.budget_monitoring import BudgetMonitoringService
from typing import List, Optional
from datetime import datetime, timedelta
from bson import ObjectId
import calendar

router = APIRouter(prefix="/budgets", tags=["budgets"])

async def get_db():
    from server import db
    return db

@router.get("/", response_model=List[dict])
async def get_budgets_with_progress(
    month: int = Query(..., ge=1, le=12),
    year: int = Query(..., ge=2020, le=2030),
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get budgets for a specific month with spending progress"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")
        
        user_id = str(user_doc["_id"])
        
        # Get budgets for the specified month/year
        budgets_docs = await db.budgets.find({
            "user_id": user_id,
            "month": month,
            "year": year
        }).to_list(100)
        
        if not budgets_docs:
            return []
        
        # Convert docs to Budget objects
        budgets = []
        for doc in budgets_docs:
            doc["id"] = str(doc["_id"])
            del doc["_id"]
            budgets.append(Budget(**doc))
        
        # Calculate date range for the month
        start_date = datetime(year, month, 1)
        _, last_day = calendar.monthrange(year, month)
        end_date = datetime(year, month, last_day, 23, 59, 59)
        
        # Get spending data for each budget category
        budget_progress = []
        
        for budget in budgets:
            # Get category details
            category_doc = await db.categories.find_one({
                "_id": ObjectId(budget.category_id) if ObjectId.is_valid(budget.category_id) else budget.category_id
            })
            
            if not category_doc:
                continue
                
            category = Category(**{**category_doc, "id": str(category_doc["_id"])})
            
            # Calculate spending for this category in the specified month
            spending_pipeline = [
                {
                    "$match": {
                        "user_id": user_id,
                        "category_id": budget.category_id,
                        "type": "expense",
                        "date": {"$gte": start_date, "$lte": end_date}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total_spent": {"$sum": "$amount"},
                        "transaction_count": {"$sum": 1}
                    }
                }
            ]
            
            spending_result = await db.transactions.aggregate(spending_pipeline).to_list(1)
            spent = spending_result[0]["total_spent"] if spending_result else 0
            transaction_count = spending_result[0]["transaction_count"] if spending_result else 0
            
            # Calculate progress metrics
            remaining = budget.amount - spent
            percentage = min((spent / budget.amount) * 100, 100) if budget.amount > 0 else 0
            is_over_budget = spent > budget.amount
            
            # Determine status
            if is_over_budget:
                status = "over"
            elif percentage >= 90:
                status = "critical"
            elif percentage >= 75:
                status = "warning"
            else:
                status = "good"
            
            budget_progress.append({
                "budget": budget.dict(),
                "category": category.dict(),
                "spent": spent,
                "remaining": remaining,
                "percentage": round(percentage, 1),
                "isOverBudget": is_over_budget,
                "status": status,
                "transaction_count": transaction_count,
                "daily_average": spent / last_day if last_day > 0 else 0,
                "projected_spending": (spent / datetime.now().day) * last_day if datetime.now().month == month and datetime.now().year == year and datetime.now().day > 0 else spent
            })
        
        return budget_progress
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching budgets: {str(e)}")

@router.post("/", response_model=Budget)
async def create_budget(budget_data: BudgetCreate, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Create a new budget"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")
        
        user_id = str(user_doc["_id"])
        
        # Verify category exists
        category_doc = await db.categories.find_one({
            "_id": ObjectId(budget_data.category_id) if ObjectId.is_valid(budget_data.category_id) else budget_data.category_id
        })
        if not category_doc:
            raise HTTPException(status_code=404, detail="Category not found")
        
        # Check if budget already exists for this category/month/year
        existing_budget = await db.budgets.find_one({
            "user_id": user_id,
            "category_id": budget_data.category_id,
            "month": budget_data.month,
            "year": budget_data.year
        })
        
        if existing_budget:
            raise HTTPException(status_code=400, detail="Budget already exists for this category and period")
        
        # Create budget
        budget = Budget(**budget_data.dict(), user_id=user_id)
        result = await db.budgets.insert_one(budget.dict())
        budget.id = str(result.inserted_id)
        
        return budget
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating budget: {str(e)}")

@router.put("/{budget_id}", response_model=Budget)
async def update_budget(budget_id: str, update_data: BudgetUpdate, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Update a budget"""
    try:
        # Get existing budget
        existing_doc = await db.budgets.find_one({
            "_id": ObjectId(budget_id) if ObjectId.is_valid(budget_id) else budget_id
        })
        if not existing_doc:
            raise HTTPException(status_code=404, detail="Budget not found")
        
        # Prepare update data
        update_dict = {k: v for k, v in update_data.dict().items() if v is not None}
        
        if update_dict:
            await db.budgets.update_one(
                {"_id": ObjectId(budget_id) if ObjectId.is_valid(budget_id) else budget_id},
                {"$set": update_dict}
            )
        
        # Return updated budget
        updated_doc = await db.budgets.find_one({
            "_id": ObjectId(budget_id) if ObjectId.is_valid(budget_id) else budget_id
        })
        updated_doc["id"] = str(updated_doc["_id"])
        del updated_doc["_id"]
        
        return Budget(**updated_doc)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating budget: {str(e)}")

@router.delete("/{budget_id}")
async def delete_budget(budget_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Delete a budget"""
    try:
        result = await db.budgets.delete_one({
            "_id": ObjectId(budget_id) if ObjectId.is_valid(budget_id) else budget_id
        })
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Budget not found")
        
        return {"message": "Budget deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting budget: {str(e)}")

@router.get("/summary")
async def get_budget_summary(
    month: int = Query(..., ge=1, le=12),
    year: int = Query(..., ge=2020, le=2030),
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get overall budget summary for a month"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")
        
        user_id = str(user_doc["_id"])
        
        # Calculate date range for the month
        start_date = datetime(year, month, 1)
        _, last_day = calendar.monthrange(year, month)
        end_date = datetime(year, month, last_day, 23, 59, 59)
        
        # Get all budgets for the month
        budgets_docs = await db.budgets.find({
            "user_id": user_id,
            "month": month,
            "year": year
        }).to_list(100)
        
        total_budget = sum(doc["amount"] for doc in budgets_docs)
        
        # Get total spending for budgeted categories
        category_ids = [doc["category_id"] for doc in budgets_docs]
        
        if category_ids:
            spending_pipeline = [
                {
                    "$match": {
                        "user_id": user_id,
                        "category_id": {"$in": category_ids},
                        "type": "expense",
                        "date": {"$gte": start_date, "$lte": end_date}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total_spent": {"$sum": "$amount"},
                        "transaction_count": {"$sum": 1}
                    }
                }
            ]
            
            spending_result = await db.transactions.aggregate(spending_pipeline).to_list(1)
            total_spent = spending_result[0]["total_spent"] if spending_result else 0
            transaction_count = spending_result[0]["transaction_count"] if spending_result else 0
        else:
            total_spent = 0
            transaction_count = 0
        
        # Get spending on uncategorized expenses
        uncategorized_pipeline = [
            {
                "$match": {
                    "user_id": user_id,
                    "category_id": {"$nin": category_ids} if category_ids else {"$exists": True},
                    "type": "expense",
                    "date": {"$gte": start_date, "$lte": end_date}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "uncategorized_spent": {"$sum": "$amount"},
                    "uncategorized_count": {"$sum": 1}
                }
            }
        ]
        
        uncategorized_result = await db.transactions.aggregate(uncategorized_pipeline).to_list(1)
        uncategorized_spent = uncategorized_result[0]["uncategorized_spent"] if uncategorized_result else 0
        uncategorized_count = uncategorized_result[0]["uncategorized_count"] if uncategorized_result else 0
        
        # Calculate metrics
        total_expenses = total_spent + uncategorized_spent
        remaining_budget = total_budget - total_spent
        overall_percentage = (total_spent / total_budget * 100) if total_budget > 0 else 0
        budgets_over_limit = len([doc for doc in budgets_docs if total_spent > doc["amount"]])
        
        # Determine overall status
        if overall_percentage >= 100:
            overall_status = "over"
        elif overall_percentage >= 90:
            overall_status = "critical"
        elif overall_percentage >= 75:
            overall_status = "warning"
        else:
            overall_status = "good"
        
        return {
            "period": {"month": month, "year": year},
            "totals": {
                "budget": total_budget,
                "spent": total_spent,
                "remaining": remaining_budget,
                "percentage": round(overall_percentage, 1),
                "uncategorized_spending": uncategorized_spent,
                "total_expenses": total_expenses
            },
            "metrics": {
                "budgets_count": len(budgets_docs),
                "budgets_over_limit": budgets_over_limit,
                "transaction_count": transaction_count,
                "uncategorized_count": uncategorized_count,
                "average_daily_spending": total_spent / last_day if last_day > 0 else 0,
                "projected_month_end": (total_spent / datetime.now().day) * last_day if datetime.now().month == month and datetime.now().year == year and datetime.now().day > 0 else total_spent
            },
            "status": overall_status,
            "alerts": _generate_budget_alerts(total_budget, total_spent, overall_percentage, budgets_over_limit, uncategorized_spent)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting budget summary: {str(e)}")

@router.get("/alerts")
async def get_budget_alerts(
    month: Optional[int] = None,
    year: Optional[int] = None,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get budget alerts and recommendations"""
    try:
        # Default to current month if not specified
        if not month or not year:
            now = datetime.now()
            month = month or now.month
            year = year or now.year
        
        # Get budget progress
        budgets_progress = await get_budgets_with_progress(month=month, year=year, db=db)
        
        alerts = []
        recommendations = []
        
        for budget_data in budgets_progress:
            category_name = budget_data["category"]["name"]
            percentage = budget_data["percentage"]
            status = budget_data["status"]
            spent = budget_data["spent"]
            remaining = budget_data["remaining"]
            
            # Generate alerts based on status
            if status == "over":
                alerts.append({
                    "type": "danger",
                    "category": category_name,
                    "message": f"You've exceeded your {category_name} budget by KSh {abs(remaining):,.2f}",
                    "action": "Consider reducing spending or adjusting your budget"
                })
            elif status == "critical":
                alerts.append({
                    "type": "warning",
                    "category": category_name,
                    "message": f"You've used {percentage}% of your {category_name} budget",
                    "action": f"Only KSh {remaining:,.2f} remaining for this month"
                })
            elif status == "warning":
                alerts.append({
                    "type": "info",
                    "category": category_name,
                    "message": f"You've used {percentage}% of your {category_name} budget",
                    "action": "Monitor your spending carefully"
                })
        
        # Generate general recommendations
        if len(budgets_progress) > 0:
            total_budget = sum(b["budget"]["amount"] for b in budgets_progress)
            total_spent = sum(b["spent"] for b in budgets_progress)
            
            if total_spent / total_budget < 0.5:
                recommendations.append("You're doing great! You're well within your budget limits.")
            elif total_spent / total_budget > 1.0:
                recommendations.append("Consider reviewing and adjusting your budgets to better match your spending patterns.")
        
        return {
            "alerts": alerts,
            "recommendations": recommendations,
            "summary": {
                "total_alerts": len(alerts),
                "danger_alerts": len([a for a in alerts if a["type"] == "danger"]),
                "warning_alerts": len([a for a in alerts if a["type"] == "warning"]),
                "info_alerts": len([a for a in alerts if a["type"] == "info"])
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting budget alerts: {str(e)}")

@router.get("/monitoring/analysis")
async def get_budget_monitoring_analysis(
    month: int = Query(..., ge=1, le=12),
    year: int = Query(..., ge=2020, le=2030),
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get comprehensive budget monitoring analysis with alerts, trends, and insights"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")

        user_id = str(user_doc["_id"])

        # Use budget monitoring service for comprehensive analysis
        monitoring_service = BudgetMonitoringService(db)
        analysis = await monitoring_service.get_comprehensive_budget_analysis(user_id, month, year)

        return analysis

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting budget analysis: {str(e)}")

@router.get("/monitoring/health-score")
async def get_budget_health_score(
    month: int = Query(..., ge=1, le=12),
    year: int = Query(..., ge=2020, le=2030),
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get budget health score and status"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")

        user_id = str(user_doc["_id"])

        monitoring_service = BudgetMonitoringService(db)
        analysis = await monitoring_service.get_comprehensive_budget_analysis(user_id, month, year)

        return {
            "health_score": analysis["health_score"],
            "status": analysis["status"],
            "summary": analysis["summary"],
            "alerts_count": len(analysis["alerts"]),
            "critical_alerts": len([a for a in analysis["alerts"] if a["severity"] == "critical"])
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting budget health score: {str(e)}")

@router.get("/monitoring/trends")
async def get_spending_trends(
    month: int = Query(..., ge=1, le=12),
    year: int = Query(..., ge=2020, le=2030),
    category_id: Optional[str] = None,
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get spending trends analysis"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")

        user_id = str(user_doc["_id"])

        monitoring_service = BudgetMonitoringService(db)
        analysis = await monitoring_service.get_comprehensive_budget_analysis(user_id, month, year)

        if category_id:
            # Return trend for specific category
            trend = analysis["trends"].get(category_id)
            if not trend:
                raise HTTPException(status_code=404, detail="Category trend not found")
            return {"category_id": category_id, "trend": trend}

        # Return all trends
        return {"trends": analysis["trends"]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting spending trends: {str(e)}")

@router.get("/monitoring/goals")
async def get_budget_optimization_goals(
    month: int = Query(..., ge=1, le=12),
    year: int = Query(..., ge=2020, le=2030),
    db: AsyncIOMotorDatabase = Depends(get_db)
):
    """Get budget optimization goals and recommendations"""
    try:
        # Get user (for demo, use first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found")

        user_id = str(user_doc["_id"])

        monitoring_service = BudgetMonitoringService(db)
        analysis = await monitoring_service.get_comprehensive_budget_analysis(user_id, month, year)

        return {
            "goals": analysis["goals"],
            "insights": analysis["insights"],
            "summary": {
                "total_goals": len(analysis["goals"]),
                "total_insights": len(analysis["insights"]),
                "potential_savings": sum(
                    goal["target_amount"] - goal["current_amount"]
                    for goal in analysis["goals"]
                    if goal["type"] == "reduce_spending"
                ),
                "optimization_opportunities": len([
                    goal for goal in analysis["goals"]
                    if goal["type"] == "optimize_categories"
                ])
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting budget goals: {str(e)}")

def _generate_budget_alerts(total_budget: float, total_spent: float, percentage: float, over_limit: int, uncategorized: float) -> List[dict]:
    """Generate budget alerts based on spending patterns"""
    alerts = []

    if percentage >= 100:
        alerts.append({
            "type": "danger",
            "message": f"You've exceeded your total budget by KSh {total_spent - total_budget:,.2f}",
            "priority": "high"
        })
    elif percentage >= 90:
        alerts.append({
            "type": "warning",
            "message": f"You've used {percentage:.1f}% of your total budget",
            "priority": "medium"
        })

    if over_limit > 0:
        alerts.append({
            "type": "warning",
            "message": f"{over_limit} categories are over budget",
            "priority": "medium"
        })

    if uncategorized > total_budget * 0.2:
        alerts.append({
            "type": "info",
            "message": f"KSh {uncategorized:,.2f} spent on uncategorized expenses",
            "priority": "low"
        })

    return alerts
