from typing import List, Dict, Optional, Tuple
from motor.motor_asyncio import AsyncIOMotorDatabase
from models.budget import Budget
from models.transaction import Transaction
from models.user import Category
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from bson import ObjectId
import calendar

class AlertSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class BudgetStatus(Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    WARNING = "warning"
    DANGER = "danger"
    CRITICAL = "critical"

@dataclass
class BudgetAlert:
    type: str
    severity: AlertSeverity
    category_id: Optional[str]
    category_name: Optional[str]
    title: str
    message: str
    action_required: str
    amount: Optional[float] = None
    percentage: Optional[float] = None
    days_remaining: Optional[int] = None
    trend: Optional[str] = None

@dataclass
class SpendingTrend:
    direction: str  # "increasing", "decreasing", "stable"
    percentage_change: float
    days_analyzed: int
    prediction: str
    confidence: float

@dataclass
class BudgetInsight:
    type: str
    title: str
    description: str
    impact: str  # "positive", "negative", "neutral"
    recommendation: str
    potential_savings: Optional[float] = None
    category_id: Optional[str] = None

@dataclass
class BudgetGoal:
    type: str  # "reduce_spending", "increase_budget", "optimize_categories"
    category_id: Optional[str]
    category_name: Optional[str]
    current_amount: float
    target_amount: float
    timeframe: str
    difficulty: str  # "easy", "moderate", "challenging"
    potential_impact: str

class BudgetMonitoringService:
    """Comprehensive budget monitoring and intelligence service"""
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
    
    async def get_comprehensive_budget_analysis(
        self, 
        user_id: str, 
        month: int, 
        year: int
    ) -> Dict:
        """Get comprehensive budget analysis including alerts, trends, and insights"""
        
        # Get budget data
        budgets = await self._get_budgets_with_spending(user_id, month, year)
        
        if not budgets:
            return self._empty_analysis(month, year)
        
        # Generate alerts
        alerts = await self._generate_comprehensive_alerts(user_id, budgets, month, year)
        
        # Analyze spending trends
        trends = await self._analyze_spending_trends(user_id, budgets, month, year)
        
        # Generate insights
        insights = await self._generate_budget_insights(user_id, budgets, trends, month, year)
        
        # Generate optimization goals
        goals = await self._generate_budget_goals(user_id, budgets, trends, month, year)
        
        # Calculate overall health score
        health_score = self._calculate_budget_health_score(budgets, alerts)
        
        return {
            "period": {"month": month, "year": year},
            "health_score": health_score,
            "status": self._determine_overall_status(budgets, alerts),
            "budgets": budgets,
            "alerts": [alert.__dict__ for alert in alerts],
            "trends": {cat_id: trend.__dict__ for cat_id, trend in trends.items()},
            "insights": [insight.__dict__ for insight in insights],
            "goals": [goal.__dict__ for goal in goals],
            "summary": self._generate_summary(budgets, alerts, trends)
        }
    
    async def _get_budgets_with_spending(
        self, 
        user_id: str, 
        month: int, 
        year: int
    ) -> List[Dict]:
        """Get budgets with detailed spending analysis"""
        
        # Date range for the month
        start_date = datetime(year, month, 1)
        _, last_day = calendar.monthrange(year, month)
        end_date = datetime(year, month, last_day, 23, 59, 59)
        current_day = datetime.now().day if datetime.now().month == month and datetime.now().year == year else last_day
        
        # Get budgets
        budgets_docs = await self.db.budgets.find({
            "user_id": user_id,
            "month": month,
            "year": year
        }).to_list(100)
        
        budgets_with_spending = []
        
        for budget_doc in budgets_docs:
            budget = Budget(**{**budget_doc, "id": str(budget_doc["_id"])})
            
            # Get category info
            category_doc = await self.db.categories.find_one({
                "_id": ObjectId(budget.category_id) if ObjectId.is_valid(budget.category_id) else budget.category_id
            })
            if not category_doc:
                continue
            
            category = Category(**{**category_doc, "id": str(category_doc["_id"])})
            
            # Get spending details
            spending_data = await self._get_detailed_spending(
                user_id, budget.category_id, start_date, end_date, current_day, last_day
            )
            
            budgets_with_spending.append({
                "budget": budget,
                "category": category,
                **spending_data
            })
        
        return budgets_with_spending
    
    async def _get_detailed_spending(
        self, 
        user_id: str, 
        category_id: str, 
        start_date: datetime, 
        end_date: datetime,
        current_day: int,
        total_days: int
    ) -> Dict:
        """Get detailed spending analysis for a category"""
        
        # Overall spending
        total_pipeline = [
            {
                "$match": {
                    "user_id": user_id,
                    "category_id": category_id,
                    "type": "expense",
                    "date": {"$gte": start_date, "$lte": end_date}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_spent": {"$sum": "$amount"},
                    "transaction_count": {"$sum": 1},
                    "avg_transaction": {"$avg": "$amount"},
                    "max_transaction": {"$max": "$amount"},
                    "min_transaction": {"$min": "$amount"}
                }
            }
        ]
        
        total_result = await self.db.transactions.aggregate(total_pipeline).to_list(1)
        total_data = total_result[0] if total_result else {
            "total_spent": 0, "transaction_count": 0, "avg_transaction": 0,
            "max_transaction": 0, "min_transaction": 0
        }
        
        # Daily spending pattern
        daily_pipeline = [
            {
                "$match": {
                    "user_id": user_id,
                    "category_id": category_id,
                    "type": "expense",
                    "date": {"$gte": start_date, "$lte": end_date}
                }
            },
            {
                "$group": {
                    "_id": {"$dayOfMonth": "$date"},
                    "daily_amount": {"$sum": "$amount"},
                    "daily_count": {"$sum": 1}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        daily_result = await self.db.transactions.aggregate(daily_pipeline).to_list(31)
        
        # Weekly spending
        weekly_pipeline = [
            {
                "$match": {
                    "user_id": user_id,
                    "category_id": category_id,
                    "type": "expense",
                    "date": {"$gte": start_date, "$lte": end_date}
                }
            },
            {
                "$group": {
                    "_id": {"$week": "$date"},
                    "weekly_amount": {"$sum": "$amount"},
                    "weekly_count": {"$sum": 1}
                }
            }
        ]
        
        weekly_result = await self.db.transactions.aggregate(weekly_pipeline).to_list(5)
        
        total_spent = total_data["total_spent"]
        
        return {
            "total_spent": total_spent,
            "transaction_count": total_data["transaction_count"],
            "avg_transaction": total_data["avg_transaction"],
            "max_transaction": total_data["max_transaction"],
            "min_transaction": total_data["min_transaction"],
            "daily_average": total_spent / current_day if current_day > 0 else 0,
            "monthly_average": total_spent / total_days * 30,
            "projected_spending": (total_spent / current_day) * total_days if current_day > 0 else total_spent,
            "spending_velocity": total_spent / current_day if current_day > 0 else 0,
            "daily_pattern": daily_result,
            "weekly_pattern": weekly_result,
            "days_with_spending": len(daily_result),
            "spending_frequency": len(daily_result) / total_days if total_days > 0 else 0
        }
    
    async def _generate_comprehensive_alerts(
        self, 
        user_id: str, 
        budgets: List[Dict], 
        month: int, 
        year: int
    ) -> List[BudgetAlert]:
        """Generate comprehensive budget alerts"""
        
        alerts = []
        current_date = datetime.now()
        _, last_day = calendar.monthrange(year, month)
        days_remaining = max(0, last_day - current_date.day) if current_date.month == month and current_date.year == year else 0
        
        for budget_data in budgets:
            budget = budget_data["budget"]
            category = budget_data["category"]
            spent = budget_data["total_spent"]
            projected = budget_data["projected_spending"]
            
            percentage = (spent / budget.amount * 100) if budget.amount > 0 else 0
            projected_percentage = (projected / budget.amount * 100) if budget.amount > 0 else 0
            
            # Critical: Over budget
            if spent > budget.amount:
                overspend = spent - budget.amount
                alerts.append(BudgetAlert(
                    type="overspend",
                    severity=AlertSeverity.CRITICAL,
                    category_id=budget.category_id,
                    category_name=category.name,
                    title=f"{category.name} Budget Exceeded",
                    message=f"You've spent KSh {overspend:,.2f} more than your {category.name} budget",
                    action_required="Reduce spending immediately or adjust budget",
                    amount=overspend,
                    percentage=percentage,
                    days_remaining=days_remaining
                ))
            
            # High: Projected to exceed
            elif projected > budget.amount and days_remaining > 0:
                projected_overspend = projected - budget.amount
                alerts.append(BudgetAlert(
                    type="projected_overspend",
                    severity=AlertSeverity.HIGH,
                    category_id=budget.category_id,
                    category_name=category.name,
                    title=f"{category.name} Projected to Exceed Budget",
                    message=f"At current pace, you'll exceed budget by KSh {projected_overspend:,.2f}",
                    action_required="Reduce daily spending to stay within budget",
                    amount=projected_overspend,
                    percentage=projected_percentage,
                    days_remaining=days_remaining,
                    trend="increasing"
                ))
            
            # Medium: High spending velocity
            elif percentage >= 80:
                remaining = budget.amount - spent
                alerts.append(BudgetAlert(
                    type="high_usage",
                    severity=AlertSeverity.MEDIUM,
                    category_id=budget.category_id,
                    category_name=category.name,
                    title=f"{category.name} Budget {percentage:.0f}% Used",
                    message=f"Only KSh {remaining:,.2f} remaining in your {category.name} budget",
                    action_required="Monitor spending carefully",
                    amount=remaining,
                    percentage=percentage,
                    days_remaining=days_remaining
                ))
            
            # Spending pattern alerts
            if budget_data["transaction_count"] > 0:
                avg_transaction = budget_data["avg_transaction"]
                max_transaction = budget_data["max_transaction"]
                
                # Large transaction alert
                if max_transaction > budget.amount * 0.3:
                    alerts.append(BudgetAlert(
                        type="large_transaction",
                        severity=AlertSeverity.MEDIUM,
                        category_id=budget.category_id,
                        category_name=category.name,
                        title=f"Large {category.name} Transaction",
                        message=f"Recent transaction of KSh {max_transaction:,.2f} is unusually large",
                        action_required="Review if this aligns with your budget goals",
                        amount=max_transaction,
                        percentage=max_transaction / budget.amount * 100
                    ))
                
                # Spending frequency alert
                spending_frequency = budget_data["spending_frequency"]
                if spending_frequency > 0.8:  # Spending most days
                    alerts.append(BudgetAlert(
                        type="frequent_spending",
                        severity=AlertSeverity.LOW,
                        category_id=budget.category_id,
                        category_name=category.name,
                        title=f"Frequent {category.name} Spending",
                        message=f"You've spent on {category.name} {budget_data['days_with_spending']} days this month",
                        action_required="Consider if daily spending aligns with your goals",
                        percentage=spending_frequency * 100
                    ))
        
        # Overall budget alerts
        total_budget = sum(b["budget"].amount for b in budgets)
        total_spent = sum(b["total_spent"] for b in budgets)
        
        if total_budget > 0:
            overall_percentage = total_spent / total_budget * 100
            
            if overall_percentage >= 100:
                alerts.append(BudgetAlert(
                    type="total_overspend",
                    severity=AlertSeverity.CRITICAL,
                    category_id=None,
                    category_name=None,
                    title="Total Budget Exceeded",
                    message=f"You've exceeded your total monthly budget",
                    action_required="Review all spending categories immediately",
                    amount=total_spent - total_budget,
                    percentage=overall_percentage
                ))
        
        return alerts
    
    async def _analyze_spending_trends(
        self, 
        user_id: str, 
        budgets: List[Dict], 
        month: int, 
        year: int
    ) -> Dict[str, SpendingTrend]:
        """Analyze spending trends for each budget category"""
        
        trends = {}
        
        for budget_data in budgets:
            category_id = budget_data["budget"].category_id
            
            # Get historical data for trend analysis
            trend = await self._calculate_category_trend(user_id, category_id, month, year)
            trends[category_id] = trend
        
        return trends
    
    async def _calculate_category_trend(
        self, 
        user_id: str, 
        category_id: str, 
        month: int, 
        year: int
    ) -> SpendingTrend:
        """Calculate spending trend for a specific category"""
        
        # Get last 3 months of data
        current_month_start = datetime(year, month, 1)
        months_back = []
        
        for i in range(3):
            if month - i <= 0:
                prev_month = 12 + (month - i)
                prev_year = year - 1
            else:
                prev_month = month - i
                prev_year = year
            
            month_start = datetime(prev_year, prev_month, 1)
            _, last_day = calendar.monthrange(prev_year, prev_month)
            month_end = datetime(prev_year, prev_month, last_day, 23, 59, 59)
            months_back.append((month_start, month_end))
        
        # Get spending for each month
        monthly_spending = []
        for start_date, end_date in months_back:
            spending_pipeline = [
                {
                    "$match": {
                        "user_id": user_id,
                        "category_id": category_id,
                        "type": "expense",
                        "date": {"$gte": start_date, "$lte": end_date}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "total": {"$sum": "$amount"}
                    }
                }
            ]
            
            result = await self.db.transactions.aggregate(spending_pipeline).to_list(1)
            spending = result[0]["total"] if result else 0
            monthly_spending.append(spending)
        
        # Calculate trend
        if len(monthly_spending) >= 2:
            current_spending = monthly_spending[0]
            previous_spending = monthly_spending[1]
            
            if previous_spending > 0:
                percentage_change = ((current_spending - previous_spending) / previous_spending) * 100
            else:
                percentage_change = 100 if current_spending > 0 else 0
            
            if abs(percentage_change) < 5:
                direction = "stable"
                prediction = "Spending will remain consistent"
            elif percentage_change > 0:
                direction = "increasing"
                prediction = f"Spending trending up by {percentage_change:.1f}%"
            else:
                direction = "decreasing"
                prediction = f"Spending trending down by {abs(percentage_change):.1f}%"
            
            # Calculate confidence based on data consistency
            if len(monthly_spending) >= 3:
                variance = sum((x - sum(monthly_spending)/len(monthly_spending))**2 for x in monthly_spending) / len(monthly_spending)
                avg_spending = sum(monthly_spending) / len(monthly_spending)
                confidence = 1.0 / (1.0 + variance / (avg_spending**2)) if avg_spending > 0 else 0.5
            else:
                confidence = 0.7
            
            return SpendingTrend(
                direction=direction,
                percentage_change=percentage_change,
                days_analyzed=len(monthly_spending) * 30,
                prediction=prediction,
                confidence=min(confidence, 1.0)
            )
        
        return SpendingTrend(
            direction="stable",
            percentage_change=0,
            days_analyzed=30,
            prediction="Insufficient data for trend analysis",
            confidence=0.3
        )
    
    async def _generate_budget_insights(
        self, 
        user_id: str, 
        budgets: List[Dict], 
        trends: Dict[str, SpendingTrend], 
        month: int, 
        year: int
    ) -> List[BudgetInsight]:
        """Generate actionable budget insights"""
        
        insights = []
        
        # Analyze budget allocation efficiency
        total_budget = sum(b["budget"].amount for b in budgets)
        if total_budget > 0:
            for budget_data in budgets:
                budget = budget_data["budget"]
                category = budget_data["category"]
                spent = budget_data["total_spent"]
                
                budget_percentage = (budget.amount / total_budget) * 100
                spending_percentage = (spent / sum(b["total_spent"] for b in budgets)) * 100 if sum(b["total_spent"] for b in budgets) > 0 else 0
                
                # Over-allocated categories
                if budget_percentage > spending_percentage + 15:
                    potential_savings = budget.amount - spent
                    insights.append(BudgetInsight(
                        type="over_allocation",
                        title=f"{category.name} Budget Over-Allocated",
                        description=f"You allocated {budget_percentage:.1f}% of budget but only spent {spending_percentage:.1f}%",
                        impact="positive",
                        recommendation=f"Consider reducing {category.name} budget by KSh {potential_savings * 0.3:,.0f} and reallocating",
                        potential_savings=potential_savings * 0.3,
                        category_id=budget.category_id
                    ))
                
                # Under-allocated categories
                elif spending_percentage > budget_percentage + 15:
                    insights.append(BudgetInsight(
                        type="under_allocation",
                        title=f"{category.name} Budget Under-Allocated",
                        description=f"You spent {spending_percentage:.1f}% but only allocated {budget_percentage:.1f}%",
                        impact="negative",
                        recommendation=f"Consider increasing {category.name} budget to better reflect spending patterns",
                        category_id=budget.category_id
                    ))
        
        # Trend-based insights
        for category_id, trend in trends.items():
            category_data = next((b for b in budgets if b["budget"].category_id == category_id), None)
            if not category_data:
                continue
            
            category = category_data["category"]
            
            if trend.direction == "increasing" and abs(trend.percentage_change) > 20:
                insights.append(BudgetInsight(
                    type="increasing_trend",
                    title=f"{category.name} Spending Increasing",
                    description=f"Spending increased by {trend.percentage_change:.1f}% compared to last month",
                    impact="negative",
                    recommendation="Review recent transactions and identify opportunities to reduce spending",
                    category_id=category_id
                ))
            
            elif trend.direction == "decreasing" and abs(trend.percentage_change) > 15:
                insights.append(BudgetInsight(
                    type="decreasing_trend",
                    title=f"{category.name} Spending Improving",
                    description=f"Spending decreased by {abs(trend.percentage_change):.1f}% compared to last month",
                    impact="positive",
                    recommendation="Great progress! Consider applying similar strategies to other categories",
                    category_id=category_id
                ))
        
        # Spending pattern insights
        for budget_data in budgets:
            category = budget_data["category"]
            
            # High transaction frequency
            if budget_data["spending_frequency"] > 0.7:
                insights.append(BudgetInsight(
                    type="frequent_spending",
                    title=f"Frequent {category.name} Transactions",
                    description=f"You made transactions on {budget_data['days_with_spending']} days this month",
                    impact="neutral",
                    recommendation="Consider consolidating purchases or setting specific spending days",
                    category_id=budget_data["budget"].category_id
                ))
            
            # Large transaction variance
            if budget_data["transaction_count"] > 5:
                avg_txn = budget_data["avg_transaction"]
                max_txn = budget_data["max_transaction"]
                
                if max_txn > avg_txn * 3:
                    insights.append(BudgetInsight(
                        type="transaction_variance",
                        title=f"Inconsistent {category.name} Spending",
                        description=f"Transaction amounts vary significantly (avg: KSh {avg_txn:.0f}, max: KSh {max_txn:.0f})",
                        impact="neutral",
                        recommendation="Consider budgeting separately for regular vs. large purchases",
                        category_id=budget_data["budget"].category_id
                    ))
        
        return insights
    
    async def _generate_budget_goals(
        self, 
        user_id: str, 
        budgets: List[Dict], 
        trends: Dict[str, SpendingTrend], 
        month: int, 
        year: int
    ) -> List[BudgetGoal]:
        """Generate actionable budget optimization goals"""
        
        goals = []
        
        for budget_data in budgets:
            budget = budget_data["budget"]
            category = budget_data["category"]
            spent = budget_data["total_spent"]
            
            # Goal: Reduce overspending
            if spent > budget.amount:
                overspend = spent - budget.amount
                target_reduction = overspend + (budget.amount * 0.1)  # 10% buffer
                
                goals.append(BudgetGoal(
                    type="reduce_spending",
                    category_id=budget.category_id,
                    category_name=category.name,
                    current_amount=spent,
                    target_amount=budget.amount - target_reduction,
                    timeframe="next_month",
                    difficulty="challenging" if overspend > budget.amount * 0.5 else "moderate",
                    potential_impact=f"Save KSh {target_reduction:,.2f} monthly"
                ))
            
            # Goal: Optimize under-utilized budgets
            elif spent < budget.amount * 0.6:
                potential_reallocation = (budget.amount - spent) * 0.5
                
                goals.append(BudgetGoal(
                    type="optimize_categories",
                    category_id=budget.category_id,
                    category_name=category.name,
                    current_amount=budget.amount,
                    target_amount=budget.amount - potential_reallocation,
                    timeframe="next_month",
                    difficulty="easy",
                    potential_impact=f"Reallocate KSh {potential_reallocation:,.2f} to high-need categories"
                ))
        
        # Goal: Address increasing trends
        for category_id, trend in trends.items():
            if trend.direction == "increasing" and trend.percentage_change > 25:
                category_data = next((b for b in budgets if b["budget"].category_id == category_id), None)
                if category_data:
                    current_spending = category_data["total_spent"]
                    target_reduction = current_spending * 0.15  # 15% reduction goal
                    
                    goals.append(BudgetGoal(
                        type="reduce_spending",
                        category_id=category_id,
                        category_name=category_data["category"].name,
                        current_amount=current_spending,
                        target_amount=current_spending - target_reduction,
                        timeframe="next_month",
                        difficulty="moderate",
                        potential_impact=f"Reverse increasing trend, save KSh {target_reduction:,.2f}"
                    ))
        
        return goals
    
    def _calculate_budget_health_score(self, budgets: List[Dict], alerts: List[BudgetAlert]) -> Dict:
        """Calculate overall budget health score"""
        
        if not budgets:
            return {"score": 0, "grade": "F", "description": "No budgets set"}
        
        # Base score from budget vs spending performance
        total_budget = sum(b["budget"].amount for b in budgets)
        total_spent = sum(b["total_spent"] for b in budgets)
        
        if total_budget == 0:
            performance_score = 0
        else:
            utilization = total_spent / total_budget
            if utilization <= 0.9:
                performance_score = 100 - (utilization * 50)  # Great if under 90%
            else:
                performance_score = max(0, 100 - ((utilization - 0.9) * 500))  # Penalty for overspending
        
        # Penalty for alerts
        alert_penalty = 0
        for alert in alerts:
            if alert.severity == AlertSeverity.CRITICAL:
                alert_penalty += 25
            elif alert.severity == AlertSeverity.HIGH:
                alert_penalty += 15
            elif alert.severity == AlertSeverity.MEDIUM:
                alert_penalty += 10
            else:
                alert_penalty += 5
        
        final_score = max(0, performance_score - alert_penalty)
        
        # Grade assignment
        if final_score >= 90:
            grade = "A+"
            description = "Excellent budget management"
        elif final_score >= 80:
            grade = "A"
            description = "Very good budget control"
        elif final_score >= 70:
            grade = "B"
            description = "Good budget management"
        elif final_score >= 60:
            grade = "C"
            description = "Fair budget control"
        elif final_score >= 50:
            grade = "D"
            description = "Needs improvement"
        else:
            grade = "F"
            description = "Poor budget management"
        
        return {
            "score": round(final_score),
            "grade": grade,
            "description": description
        }
    
    def _determine_overall_status(self, budgets: List[Dict], alerts: List[BudgetAlert]) -> BudgetStatus:
        """Determine overall budget status"""
        
        critical_alerts = [a for a in alerts if a.severity == AlertSeverity.CRITICAL]
        high_alerts = [a for a in alerts if a.severity == AlertSeverity.HIGH]
        
        if critical_alerts:
            return BudgetStatus.CRITICAL
        elif high_alerts:
            return BudgetStatus.DANGER
        elif len(alerts) > 3:
            return BudgetStatus.WARNING
        elif len(alerts) <= 1:
            return BudgetStatus.EXCELLENT
        else:
            return BudgetStatus.GOOD
    
    def _generate_summary(self, budgets: List[Dict], alerts: List[BudgetAlert], trends: Dict) -> Dict:
        """Generate executive summary"""
        
        total_budget = sum(b["budget"].amount for b in budgets)
        total_spent = sum(b["total_spent"] for b in budgets)
        categories_count = len(budgets)
        over_budget_count = len([b for b in budgets if b["total_spent"] > b["budget"].amount])
        
        increasing_trends = len([t for t in trends.values() if t.direction == "increasing"])
        decreasing_trends = len([t for t in trends.values() if t.direction == "decreasing"])
        
        return {
            "total_budget": total_budget,
            "total_spent": total_spent,
            "utilization_percentage": (total_spent / total_budget * 100) if total_budget > 0 else 0,
            "categories_tracked": categories_count,
            "categories_over_budget": over_budget_count,
            "total_alerts": len(alerts),
            "critical_alerts": len([a for a in alerts if a.severity == AlertSeverity.CRITICAL]),
            "increasing_trends": increasing_trends,
            "decreasing_trends": decreasing_trends,
            "stable_trends": len(trends) - increasing_trends - decreasing_trends
        }
    
    def _empty_analysis(self, month: int, year: int) -> Dict:
        """Return empty analysis structure"""
        return {
            "period": {"month": month, "year": year},
            "health_score": {"score": 0, "grade": "N/A", "description": "No budgets set"},
            "status": BudgetStatus.GOOD,
            "budgets": [],
            "alerts": [],
            "trends": {},
            "insights": [],
            "goals": [],
            "summary": {
                "total_budget": 0,
                "total_spent": 0,
                "utilization_percentage": 0,
                "categories_tracked": 0,
                "categories_over_budget": 0,
                "total_alerts": 0,
                "critical_alerts": 0,
                "increasing_trends": 0,
                "decreasing_trends": 0,
                "stable_trends": 0
            }
        }
