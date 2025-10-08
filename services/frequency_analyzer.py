from typing import List, Dict, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from models.transaction import Transaction
from models.user import Category
from datetime import datetime, timedelta
import re
from collections import defaultdict
from dataclasses import dataclass

@dataclass
class FrequentTransaction:
    """Represents a frequently occurring transaction pattern"""
    pattern: str
    description_samples: List[str]
    count: int
    total_amount: float
    avg_amount: float
    category_id: Optional[str]
    category_name: Optional[str]
    first_seen: datetime
    last_seen: datetime
    transaction_ids: List[str]
    confidence_score: float
    suggested_category: Optional[str]

class TransactionFrequencyAnalyzer:
    """Analyzes transaction patterns to identify frequently occurring transactions"""
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        
    async def analyze_frequent_transactions(
        self, 
        user_id: str, 
        min_frequency: int = 3,
        days_back: int = 90
    ) -> List[FrequentTransaction]:
        """
        Analyze transactions to find patterns that occur frequently
        
        Args:
            user_id: User ID to analyze
            min_frequency: Minimum number of occurrences to consider frequent
            days_back: Number of days to look back for analysis
            
        Returns:
            List of frequent transaction patterns
        """
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Get transactions for analysis
        transactions_docs = await self.db.transactions.find({
            "user_id": user_id,
            "date": {"$gte": start_date, "$lte": end_date}
        }).to_list(1000)
        
        if not transactions_docs:
            return []
        
        # Convert to Transaction objects
        transactions = []
        for doc in transactions_docs:
            doc["id"] = str(doc["_id"])
            del doc["_id"]
            transactions.append(Transaction(**doc))
        
        # Group transactions by similarity patterns
        patterns = self._group_by_similarity(transactions)
        
        # Filter patterns by frequency and analyze
        frequent_patterns = []
        for pattern, txn_group in patterns.items():
            if len(txn_group) >= min_frequency:
                frequent_txn = await self._analyze_pattern(pattern, txn_group)
                if frequent_txn:
                    frequent_patterns.append(frequent_txn)
        
        # Sort by frequency and confidence
        frequent_patterns.sort(key=lambda x: (x.count, x.confidence_score), reverse=True)
        
        return frequent_patterns
    
    def _group_by_similarity(self, transactions: List[Transaction]) -> Dict[str, List[Transaction]]:
        """Group transactions by similarity patterns"""
        patterns = defaultdict(list)
        
        for transaction in transactions:
            # Create pattern from description
            pattern = self._extract_pattern(transaction.description)
            patterns[pattern].append(transaction)
        
        return dict(patterns)
    
    def _extract_pattern(self, description: str) -> str:
        """Extract a pattern from transaction description for grouping"""
        # Clean and normalize description
        desc = description.lower().strip()
        
        # Remove transaction-specific details (amounts, dates, reference numbers)
        # Remove common M-Pesa reference patterns
        desc = re.sub(r'\b[a-z]{2}\d{8}[a-z]{2}\b', '[REF]', desc)  # M-Pesa refs like NL12345678MN
        desc = re.sub(r'\b\d{10,12}\b', '[REF]', desc)  # Long numbers
        desc = re.sub(r'\bksh\.?\s*\d+(?:,\d{3})*(?:\.\d{2})?\b', '[AMOUNT]', desc)  # Amounts
        desc = re.sub(r'\b\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}\b', '[DATE]', desc)  # Dates
        desc = re.sub(r'\b\d{1,2}:\d{2}\b', '[TIME]', desc)  # Times
        desc = re.sub(r'\b0[17]\d{8}\b', '[PHONE]', desc)  # Phone numbers
        
        # Remove extra whitespace
        desc = re.sub(r'\s+', ' ', desc).strip()
        
        # Create semantic patterns for common transaction types
        if any(word in desc for word in ['paybill', 'till', 'buy goods']):
            # Extract merchant info but normalize transaction details
            desc = re.sub(r'\b(paybill|till)\s+\d+\b', lambda m: f'{m.group(1)} [NUMBER]', desc)
        
        if 'sent to' in desc or 'received from' in desc:
            # Normalize person-to-person transfers
            desc = re.sub(r'(sent to|received from)\s+[^\s]+', r'\1 [PERSON]', desc)
        
        return desc
    
    async def _analyze_pattern(self, pattern: str, transactions: List[Transaction]) -> Optional[FrequentTransaction]:
        """Analyze a group of similar transactions"""
        if not transactions:
            return None
        
        # Calculate metrics
        count = len(transactions)
        total_amount = sum(t.amount for t in transactions)
        avg_amount = total_amount / count
        
        # Get date range
        dates = [t.date for t in transactions]
        first_seen = min(dates)
        last_seen = max(dates)
        
        # Collect sample descriptions
        description_samples = list(set(t.description for t in transactions))[:5]
        
        # Get transaction IDs
        transaction_ids = [t.id for t in transactions]
        
        # Analyze category distribution
        categories = [t.category_id for t in transactions]
        category_counts = defaultdict(int)
        for cat in categories:
            category_counts[cat] += 1
        
        # Find most common category
        most_common_category = max(category_counts.items(), key=lambda x: x[1]) if category_counts else (None, 0)
        category_id = most_common_category[0] if most_common_category[1] > count * 0.5 else None
        
        # Get category name
        category_name = None
        if category_id:
            category_doc = await self.db.categories.find_one({"_id": category_id})
            if category_doc:
                category_name = category_doc["name"]
        
        # Calculate confidence score
        confidence_score = self._calculate_confidence(transactions, pattern)
        
        # Suggest category if not consistently categorized
        suggested_category = await self._suggest_category(pattern, transactions) if not category_id else None
        
        return FrequentTransaction(
            pattern=pattern,
            description_samples=description_samples,
            count=count,
            total_amount=total_amount,
            avg_amount=avg_amount,
            category_id=category_id,
            category_name=category_name,
            first_seen=first_seen,
            last_seen=last_seen,
            transaction_ids=transaction_ids,
            confidence_score=confidence_score,
            suggested_category=suggested_category
        )
    
    def _calculate_confidence(self, transactions: List[Transaction], pattern: str) -> float:
        """Calculate confidence score for the pattern"""
        base_score = min(len(transactions) / 10.0, 1.0)  # More transactions = higher confidence
        
        # Check amount consistency
        amounts = [t.amount for t in transactions]
        if amounts:
            avg_amount = sum(amounts) / len(amounts)
            amount_variance = sum((a - avg_amount) ** 2 for a in amounts) / len(amounts)
            amount_consistency = 1.0 / (1.0 + amount_variance / (avg_amount ** 2)) if avg_amount > 0 else 0.5
        else:
            amount_consistency = 0.5
        
        # Check date regularity
        if len(transactions) >= 3:
            dates = sorted([t.date for t in transactions])
            intervals = [(dates[i+1] - dates[i]).days for i in range(len(dates)-1)]
            avg_interval = sum(intervals) / len(intervals) if intervals else 30
            interval_variance = sum((i - avg_interval) ** 2 for i in intervals) / len(intervals) if intervals else 0
            date_regularity = 1.0 / (1.0 + interval_variance / (avg_interval ** 2)) if avg_interval > 0 else 0.5
        else:
            date_regularity = 0.5
        
        # Combine scores
        confidence = (base_score * 0.4 + amount_consistency * 0.3 + date_regularity * 0.3)
        return min(confidence, 1.0)
    
    async def _suggest_category(self, pattern: str, transactions: List[Transaction]) -> Optional[str]:
        """Suggest a category for the transaction pattern"""
        # Get all categories
        categories_docs = await self.db.categories.find().to_list(100)
        categories = [Category(**{**doc, "id": str(doc["_id"])}) for doc in categories_docs]
        
        # Use the categorization service logic
        from services.categorization import CategorizationService
        
        # Try to categorize based on the pattern
        suggested_category_id = CategorizationService.auto_categorize(pattern, categories)
        
        if suggested_category_id:
            category_doc = await self.db.categories.find_one({"_id": suggested_category_id})
            if category_doc:
                return category_doc["name"]
        
        return None
    
    async def get_uncategorized_frequent_transactions(
        self, 
        user_id: str, 
        min_frequency: int = 3
    ) -> List[FrequentTransaction]:
        """Get frequent transactions that need categorization"""
        frequent_transactions = await self.analyze_frequent_transactions(user_id, min_frequency)
        
        # Filter for transactions that need attention
        uncategorized = []
        for ft in frequent_transactions:
            # Include if no consistent category or if categorized as "Other"
            if (not ft.category_id or 
                ft.category_name == "Other" or 
                ft.confidence_score < 0.7):
                uncategorized.append(ft)
        
        return uncategorized
    
    async def apply_category_to_pattern(
        self, 
        user_id: str, 
        pattern: str, 
        category_id: str,
        transaction_ids: List[str]
    ) -> int:
        """Apply a category to all transactions matching a pattern"""
        # Update all transactions with the new category
        result = await self.db.transactions.update_many(
            {
                "_id": {"$in": transaction_ids},
                "user_id": user_id
            },
            {
                "$set": {
                    "category_id": category_id,
                    "updated_at": datetime.utcnow()
                }
            }
        )
        
        return result.modified_count
    
    async def mark_pattern_as_reviewed(
        self, 
        user_id: str, 
        pattern: str,
        transaction_ids: List[str]
    ) -> int:
        """Mark a pattern as reviewed to avoid future prompts"""
        # Add metadata to mark as reviewed
        result = await self.db.transactions.update_many(
            {
                "_id": {"$in": transaction_ids},
                "user_id": user_id
            },
            {
                "$set": {
                    "pattern_reviewed": True,
                    "pattern_reviewed_at": datetime.utcnow()
                }
            }
        )
        
        return result.modified_count
