from typing import List, Optional, Dict, Any
from motor.motor_asyncio import AsyncIOMotorDatabase
from datetime import datetime, timedelta
from models.transaction import Transaction
import hashlib

class DuplicateDetector:
    """
    Service to detect and prevent duplicate M-Pesa transactions from SMS parsing
    """
    
    @staticmethod
    async def is_duplicate_by_hash(db: AsyncIOMotorDatabase, message_hash: str) -> bool:
        """
        Check if a transaction with the same message hash already exists
        """
        existing = await db.transactions.find_one({
            "sms_metadata.original_message_hash": message_hash
        })
        return existing is not None
    
    @staticmethod
    async def is_duplicate_by_transaction_id(db: AsyncIOMotorDatabase, transaction_id: str) -> bool:
        """
        Check if a transaction with the same M-Pesa transaction ID already exists
        """
        if not transaction_id:
            return False
            
        existing = await db.transactions.find_one({
            "mpesa_details.transaction_id": transaction_id
        })
        return existing is not None
    
    @staticmethod
    async def find_similar_transactions(
        db: AsyncIOMotorDatabase, 
        amount: float, 
        user_id: str,
        time_window_hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Find transactions with similar amount within a time window
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=time_window_hours)
        
        # Find transactions with same amount (+/- 1 KSh for rounding)
        similar_transactions = await db.transactions.find({
            "user_id": user_id,
            "amount": {"$gte": amount - 1, "$lte": amount + 1},
            "created_at": {"$gte": cutoff_time}
        }).to_list(50)
        
        return similar_transactions
    
    @staticmethod
    async def check_comprehensive_duplicate(
        db: AsyncIOMotorDatabase,
        user_id: str,
        amount: float,
        transaction_id: Optional[str] = None,
        message_hash: Optional[str] = None,
        recipient: Optional[str] = None,
        time_window_hours: int = 24
    ) -> Dict[str, Any]:
        """
        Comprehensive duplicate check using multiple criteria
        """
        duplicate_reasons = []
        confidence = 0.0
        
        # Check by message hash (highest confidence)
        if message_hash and await DuplicateDetector.is_duplicate_by_hash(db, message_hash):
            duplicate_reasons.append("exact_message_match")
            confidence = 1.0
        
        # Check by transaction ID (high confidence)
        if transaction_id and await DuplicateDetector.is_duplicate_by_transaction_id(db, transaction_id):
            duplicate_reasons.append("transaction_id_match")
            confidence = max(confidence, 0.9)
        
        # Check for similar transactions (lower confidence)
        similar_transactions = await DuplicateDetector.find_similar_transactions(
            db, amount, user_id, time_window_hours
        )
        
        if similar_transactions:
            # Check for exact amount and recipient match
            for transaction in similar_transactions:
                if (transaction.get("amount") == amount and 
                    transaction.get("mpesa_details", {}).get("recipient") == recipient):
                    duplicate_reasons.append("amount_recipient_match")
                    confidence = max(confidence, 0.7)
                    break
            
            # If no exact match, but similar amounts exist
            if not duplicate_reasons and len(similar_transactions) > 0:
                duplicate_reasons.append("similar_amount_recent")
                confidence = max(confidence, 0.3)
        
        return {
            "is_duplicate": len(duplicate_reasons) > 0 and confidence >= 0.7,
            "confidence": confidence,
            "reasons": duplicate_reasons,
            "similar_transactions": similar_transactions[:5]  # Return top 5 for review
        }
    
    @staticmethod
    def calculate_similarity_score(transaction1: Dict[str, Any], transaction2: Dict[str, Any]) -> float:
        """
        Calculate similarity score between two transactions (0.0 - 1.0)
        """
        score = 0.0
        
        # Amount similarity (40% weight)
        amount1 = transaction1.get("amount", 0)
        amount2 = transaction2.get("amount", 0)
        if amount1 > 0 and amount2 > 0:
            amount_diff = abs(amount1 - amount2)
            amount_similarity = max(0, 1 - (amount_diff / max(amount1, amount2)))
            score += amount_similarity * 0.4
        
        # Recipient similarity (30% weight)
        recipient1 = transaction1.get("mpesa_details", {}).get("recipient", "")
        recipient2 = transaction2.get("mpesa_details", {}).get("recipient", "")
        if recipient1 and recipient2:
            recipient_similarity = DuplicateDetector._string_similarity(recipient1, recipient2)
            score += recipient_similarity * 0.3
        
        # Time proximity (20% weight)
        time1 = transaction1.get("created_at")
        time2 = transaction2.get("created_at")
        if time1 and time2:
            time_diff = abs((time1 - time2).total_seconds())
            time_similarity = max(0, 1 - (time_diff / (24 * 3600)))  # 24 hour window
            score += time_similarity * 0.2
        
        # Transaction ID similarity (10% weight)
        txn_id1 = transaction1.get("mpesa_details", {}).get("transaction_id", "")
        txn_id2 = transaction2.get("mpesa_details", {}).get("transaction_id", "")
        if txn_id1 and txn_id2:
            id_similarity = 1.0 if txn_id1 == txn_id2 else 0.0
            score += id_similarity * 0.1
        
        return score
    
    @staticmethod
    def _string_similarity(str1: str, str2: str) -> float:
        """
        Calculate string similarity using simple character overlap
        """
        if not str1 or not str2:
            return 0.0
        
        str1 = str1.lower().strip()
        str2 = str2.lower().strip()
        
        if str1 == str2:
            return 1.0
        
        # Simple Jaccard similarity
        set1 = set(str1.split())
        set2 = set(str2.split())
        
        if not set1 or not set2:
            return 0.0
        
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        return intersection / union if union > 0 else 0.0
    
    @staticmethod
    async def log_duplicate_attempt(
        db: AsyncIOMotorDatabase,
        user_id: str,
        message_hash: str,
        duplicate_info: Dict[str, Any]
    ):
        """
        Log duplicate detection attempt for analysis
        """
        log_entry = {
            "user_id": user_id,
            "message_hash": message_hash,
            "duplicate_confidence": duplicate_info["confidence"],
            "duplicate_reasons": duplicate_info["reasons"],
            "detected_at": datetime.utcnow(),
            "action_taken": "blocked" if duplicate_info["is_duplicate"] else "allowed"
        }
        
        await db.duplicate_logs.insert_one(log_entry)
    
    @staticmethod
    async def get_duplicate_statistics(
        db: AsyncIOMotorDatabase,
        user_id: str,
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Get duplicate detection statistics for a user
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Count duplicates blocked
        duplicates_blocked = await db.duplicate_logs.count_documents({
            "user_id": user_id,
            "detected_at": {"$gte": cutoff_date},
            "action_taken": "blocked"
        })
        
        # Count SMS transactions processed
        sms_transactions = await db.transactions.count_documents({
            "user_id": user_id,
            "source": "sms",
            "created_at": {"$gte": cutoff_date}
        })
        
        # Get common duplicate reasons
        pipeline = [
            {
                "$match": {
                    "user_id": user_id,
                    "detected_at": {"$gte": cutoff_date},
                    "action_taken": "blocked"
                }
            },
            {"$unwind": "$duplicate_reasons"},
            {"$group": {"_id": "$duplicate_reasons", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        duplicate_reasons = await db.duplicate_logs.aggregate(pipeline).to_list(10)
        
        return {
            "duplicates_blocked": duplicates_blocked,
            "sms_transactions_processed": sms_transactions,
            "duplicate_rate": duplicates_blocked / max(sms_transactions + duplicates_blocked, 1),
            "common_duplicate_reasons": duplicate_reasons
        }
