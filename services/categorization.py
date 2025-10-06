from typing import List, Optional
from models.user import Category

class CategorizationService:
    
    @staticmethod
    def get_default_categories() -> List[dict]:
        """Return default categories for M-Pesa transactions"""
        return [
            {
                "name": "Food & Dining",
                "icon": "restaurant",
                "color": "#FF6B6B",
                "keywords": ["restaurant", "food", "dining", "lunch", "dinner", "breakfast", "cafe", "hotel"]
            },
            {
                "name": "Transport",
                "icon": "car",
                "color": "#4ECDC4",
                "keywords": ["uber", "taxi", "matatu", "fuel", "parking", "transport", "travel", "bus"]
            },
            {
                "name": "Utilities",
                "icon": "flash",
                "color": "#45B7D1",
                "keywords": ["kplc", "electricity", "water", "internet", "safaricom", "airtel", "telkom"]
            },
            {
                "name": "Shopping",
                "icon": "shopping-bag",
                "color": "#96CEB4",
                "keywords": ["shop", "store", "market", "buy", "purchase", "retail"]
            },
            {
                "name": "Entertainment",
                "icon": "music-note",
                "color": "#FFEAA7",
                "keywords": ["movie", "cinema", "game", "entertainment", "fun", "sport"]
            },
            {
                "name": "Health",
                "icon": "medical",
                "color": "#FD79A8",
                "keywords": ["hospital", "clinic", "pharmacy", "doctor", "medical", "health"]
            },
            {
                "name": "Education",
                "icon": "school",
                "color": "#6C5CE7",
                "keywords": ["school", "education", "course", "training", "tuition", "books"]
            },
            {
                "name": "Bills & Fees",
                "icon": "receipt",
                "color": "#A29BFE",
                "keywords": ["bill", "fee", "charge", "service", "maintenance", "subscription"]
            },
            {
                "name": "Income",
                "icon": "cash",
                "color": "#00B894",
                "keywords": ["salary", "payment", "income", "received", "earnings", "refund"]
            },
            {
                "name": "Other",
                "icon": "ellipsis-horizontal",
                "color": "#636E72",
                "keywords": []
            }
        ]
    
    @staticmethod
    def auto_categorize(description: str, categories: List[Category]) -> Optional[str]:
        """Auto-categorize a transaction based on description"""
        description_lower = description.lower()
        
        # Find category with matching keywords
        for category in categories:
            for keyword in category.keywords:
                if keyword.lower() in description_lower:
                    return category.id
        
        # Default to "Other" category if no match found
        other_category = next((c for c in categories if c.name == "Other"), None)
        return other_category.id if other_category else categories[0].id if categories else None