from fastapi import APIRouter, HTTPException, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase
from models.user import User, UserCreate, UserVerify, Category
from services.categorization import CategorizationService
import bcrypt
import os

router = APIRouter(prefix="/auth", tags=["auth"])

async def get_db():
    from server import db
    return db

@router.post("/setup-pin")
async def setup_pin(user_data: UserCreate, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Setup PIN for new user and create default categories"""
    try:
        # Check if user already exists (for demo, we'll use a single user)
        existing_user = await db.users.find_one({})
        if existing_user:
            raise HTTPException(status_code=400, detail="User already exists")
        
        # Hash the PIN
        pin_hash = bcrypt.hashpw(user_data.pin.encode('utf-8'), bcrypt.gensalt())
        
        # Create user
        user = User(
            pin_hash=pin_hash.decode('utf-8'),
            preferences={"default_currency": "KES"}
        )
        
        # Insert user
        result = await db.users.insert_one(user.dict())
        user_id = str(result.inserted_id)
        
        # Create default categories
        default_categories = CategorizationService.get_default_categories()
        categories = []
        for cat_data in default_categories:
            category = Category(**cat_data)
            await db.categories.insert_one(category.dict())
            categories.append(category)
        
        return {
            "message": "PIN setup successful",
            "user_id": user_id,
            "categories": len(categories)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error setting up PIN: {str(e)}")

@router.post("/verify-pin")
async def verify_pin(verify_data: UserVerify, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Verify user PIN"""
    try:
        # Get user (for demo, get the first user)
        user_doc = await db.users.find_one({})
        if not user_doc:
            raise HTTPException(status_code=404, detail="User not found. Please setup PIN first.")
        
        # Verify PIN
        stored_hash = user_doc["pin_hash"].encode('utf-8')
        is_valid = bcrypt.checkpw(verify_data.pin.encode('utf-8'), stored_hash)
        
        if not is_valid:
            raise HTTPException(status_code=401, detail="Invalid PIN")
        
        return {
            "message": "PIN verified successfully",
            "user_id": str(user_doc["_id"])
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error verifying PIN: {str(e)}")

@router.get("/user-status")
async def get_user_status(db: AsyncIOMotorDatabase = Depends(get_db)):
    """Check if user exists and has PIN setup"""
    try:
        user_doc = await db.users.find_one({})
        categories_count = await db.categories.count_documents({})
        
        return {
            "has_user": user_doc is not None,
            "user_id": str(user_doc["_id"]) if user_doc else None,
            "categories_count": categories_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking user status: {str(e)}")