from fastapi import APIRouter, HTTPException, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase
from models.user import Category, CategoryCreate
from typing import List
from bson import ObjectId

router = APIRouter(prefix="/categories", tags=["categories"])

async def get_db():
    from server import db
    return db

@router.get("/", response_model=List[Category])
async def get_categories(db: AsyncIOMotorDatabase = Depends(get_db)):
    """Get all categories"""
    try:
        categories_docs = await db.categories.find().to_list(100)
        categories = []
        for doc in categories_docs:
            doc["id"] = str(doc["_id"])
            del doc["_id"]
            categories.append(Category(**doc))
        return categories
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching categories: {str(e)}")

@router.post("/", response_model=Category)
async def create_category(category_data: CategoryCreate, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Create a new category"""
    try:
        category = Category(**category_data.dict(), is_default=False)
        result = await db.categories.insert_one(category.dict())
        category.id = str(result.inserted_id)
        return category
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating category: {str(e)}")

@router.delete("/{category_id}")
async def delete_category(category_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Delete a category (only non-default categories)"""
    try:
        # Check if category exists and is not default
        category_doc = await db.categories.find_one({"_id": ObjectId(category_id) if ObjectId.is_valid(category_id) else category_id})
        if not category_doc:
            raise HTTPException(status_code=404, detail="Category not found")
        
        if category_doc.get("is_default", True):
            raise HTTPException(status_code=400, detail="Cannot delete default category")
        
        # Check if category is being used by transactions
        transaction_count = await db.transactions.count_documents({"category_id": category_id})
        if transaction_count > 0:
            raise HTTPException(status_code=400, detail="Cannot delete category with existing transactions")
        
        # Delete category
        result = await db.categories.delete_one({"_id": ObjectId(category_id) if ObjectId.is_valid(category_id) else category_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Category not found")
        
        return {"message": "Category deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting category: {str(e)}")
