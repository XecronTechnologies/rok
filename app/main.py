import asyncpg
from fastapi import FastAPI, HTTPException
from typing import Dict, Any

from app.db import init_db, fetch_users, create_user

app = FastAPI(title="FastAPI + Neon")

@app.on_event("startup")
async def startup_event():
    await init_db()

@app.get("/")
async def root():
    return {"message": "FastAPI + Neon ready"}

@app.get("/users")
async def list_users():
    try:
        users = await fetch_users()
        return users
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/users")
async def add_user(user_data: Dict[str, Any]):
    try:
        # Basic validation
        if not user_data.get("name") or not user_data.get("email"):
            raise HTTPException(status_code=400, detail="Name and email are required")
        
        name = user_data["name"]
        email = user_data["email"]
        
        user = await create_user(name, email)
        return user
    except asyncpg.exceptions.UniqueViolationError:
        raise HTTPException(status_code=400, detail="Email already exists")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))