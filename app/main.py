import asyncpg
from fastapi import FastAPI, HTTPException
from typing import Dict, Any, Optional
import os,json
import firebase_admin
from firebase_admin import credentials, firestore

from app.db import  fetch_users
from app.platforms.route import fn_route

app = FastAPI()

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

@app.post("/rok_db")
async def fn_rok_db(input_map: dict):
    try:
        record = await fn_route(input_map)
        return record
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get_access_token")
async def get_access_token(input_map: Optional[dict]):
    try:
        record = await fn_route(input_map)
        return record
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
        
        user = await fn_add_record({"name":name,"email":email})
        return user
    except asyncpg.exceptions.UniqueViolationError:
        raise HTTPException(status_code=400, detail="Email already exists")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/firebase/{collection_name}/{document_id}")
async def get_document(collection_name: str, document_id: str):
    cred = credentials.Certificate(os.path.join(os.path.dirname(__file__), "service_account.json"))
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    if not db:
        raise HTTPException(status_code=503, detail="Firebase not configured or initialized")
    try:
        doc_ref = db.collection(collection_name).document(document_id)
        doc = doc_ref.get()
        temp =  json.loads(json.dumps({"as":doc.to_dict()}))
        temp["id"] = doc.to_dict().get("id")
        return temp

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
6