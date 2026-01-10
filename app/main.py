import asyncpg
import io
import httpx
from fastapi import FastAPI, HTTPException, File, UploadFile, Form, Body
from pydantic import BaseModel
from typing import Dict, Any, Optional
import os, json
from dotenv import load_dotenv

from app.db import fetch_users
from app.platforms.route import fn_route
from app.platforms.google.gdrive import upload_file_to_drive
from app.platforms.telegram.telegram_logics import fn_send_message
from telegram import Bot

load_dotenv()

# Telegram
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
WEBHOOK_URL = os.getenv("TELEGRAM_WEBHOOK_URL")
bot = Bot(token=BOT_TOKEN)

app = FastAPI()

@app.post("/login")
async def login(message: dict = Body(...)):
    try:
        record = await fn_route(message)
        return record
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/signup")
async def signup(message: dict = Body(...)):
    try:
        record = await fn_route(message)
        return record
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/")
async def root():
    return {"message": "FastAPI + Neon ready"}


@app.post("/rok_db")
async def fn_rok_db(input_map: dict):
    try:
        record = await fn_route(input_map)
        return record
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload-to-drive")
async def upload_to_drive(
    file: UploadFile = File(...),
    folder_id: Optional[str] = Form(None)
):
    try:
        content = await file.read()
        result = await upload_file_to_drive(
            file_content=content,
            filename=file.filename,
            mimetype=file.content_type or 'application/octet-stream',
            folder_id=folder_id
        )
        
        if result.get('success'):
            return result
        else:
            raise HTTPException(status_code=500, detail=result.get('error'))
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {str(e)}"
        )


@app.post("/telegram-callback")
async def telegram_callback(update: dict = Body(...)):
    """Handle Telegram callback queries (button clicks)"""
    try:
        callback_query = update.get("callback_query")
        if not callback_query:
            return {"status": "no callback query"}
        
        callback_data = callback_query.get("data", "")
        chat_id = callback_query.get("message", {}).get("chat", {}).get("id")
        callback_id = callback_query.get("id")
        user = callback_query.get("from", {})
        
        # Send the callback data to the webhook
        if callback_data.startswith("report_"):
            error_type = callback_data.replace("report_", "")
            webhook_payload = {
                "error_type": error_type,
                "user_id": user.get("id"),
                "username": user.get("username"),
                "first_name": user.get("first_name"),
                "chat_id": chat_id,
                "callback_data": callback_data,
                "message": callback_query.get("message", {}).get("text", "")
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(WEBHOOK_URL, json=webhook_payload)
            
            # Acknowledge the callback and send a response message
            await bot.answer_callback_query(callback_id, text="Issue reported successfully!")
            await bot.send_message(chat_id=chat_id, text="Thank you! Your issue has been reported.")
            
            return {"status": "reported", "webhook_response": response.status_code}
        
        elif callback_data.startswith("retry_"):
            await bot.answer_callback_query(callback_id, text="Please try again")
            return {"status": "retry acknowledged"}
        
        return {"status": "unknown callback"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))