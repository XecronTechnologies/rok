import os
from dotenv import load_dotenv
from telegram import Bot
from pydantic import BaseModel
from app.utils.db_pool import get_pool
from app.platforms.neon.neon_db_logics import fn_get_record

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
bot = Bot(token=BOT_TOKEN)

# class Message(BaseModel):
#     text: str
#     chat_id: str = "5863640313"


async def fn_send_message(message):
    """Send message to Telegram, splitting if too long"""
    text = message["text"] or "a"
    max_length = 4000
    
    chunks = [text[i:i+max_length] for i in range(0, len(text), max_length)]
    
    for chunk in chunks:
        await bot.send_message(chat_id=message.get("chat_id"), text=chunk)
        
    return {"status": "sent", "chunks": len(chunks)}


