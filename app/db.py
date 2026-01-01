import os
import asyncpg
import json
from dotenv import load_dotenv
from typing import Optional

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

pool: Optional[asyncpg.pool.Pool] = None

async def fetch_users():
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM users ORDER BY id")
        # print(f"rows : {rows}")
        temp = []
        for row in rows:
            temp.append(dict(row))
        print(f"temp: {temp}")
        return [dict(r) for r in rows]

async def add_record(input_map:dict):
    
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name, email",
            name,
            email,
        )
        return dict(row)