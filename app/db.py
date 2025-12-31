import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

pool: asyncpg.pool.Pool | None = None

async def init_db() -> None:
    global pool
    if pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is not set in environment")
        pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE
            );
            """
        )

async def fetch_users():
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM users ORDER BY id")
        return [dict(r) for r in rows]

async def create_user(name: str, email: str):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name, email",
            name,
            email,
        )
        return dict(row)