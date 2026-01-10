import asyncpg
from dotenv import load_dotenv, find_dotenv
from typing import Optional
from urllib.parse import urlparse
import os

load_dotenv(find_dotenv())

# Database URL - can be set via environment variable or default
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_cd4xtiJEswK6@ep-late-dawn-a1otplkv-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
)

parsed = urlparse(DATABASE_URL)

# Global pool instance
_pool: Optional[asyncpg.pool.Pool] = None


async def get_pool() -> asyncpg.pool.Pool:
    """
    Get or create the database connection pool.
    This is a singleton pattern - only one pool is created and reused.
    Can be called from any API.
    """
    global _pool
    if _pool is None:
        try:
            print(f"Connecting to: {parsed.hostname}")
            _pool = await asyncpg.create_pool(
                host=parsed.hostname,
                port=parsed.port or 5432,
                user=parsed.username,
                password=parsed.password,
                database=parsed.path.lstrip('/'),
                ssl='require',
                timeout=60,
                command_timeout=60
            )
            print(f"Pool created successfully: {_pool}")
        except Exception as e:
            print(f"Pool creation failed: {type(e).__name__}: {repr(e)}")
            raise e
    else:
        print("Pool already exists, reusing...")
    return _pool


async def close_pool():
    """
    Close the database connection pool.
    Call this during application shutdown.
    """
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        print("Pool closed successfully")
