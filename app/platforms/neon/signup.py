from typing import Optional
from app.utils.time_handler import unix_time_handler
from app.platforms.neon.neon_db_logics import fn_add_record, fn_bulk_add_record
async def fn_signup(pool, input_map: Optional[dict]):
    try:
        print(f"input map : {input_map}")
        return await fn_add_record(pool, input_map)
        # return {"status": "failure", "message": "Record not found at user_master", "table_name": "user_master"}
    except Exception as e:
        return {"message": str(e)}
