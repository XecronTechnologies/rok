import os
import asyncpg
import json
from typing import Optional
from app.utils.time_handler import unix_time_handler
from app.utils.db_pool import get_pool
from app.platforms.neon.auth import fn_check_permission
from app.platforms.neon.signup import fn_signup
from app.platforms.neon.neon_db_logics import fn_add_record,fn_bulk_add_record,fn_update_record,fn_get_record


async def fn_neon_db(input_map:Optional[dict]):
    TABLE_NAME = input_map.get("parameters").get("table_name")
    SCHEMA_NAME = input_map.get("parameters").get("schema_name")
    print(f"fn_neon_db called with: {input_map}")
    pool = await get_pool()
    if(input_map.get("action_type")=="get_record"):
        try:
            return await fn_get_record(pool,input_map)
        except Exception as e:
            return {"message":str(e)}
    
    elif(input_map.get("action_type")=="add_record"):
        try:
            if isinstance(input_map.get("parameters").get("row_values"), list):
                return await fn_bulk_add_record(pool,input_map)
            else:
                return await fn_add_record(pool, input_map)
        except Exception as e:
            return {"message":str(e)}

    elif(input_map.get("action_type")=="update_record"):
        try:
            return await fn_update_record(pool,input_map)
        except Exception as e:
            return {"message":str(e)}
    
    elif(input_map.get("action_type")=="delete_record"):
        try:

            record_id = input_map.get("parameters").get("record_id")
            query = f"DELETE FROM {TABLE_NAME} WHERE record_id='{record_id}' RETURNING id"
            async with pool.acquire() as conn:
                row = await conn.fetchrow(query)
                if row:
                    return dict(row)
                else:
                    return {"message": "Record not found"}
        except Exception as e:
            return {"message":str(e)}
            
    elif(input_map.get("action_type")=="check_permission"):
        return await fn_check_permission(pool, input_map)
    elif(input_map.get("action_type")=="signup"):
        # return {"a":1}
        input_map["parameters"]["schema_name"] = "client_organizations"
        input_map["parameters"]["table_name"] = "user_organization"
        return await fn_signup(pool, input_map)
    else:
        return {"message":"try with valid action type"}