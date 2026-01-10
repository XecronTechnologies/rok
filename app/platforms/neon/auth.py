from typing import Optional

async def fn_get_db_name(pool,input_map:Optional[dict]):
    try:
        PLATFORM = input_map.get("platform")

        if(PLATFORM):
            query = f"SELECT db_name FROM db_schema.db_id WHERE record_id='{record_id}'"

            async with pool.acquire() as conn:
                rows = await conn.fetch(query)
                temp = []
                for row in rows:
                    temp.append(dict(row))
                print(f"TEMP FROM GET DB NAME {temp}")
                return temp
        else:
            return {"status":"failure","message":"platform not defined"}
    except Exception as e:
        return {"message":str(e)}

async def fn_check_permission(pool, input_map: Optional[dict]):
    try:
        module_verified = {}
        print("check permission triggered")
        target_table = input_map.get("parameters").get("table_name")
        email = input_map.get('auth_parameters').get('email')
        
        async def fetch_one(query, table_name, key):
            async with pool.acquire() as conn:
                rows = await conn.fetch(query)
                print(f"Query: {query}")
                if rows:
                    return dict(rows[0]).get(key), None
                return None, f"Record not found at {table_name}"
        
        role_id, error = await fetch_one(
            f"SELECT role_id FROM user_master WHERE email='{email}'",
            "user_master", "role_id"
        )
        if error:
            return {"status": "failure", "message": error, "table_name": "user_master"}
        
        role_id, error = await fetch_one(
            f"SELECT record_id FROM role_master WHERE record_id='{role_id}'",
            "role_master", "record_id"
        )
        if error:
            return {"status": "failure", "message": error, "table_name": "role_master"}
        
        permission_id, error = await fetch_one(
            f"SELECT record_id FROM permission_master WHERE role_id='{role_id}'",
            "permission_master", "record_id"
        )
        if error:
            return {"status": "failure", "message": error, "table_name": "permission_master"}
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {target_table} WHERE permission_id='{permission_id}'"
            )
            if rows:
                return {
                    "status": "success",
                    "table_name": "output_table",
                    "role_id": permission_id,
                    "output_map": dict(rows[0])
                }
            return {"status": "failure", "message": "Record not found at output_table", "table_name": "output_table"}
                    
    except Exception as e:
        return {"message": str(e)}

