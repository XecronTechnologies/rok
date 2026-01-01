import os
import asyncpg
import json
from dotenv import load_dotenv, find_dotenv
from typing import Optional
from urllib.parse import urlparse, parse_qsl
from app.utils.time_handler import unix_time_handler
from app.platforms.neon.permission_master import fn_check_permission

load_dotenv(find_dotenv())
DATABASE_URL = "postgresql://neondb_owner:npg_cd4xtiJEswK6@ep-late-dawn-a1otplkv-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

parsed = urlparse(DATABASE_URL)

pool: Optional[asyncpg.pool.Pool] = None
async def initialize_pool():
    global pool
    if pool is None:
        try:
            print(f"Connecting to: {parsed.hostname}")
            pool = await asyncpg.create_pool(
                host=parsed.hostname,
                port=parsed.port or 5432,
                user=parsed.username,
                password=parsed.password,
                database=parsed.path.lstrip('/'),
                ssl='require',
                timeout=60,
                command_timeout=60
            )
            print(f"Pool created successfully: {pool}")
        except Exception as e:
            print(f"Pool creation failed: {type(e).__name__}: {repr(e)}")
            raise e
    else:
        print("Pool already exists, reusing...")

async def fn_neon_db(input_map:Optional[dict]):
    TABLE_NAME = input_map.get("parameters").get("table_name")
    print(f"fn_neon_db called with: {input_map}")
    await initialize_pool()
    if(input_map.get("action_type")=="get_record"):
        try:
            record_id = input_map.get("parameters").get("record_id")
            query = f"SELECT * FROM {TABLE_NAME} WHERE record_id='{record_id}'"
            async with pool.acquire() as conn:
                rows = await conn.fetch(query)
                temp = []
                for row in rows:
                    temp.append(dict(row))
                return temp
        except Exception as e:
            return {"message":str(e)}
    
    elif(input_map.get("action_type")=="add_record"):
        try:
            print(f"input_map: ",input_map)
            column_names = input_map.get("parameters").get("column_names")  
            column_names.append("record_id")
            row_values = input_map.get("parameters").get("row_values") 
            record_id = unix_time_handler()
            ROW_VALUES = ""
            COLUMN_NAMES = ",".join(column_names)
            print(f"COLUMN_NAMES: ",COLUMN_NAMES)
            row_values["record_id"] = record_id
            ROW_VALUES = ",".join([f"'{row_values[col]}'" for col in column_names])
            print(f"ROW_VALUES: ",ROW_VALUES)
            query = f"INSERT INTO {TABLE_NAME}({COLUMN_NAMES}) VALUES ({ROW_VALUES}) RETURNING id,{COLUMN_NAMES}"
            async with pool.acquire() as conn:
                row = await conn.fetchrow(query)
                return dict(row)
        except Exception as e:
            return {"message":str(e)}

    elif(input_map.get("action_type")=="update_record"):
        try:
            column_names = input_map.get("parameters").get("column_names")  
            row_values = input_map.get("parameters").get("row_values") 
            record_id = input_map.get("parameters").get("record_id")
            COLUMN_NAMES = ",".join(column_names)
            
            SET_VALUES = ""                        
            for col in column_names:               
                value = row_values[col]            
                SET_VALUES += f"{col}='{value}',"  
            SET_VALUES = SET_VALUES[:-1]       

            query = f"UPDATE {TABLE_NAME} SET {SET_VALUES} WHERE id={record_id} RETURNING id,{COLUMN_NAMES}"
            async with pool.acquire() as conn:
                row = await conn.fetchrow(query)
                return dict(row)
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
        try:
            module_verified = {}
            print("check permission triggered")
            table_name = input_map.get("parameters").get("table_name")
            module_verified["table_name"] = table_name
            query = f"SELECT role_id FROM user_master WHERE email='{input_map.get('auth_parameters').get('email')}'"
            async with pool.acquire() as conn:
                rows = await conn.fetch(query)
                print(rows)
                if rows:
                    role_module_id = [dict(row) for row in rows]
                    module_verified["role_id"] = role_module_id[0]["role_id"]
                    module_verified["status"] = "success"
                else:
                    module_verified["status"] = "failure"
                    module_verified["message"]= f"Record not found at {table_name}"
                    return module_verified
                    
            if(module_verified["status"]=="success" and module_verified["role_id"]):
                module_verified["table_name"] = "role_master"
                query = f"SELECT record_id FROM role_master WHERE record_id='{module_verified['role_id']}'"
                print("query1",query)
                async with pool.acquire() as conn:
                    rows = await conn.fetch(query)
                    if rows:
                        role_module_id = [dict(row) for row in rows]
                        module_verified["role_id"] = role_module_id[0]["record_id"]
                        module_verified["status"] = "success"
                        print("module_verified_role_master",module_verified)
                    else:
                        module_verified["status"] = "failure"
                        module_verified["message"]= f"Record not found at role_master"
                        return module_verified
            if(module_verified["status"]=="success" and module_verified["role_id"]):
                module_verified["table_name"]="permission_master"
                query = f"SELECT record_id FROM permission_master WHERE role_id='{module_verified['role_id']}'"
                print("query2",query)
                async with pool.acquire() as conn:
                    rows = await conn.fetch(query)
                    if rows:
                        role_module_id = [dict(row) for row in rows]
                        module_verified["role_id"] = role_module_id[0]["record_id"]
                        module_verified["status"] = "success"
                    else:
                        module_verified["status"] = "failure"
                        module_verified["message"]= f"Record not found at permission_master"
                        return module_verified
            if(module_verified["status"]=="success" and module_verified["role_id"]):
                module_verified["table_name"]="output_table"
                query = f"SELECT * FROM {input_map.get("parameters").get("table_name")} WHERE permission_id='{module_verified['role_id']}'"
                print("query2",query)
                async with pool.acquire() as conn:
                    rows = await conn.fetch(query)
                    if rows:
                        role_module_id = [dict(row) for row in rows]
                        module_verified["output_map"] = role_module_id[0]
                        module_verified["status"] = "success"
                        return module_verified
                    else:
                        module_verified["status"] = "failure"
                        module_verified["message"]= f"Record not found at output_table"
                        return module_verified
        except Exception as e:
            return {"message":str(e)}
    else:
        return {"message":"try with valid action type"}