from typing import Optional
from app.utils.time_handler import unix_time_handler


# get record
async def fn_get_record(pool,input_map:Optional[dict]):
    try:
        TABLE_NAME = input_map.get("parameters").get("table_name").strip()
        SCHEMA_NAME = input_map.get("parameters").get("schema_name").strip()
        record_id = input_map.get("parameters").get("record_id")
        query = ""
        if(record_id):
            query = f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME} WHERE record_id='{record_id}'"
        else:
            query = f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME}"

        async with pool.acquire() as conn:
            rows = await conn.fetch(query)
            temp = []
            for row in rows:
                temp.append(dict(row))
            return temp
    except Exception as e:
        return {"message":str(e)}

# get_filtered_record
async def fn_filter_record(pool,input_map:Optional[dict]):
    try:
        TABLE_NAME = input_map.get("parameters").get("table_name").strip()
        SCHEMA_NAME = input_map.get("parameters").get("schema_name").strip()
        filter_query = input_map.get("parameters").get("filter_query")
        columns = input_map.get("parameters").get("column_names")
        column_var = ""
        for col in columns:
            column_var += f"{col},"
        column_var = column_var[:-1]
        query = ""
        if filter_query:
            query = f"SELECT {column_var} FROM {SCHEMA_NAME}.{TABLE_NAME} WHERE {filter_query}"
            async with pool.acquire() as conn:
                rows = await conn.fetch(query)
                if rows:
                    return [dict(row) for row in rows]
                return []
        else:
            print("No filter query provided")
            return []

    except Exception as e:
        print(f"Error in fn_filter_record: {str(e)}")
        return []

# add record
async def fn_add_record(pool, input_map: Optional[dict]):
    try:
        print(f"input_map: ",input_map)
        column_names = input_map.get("parameters").get("column_names")  
        column_names.append("record_id")
        print(f"column_names: ",column_names)
        SCHEMA_NAME = input_map.get("parameters").get("schema_name").strip()
        TABLE_NAME = input_map.get("parameters").get("table_name").strip()
        row_values = input_map.get("parameters").get("row_values") 
        record_id = unix_time_handler()
        ROW_VALUES = ""
        COLUMN_NAMES = ",".join(column_names)
        print(f"COLUMN_NAMES: ",COLUMN_NAMES)
        row_values["record_id"] = record_id
        ROW_VALUES = ",".join([f"'{row_values[col]}'" for col in column_names])
        print(f"ROW_VALUES: ",ROW_VALUES)
        query = f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}({COLUMN_NAMES}) VALUES ({ROW_VALUES}) RETURNING id,{COLUMN_NAMES}"
        print(f"query: ",query)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query)
            return {"status":"success","data":dict(row)}
    except Exception as e:
        print(f"Error in fn_add_record: {str(e)}")
        return {"status": "error", "message": str(e)}

# Bulk add records
async def fn_bulk_add_record(pool, input_map: Optional[dict]):
    try:
        print(f"input_map: ", input_map)
        column_names = input_map.get("parameters").get("column_names").copy()  # Use copy to avoid mutating original
        column_names.append("record_id")
        print(f"column_names: ", column_names)
        SCHEMA_NAME = input_map.get("parameters").get("schema_name").strip()
        TABLE_NAME = input_map.get("parameters").get("table_name").strip()
        rows_list = input_map.get("parameters").get("row_values")
        
        COLUMN_NAMES = ",".join(column_names)
        print(f"COLUMN_NAMES: ", COLUMN_NAMES)

        all_values = []
        for row in rows_list:
            record_id = unix_time_handler() 
            row["record_id"] = record_id
            row_tuple = ",".join([f"'{row[col]}'" for col in column_names])
            all_values.append(f"({row_tuple})")
        
        VALUES_STR = ",".join(all_values)
        print(f"VALUES_STR: ", VALUES_STR)
        
        query = f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}({COLUMN_NAMES}) VALUES {VALUES_STR} RETURNING id,{COLUMN_NAMES}"
        print(f"query: ", query)
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(query)
            return {"status": "success", "data": [dict(row) for row in rows]}
    except Exception as e:
        print(f"Error in fn_bulk_add_record: {str(e)}")
        return {"status": "error", "message": str(e)}


# Update Record
async def fn_update_record(pool,input_map:Optional[dict]):
    try:
        params = input_map["parameters"]
        SCHEMA_NAME,TABLE_NAME,column_names, row_values, record_id = (
        params.get("schema_name").strip(),
        params.get("table_name").strip(),
        params.get("column_names"),
        params.get("row_values"),
        params.get("record_id"),
        )

        COLUMN_NAMES = ",".join(column_names)
        
        SET_VALUES = ""                        
        for col in column_names:               
            value = row_values[col]            
            SET_VALUES += f"{col}='{value}',"  
        SET_VALUES = SET_VALUES[:-1]       

        query = f"UPDATE {SCHEMA_NAME}.{TABLE_NAME} SET {SET_VALUES} WHERE record_id='{record_id}' RETURNING id,{COLUMN_NAMES}"
        print(f"query: ", query)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query)
            if row:
                return {"status": "success", "data": dict(row)}
            else:
                return {"status": "failure", "message": "Record not found"}
    except Exception as e:
        print(f"Error in fn_update_record: {str(e)}")
        return {"status": "error", "message": str(e)}

# Update Record by Filtered query
async def fn_filtered_update_record(pool,input_map:Optional[dict]):
    try:
        params = input_map["parameters"]
        SCHEMA_NAME,TABLE_NAME,column_names, row_values, record_id,filter_query = (
        params.get("schema_name").strip(),
        params.get("table_name").strip(),
        params.get("column_names"),
        params.get("row_values"),
        params.get("record_id"),
        params.get("filter_query"),
        )

        COLUMN_NAMES = ",".join(column_names)
        
        SET_VALUES = ""                        
        for col in column_names:               
            value = row_values[col]            
            SET_VALUES += f"{col}='{value}',"  
        SET_VALUES = SET_VALUES[:-1]       

        query = f"UPDATE {SCHEMA_NAME}.{TABLE_NAME} SET {SET_VALUES} WHERE {filter_query} RETURNING id,{COLUMN_NAMES}"
        print(f"query: ", query)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query)
            if row:
                return {"status": "success", "data": dict(row)}
            else:
                return {"status": "failure", "message": "Record not found"}
    except Exception as e:
        print(f"Error in fn_filtered_update_record: {str(e)}")
        return {"status": "error", "message": str(e)}


# Schema Logics
# Create Schema
async def fn_create_schema(pool,input_map:Optional[dict]):
    try:
        SCHEMA_NAME = input_map["schema_name"].strip()
        query = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"
        print(f"query: ", query)
        async with pool.acquire() as conn:
            await conn.execute(query)
            return {"status": "success", "message": "Schema created successfully"}
    except Exception as e:
        return {"message": str(e)}
