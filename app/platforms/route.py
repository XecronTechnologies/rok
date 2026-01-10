import json
from typing import Optional
from app.platforms.neon.neon_db import fn_neon_db
from app.logic.login_api import fn_send_otp,fn_verify_otp,fn_sign_up

async def fn_route(input_map:Optional[dict]):
    request_map = {}
    if(input_map.get("platform")=="neon"):
        request_map =  await fn_neon_db(input_map)
    elif(input_map.get("platform")=="telegram" and input_map.get("action_type")=="send_otp"):
        request_map =  await fn_send_otp(input_map)
    elif(input_map.get("platform")=="telegram" and input_map.get("action_type")=="verify_otp"):
        request_map =  await fn_verify_otp(input_map)
    elif(input_map.get("platform")=="telegram" and input_map.get("action_type")=="signup"):
        request_map =  await fn_sign_up(input_map)
    else:
        request_map =  {"message":"try with valid db"}
    return request_map
   