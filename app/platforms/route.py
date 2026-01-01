import json
from typing import Optional
from app.platforms.neon.neon_db import fn_neon_db

async def fn_route(input_map:Optional[dict]):
    request_map = {}
    if(input_map.get("platform")=="neon"):
        request_map =  await fn_neon_db(input_map)
    else:
        request_map =  {"message":"try with valid db"}
    return request_map
   