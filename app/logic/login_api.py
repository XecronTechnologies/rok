import json
import random
import bcrypt
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from pydantic import BaseModel
from app.utils.time_handler import unix_time_handler
from app.utils.id_generator import org_id_generator
from app.utils.db_pool import get_pool
from app.platforms.neon.neon_db_logics import fn_get_record,fn_filter_record,fn_add_record,fn_filtered_update_record,fn_create_schema


BOT_TOKEN = "8538090434:AAHgFqEHcuC63azYjFUrDsc-rlYtGkOL5P4"
WEBHOOK_URL = "https://play.svix.com/in/e_Gy5mosnS2bfD8ZQLaiUZJKkYkY9/"
bot = Bot(token=BOT_TOKEN)

def get_failure_keyboard(error_type: str):
    """Create an inline keyboard with a Report Issue button"""
    keyboard = [
        [InlineKeyboardButton("🔄 Retry", callback_data=f"retry_{error_type}"),
         InlineKeyboardButton("⚠️ Report Issue", callback_data=f"report_{error_type}")]
    ]
    return InlineKeyboardMarkup(keyboard)

async def fn_sign_up(message):
    pool = await get_pool()
    if(message["action_type"]!="get_record"):
        message["action_type"] = "get_record"
    # Check the org is valid
    
    message["parameters"]["filter_query"] = f"email = '{message.get('parameters').get('row_values').get('email')}'"
    get_the_record = await fn_filter_record(pool,message)
    print(f"get_the_record {get_the_record}")

    if get_the_record and get_the_record[0].get('email'):
        telegram_response = await bot.send_message(chat_id=message.get("chat_id"), text="User already exists")
    else:
        print(f"message {message}")
        # Hash the password with bcrypt
        plain_password = message["parameters"]["row_values"].get("password", "")
        hashed_password = bcrypt.hashpw(plain_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        message["parameters"]["row_values"]["password"] = hashed_password
        
        message["parameters"]["row_values"]["org_id"] = org_id_generator()
        add_the_record = await fn_add_record(pool, message)
        create_schema = await fn_create_schema(pool, {"schema_name":message.get('parameters').get('row_values').get('org_name')})
        telegram_response = await bot.send_message(chat_id=message.get("chat_id"), text="signup successfull")
    telegram_response_json =  telegram_response.to_dict()
    return telegram_response_json


async def fn_send_otp(message):
    pool = await get_pool()
    if(message["action_type"]!="get_record"):
        message["action_type"] = "get_record"
    # Check the org is valid
    message["parameters"]["filter_query"] = f"org_id = '{message.get('cl_id').get('org_id')}'"
    message["parameters"]["column_names"] = ["org_id","org_name"]

    get_the_record = await fn_filter_record(pool,message)

    if(get_the_record[0].get('org_id') and get_the_record[0].get('org_name')):
        # Check the user is valid
        message["parameters"]["table_name"] = "user_master"
        message["parameters"]["schema_name"] = get_the_record[0].get('org_name')
        message["parameters"]["column_names"]=["user_id","user_name"]
        message["parameters"]["filter_query"] = f"email = '{message.get('cl_id').get('email')}'"
        org_name = get_the_record[0].get('org_name')
        org_id = get_the_record[0].get('org_id')
        get_the_record = await fn_filter_record(pool,message)
        # Send OTP
        if(get_the_record[0].get('user_id')):
            random_6_digit_otp = str(random.randint(100000, 999999))
            user_id = get_the_record[0].get('user_id')
            user_name = get_the_record[0].get('user_name')
            
            # Check if a login record already exists for this user
            message["parameters"]["table_name"] = "login_master"
            message["parameters"]["schema_name"] = org_name
            message["parameters"]["column_names"] = ["record_id"]
            message["parameters"]["filter_query"] = f"user_id = '{user_id}'"
            existing_record = await fn_filter_record(pool, message)
            
            if existing_record and existing_record[0].get('record_id'):
                # Update existing record with new OTP (hashed with bcrypt)
                hashed_otp = bcrypt.hashpw(random_6_digit_otp.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                message["parameters"]["column_names"] = ["login_status", "otp_config", "modified_at"]
                message["parameters"]["row_values"] = {
                    "login_status": "pending",
                    "otp_config": hashed_otp,
                    "modified_at": unix_time_handler()
                }
                message["parameters"]["filter_query"] = f"user_id = '{user_id}'"
                update_result = await fn_filtered_update_record(pool, message)
                if update_result.get("status") == "success":
                    telegram_response = await bot.send_message(chat_id=message.get("chat_id"), text=f"hey {user_name}\nYour OTP is {random_6_digit_otp}")
                else:
                    telegram_response = await bot.send_message(chat_id=message.get("chat_id"), text="Failed to send OTP")
            else:
                # Add new record with hashed OTP
                hashed_otp = bcrypt.hashpw(random_6_digit_otp.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                current_time = unix_time_handler()
                message["parameters"]["column_names"] = ["login_status", "user_id", "otp_config", "created_at", "modified_at","org_id"]
                message["parameters"]["row_values"] = {
                    "login_status": "pending",
                    "user_id": user_id,
                    "otp_config": hashed_otp,
                    "created_at": current_time,
                    "modified_at": current_time,
                    "org_id":org_id
                }
                add_the_record = await fn_add_record(pool, message)
                if add_the_record.get("status") == "success":
                    telegram_response = await bot.send_message(chat_id=message.get("chat_id"), text=f"hey {user_name}\nYour OTP is {random_6_digit_otp}")
                else:
                    telegram_response = await bot.send_message(chat_id=message.get("chat_id"), text="User not found0")
        else:
            telegram_response = await bot.send_message(chat_id=message.get("chat_id"), text="User not found1")
    else:
        telegram_response = await bot.send_message(chat_id=message.get("chat_id"), text="Org/User not found")
    telegram_response_json =  telegram_response.to_dict()
    return telegram_response_json

async def fn_verify_otp(message):
    pool = await get_pool()
    if(message["action_type"]!="get_record"):
        message["action_type"] = "get_record"
    # Check the org is valid
    message["parameters"]["filter_query"] = f"org_id = '{message.get('cl_id').get('org_id')}'"
    message["parameters"]["column_names"] = ["org_id","org_name"]

    get_the_record = await fn_filter_record(pool,message)

    if(get_the_record[0].get('org_id') and get_the_record[0].get('org_name')):
        # Check the user is valid
        message["parameters"]["table_name"] = "user_master"
        message["parameters"]["schema_name"] = get_the_record[0].get('org_name')
        message["parameters"]["column_names"]=["user_id","user_name"]
        message["parameters"]["filter_query"] = f"email = '{message.get('cl_id').get('email')}'"
        org_name = get_the_record[0].get('org_name')
        get_the_record = await fn_filter_record(pool,message)
        # Send OTP
        if(get_the_record[0].get('user_id')):
            user_id = get_the_record[0].get('user_id')
            user_name = get_the_record[0].get('user_name')
            message["parameters"]["table_name"] = "login_master"
            message["parameters"]["schema_name"] = org_name
            message["parameters"]["column_names"] = ["otp_config", "record_id"]
            message["parameters"]["filter_query"] = f"user_id = '{user_id}'"
            login_records = await fn_filter_record(pool, message)
            
            # Get the OTP from the latest record (first one if sorted by id desc, or last if multiple)
            if login_records and login_records[0].get('otp_config'):
                stored_hashed_otp = login_records[-1].get('otp_config') if len(login_records) > 1 else login_records[0].get('otp_config')
                input_otp = message.get('parameters').get('otp')
                
                # Verify OTP using bcrypt
                if bcrypt.checkpw(input_otp.encode('utf-8'), stored_hashed_otp.encode('utf-8')):
                    # OTP verified - update the record
                    message["parameters"]["column_names"] = ["login_status", "modified_at"]
                    message["parameters"]["row_values"] = {"login_status": "success", "modified_at": unix_time_handler()}
                    message["parameters"]["filter_query"] = f"id = (SELECT id FROM {org_name}.login_master WHERE user_id = '{user_id}' ORDER BY id DESC LIMIT 1)"
                    update_result = await fn_filtered_update_record(pool, message)
                    
                    if update_result.get("status") == "success":
                        telegram_response = await bot.send_message(chat_id=message.get("chat_id"), text=f"Hey {user_name}, OTP verified successfully!")
                    else:
                        telegram_response = await bot.send_message(chat_id=message.get("chat_id"), text="Failed to update login status")
                else:
                    telegram_response = await bot.send_message(
                        chat_id=message.get("chat_id"), 
                        text="Invalid OTP",
                        reply_markup=get_failure_keyboard("invalid_otp")
                    )
            else:
                telegram_response = await bot.send_message(chat_id=message.get("chat_id"), text="No OTP found for this user")
        else:
            telegram_response = await bot.send_message(chat_id=message.get("chat_id"), text="User not found3")
    else:
        telegram_response = await bot.send_message(chat_id=message.get("chat_id"), text="Org/User not found")
    telegram_response_json =  telegram_response.to_dict()
    return telegram_response_json