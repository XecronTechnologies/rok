import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Path to service account credentials
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "..", "xecron-client-management-b4b191c2b6e7.json")

class GoogleSheetsService:
    """Google Sheets service for FastAPI"""
    _instance = None
    
    @staticmethod
    def get_service():
        if GoogleSheetsService._instance is None:
            try:
                if not os.path.exists(CREDENTIALS_PATH):
                    raise Exception(f"Credentials file not found: {CREDENTIALS_PATH}")
                
                credentials = service_account.Credentials.from_service_account_file(
                    CREDENTIALS_PATH,
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
                GoogleSheetsService._instance = build('sheets', 'v4', credentials=credentials)
            except Exception as e:
                raise Exception(f"Failed to initialize Google Sheets: {str(e)}")
        return GoogleSheetsService._instance


async def add_data_to_sheet(spreadsheet_id: str, range_name: str, values: list):
    """
    Add data to Google Sheet
    
    Args:
        spreadsheet_id: The ID of the spreadsheet
        range_name: Range like 'Sheet1!A:C'
        values: 2D list of values, e.g. [['Name', 'Email'], ['John', 'john@example.com']]
    
    Returns:
        dict with success status and updated info
    """
    try:
        service = GoogleSheetsService.get_service()
        
        body = {
            'values': values
        }
        
        result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        
        return {
            'success': True,
            'updated_range': result.get('updates', {}).get('updatedRange'),
            'updated_rows': result.get('updates', {}).get('updatedRows'),
            'updated_cells': result.get('updates', {}).get('updatedCells')
        }
    except HttpError as e:
        return {
            'success': False,
            'error': str(e)
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }
