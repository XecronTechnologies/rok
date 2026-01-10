import os
import io
import pickle
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError

SCOPES = ['https://www.googleapis.com/auth/drive.file']
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "client_secret_766658435727-ectu9krotjpnk482smhakvo41l75do0f.apps.googleusercontent.com.json")
TOKEN_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "token.pickle")


def get_drive_service():
    creds = None
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'rb') as token:
            creds = pickle.load(token)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_PATH):
                raise Exception(
                    f"OAuth credentials file not found at {CREDENTIALS_PATH}. "
                    "Download it from Google Cloud Console > APIs & Services > Credentials > OAuth 2.0 Client IDs"
                )
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=8085)
        
        with open(TOKEN_PATH, 'wb') as token:
            pickle.dump(creds, token)
    
    return build('drive', 'v3', credentials=creds)


async def upload_file_to_drive(file_content: bytes, filename: str, mimetype: str, folder_id: str = None):
    try:
        service = get_drive_service()
        
        file_metadata = {'name': filename}
        if folder_id:
            file_metadata['parents'] = [folder_id]
        
        media = MediaIoBaseUpload(
            io.BytesIO(file_content),
            mimetype=mimetype
        )
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink, webContentLink'
        ).execute()
        
        file_id = file.get('id')
    
        service.permissions().create(
            fileId=file_id,
            body={'type': 'anyone', 'role': 'reader'}
        ).execute()
        
        is_media = mimetype.startswith('image/') or mimetype.startswith('video/')
        if is_media:
            public_url = f"https://lh3.googleusercontent.com/d/{file_id}"
        else:
            public_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        
        return {
            'success': True,
            'file_id': file_id,
            'file_name': file.get('name'),
            'public_url': public_url,
            # 'direct_url': "https://lh3.googleusercontent.com/d/{file_id}" if is_media else f"https://drive.google.com/uc?export=download&id={file_id}",
            'view_url': f"https://drive.google.com/uc?export=view&id={file_id}",
            'download_url': f"https://drive.google.com/uc?export=download&id={file_id}",
            'file': file
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
