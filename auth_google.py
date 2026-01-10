"""
Run this script ONCE to authenticate with Google.
After running, a token.pickle file will be created and the API will work.
"""
import os
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/drive.file']
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "app", "client_secret_766658435727-ectu9krotjpnk482smhakvo41l75do0f.apps.googleusercontent.com.json")
TOKEN_PATH = os.path.join(os.path.dirname(__file__), "app", "token.pickle")

def authenticate():
    print(f"Looking for credentials at: {CREDENTIALS_PATH}")
    print(f"Token will be saved at: {TOKEN_PATH}")
    
    if not os.path.exists(CREDENTIALS_PATH):
        print(f"ERROR: Credentials file not found!")
        return
    
    creds = None
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'rb') as token:
            creds = pickle.load(token)
        print("Existing token found!")
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing expired token...")
            creds.refresh(Request())
        else:
            print("Opening browser for authentication...")
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=8000)
        
        with open(TOKEN_PATH, 'wb') as token:
            pickle.dump(creds, token)
        print(f"Token saved to {TOKEN_PATH}")
    
    print("\n✅ Authentication successful! You can now use the API.")

if __name__ == "__main__":
    authenticate()
