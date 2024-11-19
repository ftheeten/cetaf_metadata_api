from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from  urllib.parse import quote
import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO
import pandas as pnd
import numpy as np
import openpyxl

class read_excel():
    def __init__(self):
        self.refresh_token=""
        self.client_id     = ''
        self.client_secret = ''
        self.redirect_uri="https://developers.google.com/oauthplayground"

        self.refresh_url = "https://www.googleapis.com/oauth2/v4/token"
        self.encoded_client=quote(self.client_id, safe='~()*!.\'')
        self.encoded_secret=quote(self.client_secret, safe='~()*!.\'')
        self.encoded_token=quote(self.refresh_token, safe='~()*!.\'')
        
    def get_excel(self, file_id, sheet_name, p_header=None, skiprows=None):
        headers={'Content-Type': 'Content-Type: application/x-www-form-urlencoded',}
        post_body = 'grant_type=refresh_token&client_id='+self.encoded_client+'&client_secret='+self.encoded_secret+'&refresh_token='+self.encoded_token
        resp=requests.post(self.refresh_url, params=post_body, headers=headers)
        if resp.status_code==200:
            access_token=resp.json()["access_token"]
            scopes=["https://www.googleapis.com/auth/drive"]
            creds = Credentials(token=access_token, default_scopes =scopes, client_id =self.client_id, client_secret =self.client_secret  )
            if not creds or not creds.valid:
                print("invalid")
            else:
                try: 
                    service = build('drive', 'v3', credentials=creds)
                    request = service.files().get_media(fileId=file_id)#.execute()
                    fh = BytesIO()
                    downloader = MediaIoBaseDownload(fd=fh, request=request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                    df = pnd.read_excel(fh.getvalue(), sheet_name=sheet_name, header=p_header, skiprows=skiprows)
                    df = df.replace({np.nan: None})
                    wb = openpyxl.load_workbook(fh)
                    print(wb.properties.modified)
                    return df, wb.properties.modified
                except HttpError as error:
                    # TODO(developer) - Handle errors from drive API.
                    print(f'An error occurred: {error}')
        return None, None