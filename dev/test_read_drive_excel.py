#https://stackoverflow.com/questions/70391990/implement-access-token-from-oauth-playground-google-with-pydrive


#https://stackoverflow.com/questions/65482574/read-excel-file-from-google-drive-without-downloading-file
#https://stackoverflow.com/questions/40653050/using-python-to-update-a-file-on-google-drive
#https://google-auth.readthedocs.io/en/stable/reference/google.oauth2.credentials.html
#https://deepnote.com/guides/google-cloud/how-to-download-files-from-google-drive-in-python

#TOKEN
#https://stackoverflow.com/questions/19766912/how-do-i-authorise-an-app-web-or-installed-without-user-intervention/19766913#19766913
#DATA
#https://collections.naturalsciences.be/cpb/cetaf-passport-and-collections-registry-input/forms#b_start=0
#https://docs.google.com/spreadsheets/d/1gpZt4gX9qOXWY2fJNcI6XlnBgQDBsFOxyoFqofbymn8/edit?resourcekey&usp=forms_web_b#gid=415402299

from  urllib.parse import quote
import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO
import pandas as pnd


refresh_token="";
client_id     = '';
client_secret = '';
redirect_uri="https://developers.google.com/oauthplayground";

refresh_url = "https://www.googleapis.com/oauth2/v4/token";

encoded_client=quote(client_id, safe='~()*!.\'')
encoded_secret=quote(client_secret, safe='~()*!.\'')
encoded_token=quote(refresh_token, safe='~()*!.\'')
print(encoded_client)

headers={
    'Content-Type': 'Content-Type: application/x-www-form-urlencoded',
}
post_body = 'grant_type=refresh_token&client_id='+encoded_client+'&client_secret='+encoded_secret+'&refresh_token='+encoded_token
print(post_body)
resp=requests.post(refresh_url, params=post_body, headers=headers)
print(resp)
print(resp.json())
print("=============") 
print(resp.status_code)
if resp.status_code==200:
    access_token=resp.json()["access_token"]
    print(access_token)
    scopes=["https://www.googleapis.com/auth/drive"]
    creds = Credentials(token=access_token, default_scopes =scopes, client_id =client_id, client_secret =client_secret  )
    if not creds or not creds.valid:
        print("invalid")
    else:
        print("valid")
        try:
            

            
            test_url='https://drive.google.com/file/d/xxxxx'
            print(test_url)
            service = build('drive', 'v3', credentials=creds)
            request = service.files().get_media(fileId="xxxxxx")#.execute()
            print(request)
            fh = BytesIO()
            downloader = MediaIoBaseDownload(fd=fh, request=request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print("Download Progress: {0}".format(int(status.progress() * 100)))
            print(fh.getvalue())
            df = pnd.read_excel(fh.getvalue(), sheet_name="Collection overview")
            print(df.info)
           
        except HttpError as error:
            # TODO(developer) - Handle errors from drive API.
            print(f'An error occurred: {error}')
