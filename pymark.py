from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.colab import auth
from oauth2client.client import GoogleCredentials
import pandas as pd

# Authenticate and create the PyDrive client.
def auth_google_drive():
  auth.authenticate_user()
  gauth = GoogleAuth()
  gauth.credentials = GoogleCredentials.get_application_default()
  drive = GoogleDrive(gauth)
  return drive

def get_data_from_google_drive(file_id, save_as):
  drive = auth_google_drive()
  
  downloaded = drive.CreateFile({'id':file_id}) 
  downloaded.GetContentFile(save_as)  
  data_csv = pd.read_csv(save_as)

  return data_csv
