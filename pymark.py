from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.colab import auth
from oauth2client.client import GoogleCredentials

# Authenticate and create the PyDrive client.
def auth_google_drive():
  auth.authenticate_user()
  gauth = GoogleAuth()
  gauth.credentials = GoogleCredentials.get_application_default()
  drive = GoogleDrive(gauth)
  return drive

def get_data_from_google_drive(file_id, save_as):
  import pandas as pd

  drive = auth_google_drive()
  
  downloaded = drive.CreateFile({'id':file_id}) 
  downloaded.GetContentFile(save_as)  
  data_csv = pd.read_csv(save_as)

  return data_csv

def get_x_and_y(df, y_column_name):
  df_y = df[y_column_name]
  
  ignore_cols = [col for col in df if col.startswith('Ignore') or col.startswith('Output')]

  print('Dropping these columns:', ignore_cols)
  
  df_x = df
  df_x = df_x[df.columns.drop(ignore_cols)]

  return df_x, df_y
