from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.colab import auth
from oauth2client.client import GoogleCredentials
from sklearn import preprocessing

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

# With my standard, I prefix all columns that are garbage with the word "Ignore"
# I also prefix output columns with the word "Output"
def get_x_and_y(df, y_column_name):
  df_y = df[y_column_name]
  
  ignore_cols = [col for col in df if col.startswith('Ignore') or col.startswith('Output')]

  print('Dropping these columns:', ignore_cols)
  
  df_x = df[df.columns.drop(ignore_cols)]

  return df_x, df_y

# With my standard, I prefix all categories with the word "Category"
def encode_category_features(df):
    features = [col for col in df if col.startswith('Category')]
    #features = ['Fare', 'Cabin', 'Age', 'Sex', 'Lname', 'NamePrefix']
    
    for feature in features:
        le = preprocessing.LabelEncoder()
        le = le.fit(df[feature])
        df[feature] = le.transform(df[feature])
    return df

# Build the model def for ludwig based on column names
def ludwig_build_model_definition(df, output_col=None, output_type=None):
  # returns "{input_features: [{name: text, type: text, encoder: parallel_cnn, level: word}], output_features: [{name: class, type: category}]}"
  
  if (output_col == None):
    output_col = [col for col in df if col.startswith('Output')][0] # get the FIRST "Output" column 
  
  # todo: handle category
  if (output_type == None):
    if (min(df[output_col]) == 0 and max(df[output_col]) == 1):
      output_type = 'binary'
    else:
      output_type = 'numerical'

  # print(min(df[output_col]), max(df[output_col]), output_col, output_type)
  
  inputs = []
  
  for col in df:
    if not col.startswith('Output'): # skip if it does
      if col.startswith('Category'):
        input_type = 'category'
      else:
        input_type = 'numerical'
      
      inputs.append({ 'name': col, 'type': input_type })
  
  return {
    "input_features": inputs,
    "output_features": [{
      "name": output_col,
      "type": output_type,
    }]
  }
