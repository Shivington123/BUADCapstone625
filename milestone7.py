from os import mkdir
import os
from zipfile import ZipFile
from urllib.request import urlretrieve
import requests
import pandas as pd
import regex as re

def download_extract_zip(url, inputFile, outputDir):
  urlretrieve(url, inputFile)
  with ZipFile(inputFile) as zipObj:
      zipObj.extractall(outputDir)

def extract_csv_from_dropbox(userURL):
  match = re.search(r'(\d+)\.zip', userURL)
  if match:
      extracted_term = match.group(0)

  response = requests.get(userURL)
  with open(extracted_term, 'wb') as f:
      f.write(response.content)

  return extracted_term

# download_extract_zip(url, inputFile, output_dir)

# dir_list = os.listdir(output_dir)

# new_list = []

# for i in dir_list:
#   if i.endswith(".jpg"):
#     new_list.append(i[0:10])

# df1 = pd.DataFrame(new_list, columns=['concat'])
# df1[['custID', 'bankAcctID']] = df1['concat'].str.split('_', expand=True)
# df1['custID'] = df1['custID'].astype(int)
# df1['bankAcctID'] = df1['bankAcctID'].astype(int)
# milestone7df = df1[['custID', 'bankAcctID']]

def process_directory(output_dir):
    # Step 1: Get list of files in output_dir
    dir_list = os.listdir(output_dir)

    # Step 2: Extract relevant information from file names and create DataFrame
    new_list = []
    for i in dir_list:
        if i.endswith(".jpg"):
            new_list.append(i[0:11])

    df1 = pd.DataFrame(new_list, columns=['concat'])
    df1[['custID', 'bankAcctID']] = df1['concat'].str.split('_', expand=True)
    df1['custID'] = df1['custID'].astype(int)
    df1['bankAcctID'] = df1['bankAcctID'].astype(int)

    # Step 3: Extract required columns and assign it to milestone7df
    milestone7df = df1[['custID', 'bankAcctID']]

    return milestone7df

def milestone7output(url, inputFile, outputDir):
  # Step 1: Download and extract the zip file
  download_extract_zip(url, inputFile, outputDir)

  # Step 2: Extract CSV files from Dropbox
  extract_csv_from_dropbox(url)

  # Step 3: Process directory to get milestone7df
  milestone7df = process_directory(outputDir)

  return milestone7df
