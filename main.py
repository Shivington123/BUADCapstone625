import streamlit as st
from milestone7 import download_extract_zip, extract_csv_from_dropbox, milestone7output
import pandas as pd
import base64
import os
import shutil
from milestone1 import milestone1output
from milestone2 import merge_fraud_data
from milestone3 import process_files
from milestone5 import process_transaction_data
from milestone5 import predict_next_paydate
import requests 
import regex as re 
import extract 


userURL = "https://www.dropbox.com/scl/fi/jzwdom1qivaf08z5y049m/5169813.zip?rlkey=552h1y47nr73cj3lne3bo7a4k&dl=1"
output_dir = "milestone7"
target_dir = output_dir
inputFile = "./dropBoxFile.zip"
customer_list_path = "https://raw.githubusercontent.com/Shivington123/BUADCapstone625/main/liveCustomerList.csv"
fraud_list_path = "https://raw.githubusercontent.com/Shivington123/BUADCapstone625/main/liveFraudList.csv"
live_bank_acct_path = "https://raw.githubusercontent.com/Shivington123/BUADCapstone625/main/liveBankAcct.csv"
transactions_file = "https://raw.githubusercontent.com/Shivington123/BUADCapstone625/main/bankTransactions.csv"
milestone1url = "https://www.dropbox.com/sh/6a0nlzuzhwl858p/AADSGShTB9oxVq_KauD9ZDLJa?dl=1"
milestone1output_dir = "dropbox"
milestone1output_dirnew = "dropbox/knownPics-custID_PicID/identityPics-custID_PicID"


def extract_csv_from_dropbox(userURL):
  # Match the part of the URL containing the .zip extension
  match = re.search(r'(\d+)\.zip', userURL)
  if match:
      # Extract the term and remove the .zip extension
      extracted_term = match.group(1)

  # Download the zip file from the URL
  response = requests.get(userURL)
  with open(extracted_term + '.zip', 'wb') as f:
      f.write(response.content)

  return extracted_term

extractedTerm = extract_csv_from_dropbox(userURL)

# Ensure the output directory exists
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Clear the output directory
for filename in os.listdir(output_dir):
    file_path = os.path.join(output_dir, filename)
    try:
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)
    except Exception as e:
        print(f'Failed to delete {file_path}. Reason: {e}')

df = milestone7output(userURL, inputFile, output_dir)
df2 = merge_fraud_data(df, customer_list_path, fraud_list_path)
df3 = process_files(df2, live_bank_acct_path, customer_list_path)
extract.unzip_nested_zip(milestone1url, milestone1output_dir)
df6 = milestone1output(df3, target_dir, extractedTerm,milestone1output_dirnew)
# error occurs here #
df4 = process_transaction_data(df6, transactions_file)
df5 = predict_next_paydate(df4)
df5.to_csv(extractedTerm+".csv", index=False)