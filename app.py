import streamlit as st
from milestone7 import download_extract_zip, extract_csv_from_dropbox, milestone7output
import pandas as pd
import base64
import os
import shutil
from milestone1 import milestone1output
from milestone2 import merge_fraud_data
from milestone3 import process_files
from milestone5 import process_transaction_data, predict_next_paydate
import requests
import regex as re
import extract

output_dir = "dropbox/milestone7"
inputFile = "./dropBoxFile.zip"
customer_list_path = "https://raw.githubusercontent.com/Shivington123/BUADCapstone625/main/liveCustomerList.csv"
fraud_list_path = "https://raw.githubusercontent.com/Shivington123/BUADCapstone625/main/liveFraudList.csv"
live_bank_acct_path = "https://raw.githubusercontent.com/Shivington123/BUADCapstone625/main/liveBankAcct.csv"
transactions_file = "https://raw.githubusercontent.com/Shivington123/BUADCapstone625/main/bankTransactions.csv"
milestone1url = "https://www.dropbox.com/sh/6a0nlzuzhwl858p/AADSGShTB9oxVq_KauD9ZDLJa?dl=1"
milestone1output_dir = "dropbox"
milestone1output_dirnew = "dropbox/knownPics-custID_PicID/identityPics-custID_PicID"

def main():
    st.title("Milestone 7")

    # Text input for user to provide the URL of the test data
    userURL = st.text_input('Enter URL of Test Data')

    if st.button('Process Data'):
        if userURL:
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
                    st.error(f'Failed to delete {file_path}. Reason: {e}')

            # Process the initial ZIP file and merge fraud data
            df = milestone7output(userURL, inputFile, output_dir)
            df2 = merge_fraud_data(df, customer_list_path, fraud_list_path)

            # Further processing of the data
            df3 = process_files(df2, live_bank_acct_path, customer_list_path)

            # Extract nested ZIP files
            extract.unzip_nested_zip(milestone1url, milestone1output_dir)

            # Process the milestone1 output
            extracted_term = extract_csv_from_dropbox(userURL)
            df6 = milestone1output(df3, output_dir, extracted_term, milestone1output_dirnew)

            # Error handling for process_transaction_data
            try:
                df4 = process_transaction_data(df6, transactions_file)
                df5 = predict_next_paydate(df4)
                df5.to_csv(extracted_term + ".csv", index=False)

                # Provide download link for the processed CSV file
                st.markdown(get_download_link(df5, filename=extracted_term + ".csv"), unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Error processing transaction data: {e}")
        else:
            # Display a warning message if no URL is provided
            st.warning("Please enter a URL before processing.")

def get_download_link(df, filename='file.csv'):
    """Generate a download link for a given DataFrame."""
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">Download CSV File</a>'
    return href

if __name__ == '__main__':
    main()
