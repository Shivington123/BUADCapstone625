from zipfile import ZipFile
from urllib.request import urlretrieve
import requests
import os
import pandas as pd
import re

# def download_extract_zip(url, inputFile, outputDir):
#     urlretrieve(url, inputFile)
#     with ZipFile(inputFile) as zipObj:
#         zipObj.extractall(outputDir)

# def extract_csv_from_dropbox(dropbox_url):
#     match = re.search(r'(\d+)\.csv', dropbox_url)
#     if match:
#         extracted_term = match.group(0)

#     response = requests.get(dropbox_url)
#     with open(extracted_term, 'wb') as f:
#         f.write(response.content)

#     return extracted_term

def convert_to_int(x):
    try:
        # Try converting to integer
        return int(x)
    except (ValueError, TypeError):
        # Handle NaNs or non-numeric values
        return None

def process_files(milestone2df,live_bank_acct_path,customer_list_path):
    df1 = pd.read_csv(live_bank_acct_path)
    df1 = pd.DataFrame(df1)
    df2 = pd.read_csv(customer_list_path)
    df2 = pd.DataFrame(df2)
    # from milestone 2
    # df3 = pd.read_csv(extracted_term)
    # # from milestone 2
    # df3 = pd.DataFrame(df3)
    # from milestone 2
    milestone2df["Identity"] = milestone2df["bankAcctID"].astype(str) + "," + milestone2df["custID"].astype(str)

    df2["fullName"] = df2["firstName"].str.upper() + "," + df2["lastName"].str.upper()
    df2["custID"] = df2["custID"].astype(str)
    df1["fullName"] = df1["firstName"].str.upper() + "," + df1["lastName"].str.upper()
    df4 = pd.merge(df1, df2, on="fullName", how="left")
    df5 = df4[["bankAcctID", "custID"]]
    df5 = df5.fillna("0")
    df5["bankAcctID"] = df5["bankAcctID"].astype(int)

    df5_filter = df5[df5["custID"] != "0"]
    df5_filter = pd.DataFrame(df5_filter)

    df5_filter["Identity"] = df5_filter["bankAcctID"].astype(str) + "," + df5_filter["custID"].astype(str)

    # from milestone 2
    testdf = pd.merge(milestone2df, df5_filter, on="Identity", how="left")
    testdf1 = testdf.fillna("0")
    # testdf1["bankAcctID"] = testdf1["bankAcctID"].apply(convert_to_int)
    testdf1_filter = testdf1[testdf1["custID_y"] != "0"]

    testdf2 = pd.DataFrame(testdf1_filter["custID_x"])
    testdf2 = testdf2.reset_index(drop=True)
    testdf2["rightAcctFlag"] = 1

    testdf1_nonfilter = testdf1[testdf1["custID_y"] == "0"]
    testdf3 = pd.DataFrame(testdf1_nonfilter["custID_x"])
    testdf3["rightAcctFlag"] = 0
    testdf4 = pd.concat([testdf2, testdf3])
    testdf4.rename(columns={'custID_x': 'custID'}, inplace=True)
    milestone3df = pd.merge(milestone2df,testdf4, on = "custID", how = "left")
    milestone3df = milestone3df.drop(columns=['Identity'])
    milestone3df = milestone3df.rename(columns={'custID': 'loginID'})

    return milestone3df
    

# # Call the functions
# url = "https://www.dropbox.com/sh/rlrv8z10ahyhl8l/AADQs5cFZKxKHUVlyNeAkGjua?dl=1"
# inputFile = "./dropBoxFile.zip"
# outputDir = "dropbox"

# download_extract_zip(url, inputFile, outputDir)
