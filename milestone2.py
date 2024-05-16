from datetime import datetime
from os import mkdir
import os
from zipfile import ZipFile
import pandas as pd


def merge_fraud_data(milestone7df, customer_list_path, fraud_list_path):
  # Step 1: Read liveCustomerList.csv and create fullName column
  df2 = pd.read_csv(customer_list_path)
  df2["fullName"] = df2["firstName"].str.upper() + "," + df2["lastName"].str.upper()

  # Step 2: Read liveFraudList.csv and create fullName column
  df3 = pd.read_csv(fraud_list_path)
  df3["fullName"] = df3["firstName"].str.upper() + "," + df3["lastName"].str.upper()

  # Step 3: Merge df3 and df2 on fullName, mark fraudster in df5
  df4 = pd.merge(df3, df2, on="fullName", how="left")
  df5 = pd.DataFrame(df4["custID"], columns=['custID'])
  df5["fraudster"] = 1

  # Step 4: Merge milestone7df and df5 on custID to detect fraudsters
  milestone7df['custID'] = milestone7df['custID'].astype('int64')
  df6 = pd.merge(milestone7df, df5, on="custID", how="left")
  df6.fillna(0, inplace=True)
  df6['fraudster'] = df6['fraudster'].astype('int64')
  milestone2df = df6

  return milestone2df