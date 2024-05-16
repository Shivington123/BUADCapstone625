from zipfile import ZipFile
from urllib.request import urlretrieve
import pandas as pd
import numpy as np
from datetime import timedelta
import re 
import requests
from dateutil.relativedelta import relativedelta
import csv

def process_transaction_data(milestone1df, transactions_file):
    # Read the CSV file
    df2 = pd.read_csv(transactions_file)
    df3 = pd.merge(milestone1df, df2, on="bankAcctID", how="left")
    # Filter transactions with amount > 200
    df4 = df3[df3["transAmount"] > 200]
    # Convert date column to datetime format
    df4['date'] = pd.to_datetime(df4['date'], format='%Y-%m-%d')
    # Get the last two transactions per bankAcctID
    df_last_two = df4.groupby('bankAcctID').tail(2).reset_index(drop=True)

    return df_last_two

def predict_next_paydate(df):

  df = df.sort_values(by=['bankAcctID', 'date'])

  # Calculate time differences between the last two paydates
  df['time_diff'] = df.groupby('bankAcctID')['date'].diff().dt.days
     
  next_paydates = [] 
    
  # Iterate through the DataFrame
  for index, row in df.iterrows():
      if row['fraudster'] == 1 or row['rightAcctFlag'] == 0 or row['verifiedID'] == 0:
        next_paydate = None
      elif row['date'].month < 4 and (index + 1 == len(df) or (index + 1 < len(df) and df.iloc[index + 1]['date'].month < 4)):
          next_paydate = None
      elif row['date'].day == row['date'].days_in_month and index + 1 < len(df) and df.iloc[index + 1]['date'].day == df.iloc[index + 1]['date'].days_in_month:  # Last day of the month and next row's day is also the last day of the month
          next_month = row['date'] + pd.DateOffset(months=3)  # Move to the next month
          next_paydate = next_month.replace(day=1) - pd.DateOffset(days=1)  # Set to the last day of the next month
      elif row['time_diff'] == 15:
          next_paydate = row['date'] + timedelta(days=15)
      elif row['time_diff'] == 7:
          next_paydate = row['date'] + timedelta(days=7)
      elif row['date'].weekday() == 2:  # Wednesday (0 is Monday, 1 is Tuesday, ..., 6 is Sunday)
          next_paydate = row['date'] + timedelta(weeks=2)
      elif row['date'].day == 6 and df.iloc[index + 1]['date'].day == 20:  # 6th and 20th of the month
          next_month = row['date'].replace(day=6)
          next_paydate = next_month if next_month > row['date'] else next_month.replace(month=next_month.month + 1)
      # elif row['date'].day == 6:  # 6th of the month
      #     next_month = row['date'].replace(day=20)
      #     next_paydate = next_month if next_month > row['date'] else next_month.replace(month=next_month.month + 1)
      # elif row['date'].day == 20:  # 20th of the month
      #     next_month = row['date'].replace(day=6)
      #     next_paydate = next_month if next_month > row['date'] else next_month.replace(month=next_month.month + 1)
      elif row['date'].weekday() == 4 and row['time_diff'] == 14:  # Friday and 14 days difference
          next_paydate = row['date'] + timedelta(days=14)
      elif row['date'].weekday() == 4 and row['time_diff'] == 21:
          next_paydate = row['date'] + timedelta(days=21)
      elif row['time_diff'] == 14:
          next_paydate = row['date'] + timedelta(days=14)
      else:
          next_paydate = row['date'] + timedelta(days=14)


      next_paydates.append((row['loginID'], next_paydate))

  # Convert the list of tuples into a DataFrame
  df_next_paydate = pd.DataFrame(next_paydates, columns=['loginID','next_paydate'])

  # Keep only the last row for each bankAcctID
  milestone5df = df_next_paydate.groupby('loginID').last().reset_index()
  milestone5df = milestone5df.rename(columns={'next_paydate': 'date'})
  # milestone5df = milestone5df.rename(columns={'custID': 'loginID'})

  return milestone5df

# Predict next paydate for each bank account ID
# predicted_paydates = predict_next_paydate(df_last_two)

# Save the DataFrame to a CSV file
# predicted_paydates.to_csv(extracted_term, index=False)