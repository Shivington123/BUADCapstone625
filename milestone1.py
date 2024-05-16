import os
import extract
import pandas as pd
import numpy as np
from os import mkdir
import os
import boto3
import regex as re
from zipfile import ZipFile
from urllib.request import urlretrieve
import requests

# Call the function
def createFrames(SourceDirectory, TargetDirectory):
  source_dir_list = []
  source_knownpicslist = []
  source_imageNo = []
  for filename in os.listdir(SourceDirectory):
    if filename.endswith(".jpg"):
      filepath = os.path.join(SourceDirectory, filename)
      source_dir_list.append(filepath)
      source_knownpicslist.append(filename[0:4])
      source_imageNo.append(filename[5:10])


  source_df1 = pd.DataFrame({'loginID': source_knownpicslist,'imageNo': source_imageNo,'SourceimagePath': source_dir_list})
  source_df1 = source_df1.groupby('loginID').first().reset_index()

  target_dir_list = []
  target_knownpicslist = []
  for filename in os.listdir(TargetDirectory):
    if filename.endswith(".jpg"):
      filepath = os.path.join(TargetDirectory, filename)
      target_knownpicslist.append(filename[0:4])
      target_dir_list.append(filepath)

  target_df1 = pd.DataFrame({'loginID': target_knownpicslist,'TargetimagePath': target_dir_list})

  merge_df1 = target_df1.merge(source_df1, on='loginID', how= 'left')
  first_value = merge_df1.iloc[0, 2]
  second_value = merge_df1.iloc[0, 3]
  merge_df1 = merge_df1.fillna(method='ffill')
  merge_df1 = merge_df1.fillna({'imageNo':first_value,'SourceimagePath':second_value})

  return merge_df1

def compare_faces(sourceFile, targetFile, client):
  # Load the source and target images
  imageSource = open(sourceFile, 'rb')
  imageTarget = open(targetFile, 'rb')

  response = client.compare_faces(SimilarityThreshold=80,
                                  SourceImage={'Bytes': imageSource.read()},
                                  TargetImage={'Bytes': imageTarget.read()})
  for faceMatch in response['FaceMatches']:
    position = faceMatch['Face']['BoundingBox']
    similarity = str(faceMatch['Similarity'])


  imageSource.close()
  imageTarget.close()
  return len(response['FaceMatches'])

def milestone1output(milestone3df, target_dir, extractedTerm,milestone1output_dirnew):

  # extracted_term = download_extracted_term_csv(dropbox_url, output_dir)
  input_file = "milestone7" + extractedTerm
  target_dir = "milestone7/"

  # extract_dropbox_url("dropbox/" + dirname, input_file)
  # This is where you enter source and target directories in createFrames()
  merge_df1 = createFrames(SourceDirectory=milestone1output_dirnew , TargetDirectory=target_dir)
  session = boto3.Session(
      aws_access_key_id = os.environ['aws_access_key_id'],
      aws_secret_access_key = os.environ['aws_secret_access_key'],
  )
  client = session.client('rekognition','us-east-1')

  results = []
  for index, row in merge_df1.iterrows():
    source_file = row['SourceimagePath']
    target_file = row['TargetimagePath']
    face_matches = compare_faces(source_file, target_file, client)
    results.append({'loginID': row['loginID'], 'verifiedID': face_matches})

  resultsdf = pd.DataFrame(results)
  
  resultsdf['loginID'] = resultsdf['loginID'].astype('int64')
  milestone1df = pd.merge(resultsdf,milestone3df, on = "loginID", how = "left")
  
  return milestone1df


