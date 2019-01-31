#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Several util classes used in the rest of the project
"""

# Includes
import os
import pandas as pd
import boto3
# ------------------------------------------------------------------------------------------------------------------- #


def create_dir(directory):

    """
    Creates a directory if it do not exists
    :param directory: The name of the directory
    """

    if not os.path.exists(directory):
        os.makedirs(directory)

#--# Download files from S3
def fileDownloader(Project, marker, ResultPath, GitPath):
  """
  Knowing the hash of result samples will be uploaded to S3
  :param Project   : project date
  :param marker    : marker asociated to study
  :param ResultPath: Folder where   
  :param GitPath   : Folder where is stoaged the filewith hash result
  """
  gitInfo = pd.read_csv(GitPath+'/output/links.csv', \
                        sep=',', \
                        header=0)
  for index, row in gitInfo.iterrows():
    Marker = row[0]
    S3path = row[1]
    ## Download mapped file
    s3 = boto3.resource('s3')
    try:
      s3.Bucket(S3path.split("/",3)[2]).download_file(S3path.split("/",3)[3], \
                ResultPath+'/'+Project+'/'+Marker+'_'+Project+'_mapped_reads_tax.biom')
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
      else:
        raise
