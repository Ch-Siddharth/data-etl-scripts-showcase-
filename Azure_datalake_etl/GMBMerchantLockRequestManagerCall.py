#!/usr/bin/env python
# coding: utf-8

# In[2]:

import os

from datetime import date, timedelta
import pymssql
import logging
import time
import csv
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv('secrets.env')

from presentation import fieldnames


def mysqlconnect():
    conn = pymssql.connect(
        os.getenv('AZURE_HOST'),
        os.getenv('AZURE_USERNAME'),
        os.getenv('AZURE_PASSWORD'),
        os.getenv('AZURE_DATABASE')
    )
    cursor = conn.cursor(as_dict=True)
    if(cursor):
        print("Database connection established", conn.cursor())
    toCSV = []
    try:
        cursor.execute("SELECT * FROM [DL_Gonukkad].[dbo].[GMBMerchantLockRequestManagerCall]")
        print('Query has been successfully executed')
    except Exception as e:
        print("Exception occurred", exc_info=True)
    toCSV = cursor.fetchall()
    count = 0
    return toCSV

def convert_boolean(value):
    return 1 if value else 0

new_directory = os.path.join(os.getcwd(), 'gon_req_mngr_call_responses')

if not os.path.exists(new_directory):
    os.makedirs(new_directory)

today = date.today()
yesterday = today - timedelta(days=1)
today_filename = today.strftime("%Y-%m-%d")+".csv"
yesterday_filename = 'GMBMerchantLockRequestManagerCall_'+yesterday.strftime("%Y-%m-%d")+".csv"
rows = mysqlconnect()
csv_file_path = new_directory+'/'+yesterday_filename
##csv_file_path = '/home/siddharth/Downloads/'+yesterday_filename
fieldnames = list(rows[0].keys())
# fieldnames = ['PK_GMBMerchantLockRequestManagerCallID','MerchantID','RequestManagerNo','Type','CreatedDateTime']
with open(csv_file_path, mode='w', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    # Write the header row with column names
    writer.writeheader()

    # Write the data rows
    for row in rows:
        cleaned_row = {key: convert_boolean(value) if isinstance(value, bool) else value for key, value in row.items()}
        writer.writerow(cleaned_row)

#directory_path = '/home/siddharth/Downloads/'+yesterday_filename
storage_client = storage.Client.from_service_account_json(
    'path_to_service_account.json')

def upload_to_bucket(blob_name, path_to_file, bucket_name):

    bucket = storage_client.get_bucket(bucket_name)
    print(bucket)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)

    folder_name = 'live/GMBMerchantLockRequestManagerCall/'

    files = bucket.list_blobs(prefix=folder_name)
    for file in files:
        if not file.name.endswith('/'):
            if not yesterday_filename in file.name:
                if '.' in file.name:
                    new_name = file.name.replace('.csv','')
                    print(new_name)
                    bucket.rename_blob(file,new_name)

upload_to_bucket('live/GMBMerchantLockRequestManagerCall/'+yesterday_filename,csv_file_path,'bucket_name')

#Renaming files name inside bucket on cloud

time.sleep(10)

#Data push into permanent table
project_id = os.getenv('GCP_PROJECT_ID')
credentials = service_account.Credentials.from_service_account_file('path_to_service_account.json')
bq_client = bigquery.Client(credentials=credentials,project=project_id)

source_table = 'gonukkad.temp_Live_GMB_Merchant_Lock_Request_Manager_Call'
destination_table = 'gonukkad.Live_GMB_Merchant_Lock_Request_Manager_Call'

sql_query = f'''
    CREATE OR REPLACE TABLE `{destination_table}`
    AS
    SELECT *
    FROM `{source_table}`
'''

# Run the query
query_job = bq_client.query(sql_query)

# Wait for the query to complete
query_job.result()

print(f'Table "{destination_table}" created or replaced successfully.')
# In[ ]: