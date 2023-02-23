# This script extracts posts (officially called 'statuses' or 'toots') from the Mastodon API and loads them to Bigquery

# import packages
import requests
import json
import configparser
import os
import time
from bs4 import BeautifulSoup
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Set google application credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/laurenkaye/PycharmProjects/tweets/apikey.json"

# Construct BigQuery client object
CLIENT = bigquery.Client() 

# read configs
CONFIG = configparser.ConfigParser()
CONFIG.read('CONFIG.ini')

# create function to get statuses
def get_posts():

    # Make GET request to the API endpoint
    auth = {'Authorization': f"Bearer {CONFIG['mastodon']['user_key']}"} 
    url = 'https://data-folks.masto.host//api/v1/timelines/tag/:hiring' # API endpoint
    params = {'all':['data'], 'limit': 20}

    response = requests.get(url, data=params, headers=auth)

    # Check the response status code
    if response.status_code == 200:
        
        # Convert response to JSON
        data = response.json()

        #Extract posts from the JSON
        posts = []

        for idx, item in enumerate(data):
            id = data[idx]['id']
            url = data[idx]['url']
            created_at = data[idx]['created_at']
            content_raw = data[idx]['content']
            acct = data[idx]['account']['acct']

            # Remove HTML from content of the post
            soup = BeautifulSoup(content_raw,'html.parser')
            content = soup.get_text()

            post = {
            'id': id,
            'created_at': created_at,
            'content': content,
            'url': url,
            'acct': acct
            }

            posts.append(post)

        return posts

    # If request fails, show the error
    else:
        print("Request failed with status code:", response.status_code)

# create function to put posts in jsonl file
def to_file(filename):
    
    # write result to file
    with open(filename, "w") as f:
        for line in get_posts():
            f.write(json.dumps(line) + "\n") 

    print(f"File updated: {filename}")

# Create dataset if none exists
def create_dataset(dataset_name):

    # Set dataset_id to the ID of the dataset to create.
    dataset_id = f"{CLIENT.project}.{dataset_name}"

    # Construct a Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    # Create dataset if none exists
    try:
        CLIENT.get_dataset(dataset_id) 
    except NotFound:
        dataset = CLIENT.create_dataset(dataset, timeout=30)  
        print(f"Dataset created: {dataset_id}")

    return dataset

# Create table if none exists
def create_table(table_name, dataset_name):

   # Construct table reference
   dataset = create_dataset(dataset_name)
   table_id = dataset.table(table_name)
   table = bigquery.Table(table_id)

   # Check if table exists. If not, create table.
   try:
      CLIENT.get_table(table) #API request
   except NotFound:
      table = CLIENT.create_table(table)
      print(f"Table created: {table}")

# Load table from JSONL
def load_table(table_name, dataset_name, filename):

   # Construct table reference
   dataset = create_dataset(dataset_name)
   table_id = dataset.table(table_name)

   # Define table schema
   job_config = bigquery.LoadJobConfig(
      autodetect=True,
      source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
      write_disposition='WRITE_TRUNCATE' # {WRITE_APPEND; WRITE_EMPTY}
   )

   # Upload JSONL to BigQuery
   with open(filename, "rb") as file:
      job = CLIENT.load_table_from_file(file, table_id, job_config=job_config)

   while job.state != 'DONE':
      job.reload()
      time.sleep(2)
   print(f"Job completed: {job.result()}")

if __name__ == '__main__':

    # set data landing locations
    filename = 'mastodon.jsonl'
    dataset_name = 'tweets_dataset'
    table_name = 'raw_mastodon_jobs'

    # run functions
    get_posts()
    to_file(filename)
    create_dataset(dataset_name)
    create_table(table_name, dataset_name)
    load_table(table_name, dataset_name, filename)







