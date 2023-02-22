# This script extracts posts (officially called 'statuses' or 'toots') from the Mastodon API and loads them to Bigquery

# import packages
import requests
import json
import configparser
import os
import time
from bs4 import BeautifulSoup

from pprint import pprint

# read configs
config = configparser.ConfigParser()
config.read('config.ini')

# create function to get statuses
def get_posts():

    # Make GET request to the API endpoint
    auth = {'Authorization': f"Bearer {config['mastodon']['user_key']}"} 
    url = 'https://data-folks.masto.host//api/v1/timelines/tag/:hiring' # API endpoint
    params = {'all':['data'], 'limit': 20}

    response = requests.get(url, data=params, headers=auth)

    # Check the response status code
    if response.status_code == 200:
        
        # Convert response to JSON
        data = response.json()

        #Extract statuses from the JSON
        posts = []

        for idx, item in enumerate(data):
            id = data[idx]['id']
            url = data[idx]['url']
            created_at = data[idx]['created_at']
            content_raw = data[idx]['content']
            acct = data[idx]['account']['acct']

            # Remove HTML from text
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

    print("File updated: {}".format(filename))

# Import google cloud packages
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Set google application credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/laurenkaye/PycharmProjects/tweets/apikey.json"

# Construct BigQuery client object
client = bigquery.Client() 

# Create dataset if none exists
def create_dataset(dataset_name):

    # Set dataset_id to the ID of the dataset to create.
    dataset_id = "{}.{}".format(client.project, dataset_name)

    # Construct a Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    # Create dataset if none exists
    try:
        client.get_dataset(dataset_id) 
        #print("Dataset exists: {}".format(dataset_id))
    except NotFound:
        dataset = client.create_dataset(dataset, timeout=30)  
        print("Dataset created: {}".format(dataset_id))

    return dataset

# Create table if none exists
def create_table(table_name, dataset_name):

   # Construct table reference
   dataset = create_dataset(dataset_name)
   table_id = dataset.table(table_name)
   table = bigquery.Table(table_id)

   # Check if table exists. If not, create table.
   try:
      client.get_table(table) #API request
   except NotFound:
      table = client.create_table(table)
      print("Table created: {}".format(table))

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
      job = client.load_table_from_file(file, table_id, job_config=job_config)

   while job.state != 'DONE':
      job.reload()
      time.sleep(2)
   print("Job completed: {}".format(job.result()))

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







