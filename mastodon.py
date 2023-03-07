'''
This script extracts toots from the Mastodon API and loads them to Bigquery.

This script runs the following functions
    get_toots - Get list of Toot objects from the Mastodon API based on a hashtag and optional keyword
    to_file - Load Toots to a JSONL file
    create_dataset - Create a BigQuery Dataset if it does not exist
    create_table - Create a BigQuery Table if it does not exist
    load_table - Load the BigQuery Table from the JSONL file

Last Updated: 2023-02
'''

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

# create function to get toots
def get_toots(hashtag: str, keyword: list) -> list[dict]:
    '''Get a list of Toot objects from the Mastodon API based on a hashtag and optional keywords
    
    Returns:
        A list of Toot objects containing the hashtag and keywords
    '''
    
    # Make GET request to the API endpoint
    auth: dict = {'Authorization': f"Bearer {CONFIG['mastodon']['user_key']}"} 
    url: str = f'https://data-folks.masto.host//api/v1/timelines/tag/:{hashtag}' 
    params: dict = {'all':keyword, 'limit': 20}

    response = requests.get(url, data=params, headers=auth)

    # Check the response status code
    if response.status_code == 200:
        
        # Convert response to JSON
        data = response.json()

        class Toot():
            # constructor
            def __init__(self, id, url, created_at, content, acct):
                self.id = id
                self.url = url
                self.created_at = created_at
                self.content = BeautifulSoup(content,'html.parser').get_text().replace('"','\\"') #remove html tags and escape double quotes
                self.acct = acct    

        # Extract toots from data
        extract_toots: list[Toot] = [Toot(idx['id'], idx['url'], idx['created_at'], idx['content'], idx['account']['acct']) for idx in data] 

        toots: list = []

        for toot in extract_toots:
            entry: str = f'{{"id": "{toot.id}", "url": "{toot.url}", "created_at": "{toot.created_at}", "content": "{toot.content}", "acct": "{toot.acct}"}}'
            toots.append(entry)

        # Experimentation ends here
        return toots

    # If the response fails, print the status code and reason
    else:
        print(f"Error: {response.status_code} - {response.reason}")

# create function to put toots in jsonl file
def to_file(filename: str):
    '''Load mastodon toots to a JSONL file
    
    Args:
        filename: The name of the JSONL file to write to
        
    '''    
    # write result to file
    with open(filename, "w") as f:
        for line in get_toots(hashtag, keyword):
            f.write(line + "\n") 

    print(f"File updated: {filename}")

# Create dataset if none exists
def create_dataset(dataset_name: str) -> bigquery.Dataset:
    '''Create the BigQuery Dataset if it does not already exist
    
    Args:
        dataset_name: The name of the dataset to create
        
    Returns:
        A Dataset Object
    '''
    # Set dataset_id to the ID of the dataset to create.
    dataset_id: str = f"{CLIENT.project}.{dataset_name}"

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
def create_table(table_name: str, dataset_name: str):
    '''Create the BigQuery Table if it does not already exist
    
    Args:
        dataset_name: The name of the dataset containing the table
        table_name: The name of the table to create
    '''
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
def load_table(table_name: str, dataset_name: str, filename: str):
    '''Load Mastodon Toots in the JSONL file to the BigQuery Table
    
    Args:
        dataset_name: The name of the dataset containing the table
        table_name: The name of the table to create
        filename: JSONL file containing the toots
    '''
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

    # Set hashtag and optional keywords to search Mastodon toots for
    hashtag: str = 'hiring'
    keyword: list = ['data'] #if no keyword is desired, set an empty string

    # set data landing locations
    filename: str = 'mastodon.jsonl'
    dataset_name: str = 'tweets_dataset'
    table_name: str = 'raw_mastodon_jobs'

    # run functions
    get_toots(hashtag, keyword)
    to_file(filename)
    create_dataset(dataset_name)
    create_table(table_name, dataset_name)
    load_table(table_name, dataset_name, filename)







