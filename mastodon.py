'''
This script extracts toots from the Mastodon API and loads them to Bigquery.

This script runs the following functions:
    get_mastodon_toots - Get Toot objects from the Mastodon API based on a hashtag and optional keyword
    parse_mastodon_toots - Format Toot objects as JSON and extract fields of interest
    write_toots_to_jsonl - Load toots to a JSONL file
    create_bigquery_dataset - Create a BigQuery Dataset if it does not exist
    create_bigquery_table - Create a BigQuery Table if it does not exist
    load_jsonl_to_table - Load the BigQuery Table from the JSONL file

Last Updated: 2023-02
'''

# import packages
import requests
from requests.exceptions import ConnectionError, HTTPError, Timeout, TooManyRedirects, RequestException
import json
import configparser
import os
import time
from bs4 import BeautifulSoup
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Set google application credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/laurenkaye/PycharmProjects/tweets/apikey.json"

# construct BigQuery client object
CLIENT: bigquery.Client = bigquery.Client() 

# create config object and read config file
CONFIG: configparser.ConfigParser = configparser.ConfigParser() 
CONFIG.read('CONFIG.ini') 

# create function to get toots
def get_mastodon_toots(hashtag: str, keyword: list) -> list[dict]:
    '''Get Toot objects from the Mastodon API based on a hashtag and optional keywords
    
    Returns:
        Toot objects from the Mastodon API. 

    Raises:
        Raises an exception if the response status code is not 200.
        Errors raised include: ConnectionError, HTTPError, Timeout, TooManyRedirects, RequestException
    '''
    
    # Make GET request to the API endpoint
    auth: dict = {'Authorization': f"Bearer {CONFIG['mastodon']['user_key']}"} 
    url: str = f'https://data-folks.masto.host//api/v1/timelines/tag/:{hashtag}' 
    params: dict = {'all':keyword, 'limit': 20}

    try:
        response: requests.models.Response = requests.get(url, data=params, headers=auth)
        response.raise_for_status() # raise exception if status code is not 200

        return response

    # If an exception is raised, print the error message
    except ConnectionError as ce:
        print("Error connecting to server:", ce)
    except HTTPError as he:
        print("HTTP error occurred:", he)
    except Timeout as te:
        print("Timeout error occurred:", te)
    except TooManyRedirects as tme:
        print("Too many redirects occurred:", tme)
    except RequestException as re:
        print("An error occurred: ", re)          

# parse mastodon toots to json format
def parse_mastodon_toots():
    '''Format Toot objects as JSON, do light text cleaning, and extract fields of interest. 
    
    Returns:
    A list of Toot objects containing the hashtag and keywords
    '''

    # Convert response to JSON
    data: dict = get_mastodon_toots(hashtag, keyword).json()

    class Toot():
        # constructor
        def __init__(self, id, url, created_at, content, acct):
            self.id = id
            self.url = url
            self.created_at = created_at
            self.content = BeautifulSoup(content,'html.parser').get_text().replace('"','\\"') #remove html tags and escape double quotes
            self.acct = acct    

    # Extract Toot objects from data using a list comprehension
    extract_toots: list[Toot] = [Toot(idx['id'], idx['url'], idx['created_at'], idx['content'], idx['account']['acct']) for idx in data] 

    # Format Toot objects as JSON using a list comprehension
    toots: list[str] = [f'{{"id": "{toot.id}", "url": "{toot.url}", "created_at": "{toot.created_at}", "content": "{toot.content}", "acct": "{toot.acct}"}}' for toot in extract_toots]

    return toots

# create function to put toots in jsonl file
def write_toots_to_jsonl(filename: str):
    '''Write mastodon toots to a JSONL file
    
    Args:
        filename: The name of the JSONL file to write to
        
    '''    
    # write result to file
    with open(filename, "w") as f:
        for line in parse_mastodon_toots():
            f.write(line + "\n") 

    print(f"File updated: {filename}")

# Create dataset if none exists
def create_bigquery_dataset(dataset_name: str) -> bigquery.Dataset:
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
    dataset.location: str = "US"

    # Create dataset if none exists
    try:
        CLIENT.get_dataset(dataset_id) 
    except NotFound:
        dataset = CLIENT.create_dataset(dataset, timeout=30)  
        print(f"Dataset created: {dataset_id}")

    return dataset

# Create table if none exists
def create_bigquery_table(table_name: str, dataset_name: str) -> bigquery.Table:
    '''Create the BigQuery Table if it does not already exist
    
    Args:
        dataset_name: The name of the dataset containing the table
        table_name: The name of the table to create
    '''
   # Construct table reference
    dataset: bigquery.dataset.Dataset = create_bigquery_dataset(dataset_name)
    table_id: bigquery.table.TableReference = dataset.table(table_name)
    table: bigquery.table.Table = bigquery.Table(table_id)

   # Check if table exists. If not, create table.
    try:
      CLIENT.get_table(table) #API request
    except NotFound:
      table = CLIENT.create_table(table)
      print(f"Table created: {table}")

# Load table from JSONL
def load_jsonl_to_table(table_name: str, dataset_name: str, filename: str) -> None:
    '''Load Mastodon Toots in the JSONL file to the BigQuery Table
    
    Args:
        dataset_name: The name of the dataset containing the table
        table_name: The name of the table to create
        filename: JSONL file containing the toots
    '''
   # Construct table reference
    dataset = create_bigquery_dataset(dataset_name)
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

    # Wait for the job to complete
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
    get_mastodon_toots(hashtag, keyword)
    parse_mastodon_toots()
    write_toots_to_jsonl(filename)
    create_bigquery_dataset(dataset_name)
    create_bigquery_table(table_name, dataset_name)
    load_jsonl_to_table(table_name, dataset_name, filename)








