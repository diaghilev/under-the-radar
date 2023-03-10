"""
In summary, this script extracts toots from the Mastodon API and loads them to a Bigquery table.

This script runs the following functions:
    get_mastodon_toots - Get array of Toot objects from the Mastodon API based on a hashtag and optional keyword
    parse_mastodon_toots - Extract desired fields from array of Toot objects and format as JSON
    write_toots_to_jsonl - Write jsonified toots to a JSONL file
    create_bigquery_dataset - Create a BigQuery Dataset if it does not exist
    create_bigquery_table - Create a BigQuery Table if it does not exist
    load_jsonl_to_table - Load the BigQuery Table from the JSONL file

Last Updated: 2023-03
"""

# Import packages
import os
import configparser
import requests
import time
from bs4 import BeautifulSoup
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Set google application credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='/Users/laurenkaye/PycharmProjects/tweets/apikey.json'

# Construct BigQuery client object
CLIENT: bigquery.Client = bigquery.Client() 

# Create config object and read config file
CONFIG: configparser.ConfigParser = configparser.ConfigParser() 
CONFIG.read('CONFIG.ini') 

# Create function to get toots from mastodon API
def get_mastodon_toots(hashtag: str, keyword: list) -> list[dict]:
    """Get array of Toot objects from the Mastodon API based on a hashtag and optional keyword
    
    Returns:
        Array of Toot objects from the Mastodon API. 

    Raises:
        Raises an exception if the response status code is not 200.
        Errors raised include: ConnectionError, HTTPError, Timeout, TooManyRedirects, RequestException
    """
    
    # Make GET request to the API endpoint
    auth: dict = {'Authorization': f"Bearer {CONFIG['mastodon']['user_key']}"} 
    url: str = f'https://data-folks.masto.host//api/v1/timelines/tag/:{hashtag}' 
    params: dict = {'all':keyword, 'limit': 20}

    try:
        api_response: requests.models.Response = requests.get(url, data=params, headers=auth)
        api_response.raise_for_status() # raise exception if status code is not 200

        return api_response

    # If an exception is raised, print the error message
    except requests.exceptions.ConnectionError as ce:
        print('Error connecting to server:', ce)
    except requests.exceptions.HTTPError as he:
        print('HTTP error occurred:', he)
    except requests.exceptions.Timeout as te:
        print('Timeout error occurred:', te)
    except requests.exceptions.TooManyRedirects as tme:
        print('Too many redirects occurred:', tme)
    except requests.exceptions.RequestException as re:
        print('An error occurred: ', re)          

# Create function to parse mastodon toots
def parse_mastodon_toots(toots: requests.Response) -> list[str]:
    """Extract desired fields from array of Toot objects, do light text cleaning, and format as JSON.
    
    Returns:
    A list of Toot objects in JSON format.
    """

    # Convert response to JSON
    toots_response_json: dict = toots.json()

    class TootClass():
        # constructor
        def __init__(self, id: int, url: str, created_at: str , content: str, acct: str):
            self.id = id
            self.url = url
            self.created_at = created_at
            self.content = BeautifulSoup(content,'html.parser').get_text().replace('"','\\"') #remove html tags and escape double quotes
            self.acct = acct    

    # Extract Toot objects from data using a list comprehension
    parsed_toots: list[TootClass] = [TootClass(idx['id'], idx['url'], idx['created_at'], idx['content'], idx['account']['acct']) for idx in toots_response_json] 

    # Format Toot objects as JSON using a list comprehension
    parsed_toots_json: list[str] = [f'{{"id": {toot.id}, "url": "{toot.url}", "created_at": "{toot.created_at}", "content": "{toot.content}", "acct": "{toot.acct}"}}' for toot in parsed_toots]

    return parsed_toots_json

# Create function to put toots in jsonl file
def write_toots_to_jsonl(toots_json: list[str], filename: str) -> None:
    """Write jsonified toots to a JSONL file.
    
    Args:
        filename: The name of the JSONL file to write to
        
    """    
    # Write result to file
    with open(filename, 'w') as f:
        for line in toots_json:
            f.write(line + '\n') 

    print(f'File updated: {filename}')

# Create function to create dataset if none exists
def create_bigquery_dataset(dataset_name: str) -> bigquery.Dataset:
    """Create the BigQuery Dataset if it does not already exist
    
    Args:
        dataset_name: The name of the dataset to create
        
    Returns:
        A Dataset Object
    """
    # Set dataset_id to the ID of the dataset to create.
    dataset_id: str = f"{CLIENT.project}.{dataset_name}"

    # Construct a Dataset object to send to the API.
    dataset: bigquery.Dataset = bigquery.Dataset(dataset_id)
    dataset.location: str = 'US'

    # Create dataset if none exists
    try:
        CLIENT.get_dataset(dataset_id) 
    except NotFound:
        dataset: bigquery.Dataset = CLIENT.create_dataset(dataset, timeout=30)  
        print(f'Dataset created: {dataset_id}')

    return dataset

# Create function to create table if none exists
def create_bigquery_table(table_name: str, dataset_name: str) -> bigquery.Table:
    """Create the BigQuery Table if it does not already exist
    
    Args:
        dataset_name: The name of the dataset containing the table
        table_name: The name of the table to create
    """
   # Construct table reference
    dataset: bigquery.Dataset = create_bigquery_dataset(dataset_name)
    table_id: bigquery.TableReference = dataset.table(table_name)
    table: bigquery.Table = bigquery.Table(table_id)

   # Check if table exists. If not, create table.
    try:
      CLIENT.get_table(table) #API request
    except NotFound:
      table: bigquery.Table = CLIENT.create_table(table)
      print(f'Table created: {table}')

# Create function to load table from JSONL
def load_jsonl_to_table(table_name: str, dataset_name: str, filename: str) -> None:
    """Load Mastodon Toots in the JSONL file to the BigQuery Table
    
    Args:
        dataset_name: The name of the dataset containing the table
        table_name: The name of the table to create
        filename: JSONL file containing the toots
    """
   # Construct table reference
    dataset: bigquery.Dataset = create_bigquery_dataset(dataset_name)
    table_id: bigquery.Table = dataset.table(table_name)

   # Define table schema
    job_config = bigquery.LoadJobConfig(
      autodetect=True,
      source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
      write_disposition='WRITE_TRUNCATE' # Alternatively: {WRITE_APPEND; WRITE_EMPTY}
   )

   # Upload JSONL to BigQuery
    with open(filename, 'rb') as file:
      job = CLIENT.load_table_from_file(file, table_id, job_config=job_config)

    # Wait for the job to complete
    while job.state != 'DONE':
      job.reload()
      time.sleep(2)
    print(f'Job completed: {job.result()}')

if __name__ == '__main__':

    # Set hashtag and optional keywords to search Mastodon toots for
    hashtag: str = 'hiring'
    keyword: list = ['data'] #if no keyword is desired, set an empty string

    # Set data landing locations
    filename: str = 'mastodon.jsonl'
    dataset_name: str = 'tweets_dataset'
    table_name: str = 'raw_mastodon_jobs'

    # Run functions  
    toots: requests.Response = get_mastodon_toots(hashtag, keyword)   
    toots_json: list[str] = parse_mastodon_toots(toots)
    write_toots_to_jsonl(toots_json, filename)
    create_bigquery_dataset(dataset_name)
    create_bigquery_table(table_name, dataset_name)
    load_jsonl_to_table(table_name, dataset_name, filename)








