"""
In summary, this script extracts tweets from the Twitter API and loads them to a Bigquery table.

This script runs the following functions:
   get_tweets - Get list of Tweet objects from the Twitter API based on a search query
   write_tweets_to_jsonl - Load Tweets to a JSONL file
   create_bigquery_dataset - Create a BigQuery Dataset if it does not exist
   create_bigquery_table - Create a BigQuery Table if it does not exist
   load_jsonl_to_table - Load the BigQuery Table from the JSONL file

Last Updated: 2023-03
"""

# import packages
import tweepy
import configparser
import requests
import json
import os
import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Set google application credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='/Users/laurenkaye/PycharmProjects/tweets/apikey.json'

# Construct BigQuery client object
CLIENT = bigquery.Client() 

# Create function to get tweets
def get_tweets(query: str) -> list[dict]:
   """Get a list of Tweet objects from the Twitter API
    
   Args:
        query: The search query to use to get tweets

   Returns:
        A list of Tweet objects, which are not yet processed
   """
   # Read configs
   config: configparser.ConfigParser = configparser.ConfigParser()
   config.read('config.ini')

   # Set up authentication
   api_key: str = config['twitter']['api_key']
   api_key_secret: str = config['twitter']['api_key_secret']
   access_token: str = config['twitter']['access_token']
   access_token_secret: str = config['twitter']['access_token_secret']

   tweepy_client = tweepy.Client(
      consumer_key = api_key,
      consumer_secret = api_key_secret,
      access_token = access_token,
      access_token_secret = access_token_secret,
      return_type = requests.Response 
   )

   tweets_raw = tweepy_client.search_recent_tweets(
      query=query,
      tweet_fields=['id','text','created_at'], 
      max_results=10,
      user_auth=True
   )

   return tweets_raw

# Create function to write tweets to jsonl file 
def write_tweets_to_jsonl(filename: str, query: str) -> None:
   """Load Tweet objects to a JSONL file
    
   Args:
        filename: The name of the JSONL file to write to
        query: The search query to use to get tweets
            
   """ 
   # Convert tweets to dictionary and remove metadata
   tweets_raw = get_tweets(query) #datatype <class 'requests.models.Response'>
   tweets_dict = tweets_raw.json() #datatype <class 'dict'>
   tweets = tweets_dict['data'] #datatype <class 'list'>

   # Write result to a JSONL file
   with open(filename, 'w') as f:
      for line in tweets:
         f.write(json.dumps(line) + '\n') 
   
   print(f'File updated: {filename}')

# Create function to create BigQuery dataset if none exists
def create_bigquery_dataset(dataset_name: str) -> bigquery.Dataset:
   """Create the BigQuery Dataset if it does not already exist
    
    Args:
        dataset_name: The name of the dataset to create
        
    Returns:
        A Dataset Object
    """
   # Set dataset_id to the ID of the dataset to create.
   dataset_id: str = f'{CLIENT.project}.{dataset_name}'

   # Construct a BigQuery Dataset object to send to the API.
   dataset: bigquery.Dataset = bigquery.Dataset(dataset_id)
   dataset.location: str = 'US'

   # Create BigQuery dataset if none exists
   try:
        CLIENT.get_dataset(dataset_id) 
   except NotFound:
        dataset: bigquery.Dataset = CLIENT.create_dataset(dataset, timeout=30)  
        print(f'Dataset created: {dataset_id}')

   return dataset

# Create function to create BigQuery table if none exists
def create_bigquery_table(table_name: str, dataset_name: str) -> bigquery.Table:
   """Create the BigQuery Table if it does not already exist
    
    Args:
        table_name: The name of the table to create
        dataset_name: The name of the dataset containing the table
   """
   # Construct BigQuery table reference
   dataset: bigquery.Dataset = create_bigquery_dataset(dataset_name)
   table_id: bigquery.TableReference = dataset.table(table_name)
   table: bigquery.Table = bigquery.Table(table_id)

   # Check if BigQuery table exists. If not, create table.
   try:
      CLIENT.get_table(table) #API request
   except NotFound:
      table: bigquery.Table = CLIENT.create_table(table)
      print(f'Table created: {table}')

# Create function to load BigQuery table from JSONL
def load_jsonl_to_table(table_name: str, dataset_name: str, filename: str) -> None:
   """Load Tweets in the JSONL file to the BigQuery Table
    
    Args:
        dataset_name: The name of the dataset containing the table
        table_name: The name of the table to create
        filename: JSONL file containing the tweets
    """
   # Construct BigQuery table reference
   dataset: bigquery.Dataset = create_bigquery_dataset(dataset_name)
   table_id: bigquery.TableReference = dataset.table(table_name)

   # Define BigQuery table schema
   job_config = bigquery.LoadJobConfig(
      autodetect=True,
      source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
      write_disposition='WRITE_TRUNCATE' # {WRITE_APPEND; WRITE_EMPTY}
   )

   # Upload JSONL to BigQuery table
   with open(filename, 'rb') as file:
      job = CLIENT.load_table_from_file(file, table_id, job_config=job_config)

   while job.state != 'DONE':
      job.reload()
      time.sleep(2)
   print(f'Job completed: {job.result()}')

if __name__ == '__main__':
    
    # Set query that defines tweet search query. (requires experimentation)
    #query = '("BI developer" OR "BI engineer" OR "ETL" OR "ELT" OR "data engineer" -senior -lead -sr OR "Business Intelligence" OR Analytics) (interim OR #interim OR contractor OR #contractor OR contract OR #contract OR freelance OR #freelance OR #freelancer OR parttime OR part-time OR "part time" OR #parttime OR #part-time OR flexible OR #flexible OR months OR hours) (context:131.1197909704803901440 OR #hiring) -is:retweet'
    query: str = '"analytics engineer" #hiring -is:retweet'
  

    # Set landing locations for data from the Twitter API
    filename: str = 'tweets.jsonl'
    dataset_name: str = 'tweets_dataset' 
    table_name: str = 'raw_twitter_jobs'

   # Run functions that extract tweets from the Twitter API and loads them to Bigquery
    get_tweets(query)
    write_tweets_to_jsonl(filename, query)
    create_bigquery_dataset(dataset_name)
    create_bigquery_table(table_name, dataset_name)
    load_jsonl_to_table(table_name, dataset_name, filename)





