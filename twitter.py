'''
This script extracts tweets from the Twitter API and loads them to Bigquery.

This script runs the following steps:
1 Get list of Tweet objects from the Twitter API based on a search query
2 Load Tweets to a JSONL file
3 Create a BigQuery Dataset if it does not exist
4 Create a BigQuery Table if it does not exist
5 Load the BigQuery Table from the JSONL file

Last Updated: 2023-02
'''

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
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/laurenkaye/PycharmProjects/tweets/apikey.json"

# Construct BigQuery client object
CLIENT = bigquery.Client() 

# create function to get tweets
def get_tweets(query: str):
   '''Get a list of Tweet objects from the Twitter API
    
   Args:
        query: The search query to use to get tweets

    Returns:
        A list of Tweet objects, which are not yet processed
    '''
   # read configs
   config = configparser.ConfigParser()
   config.read('config.ini')

   # set up authentication
   api_key = config['twitter']['api_key']
   api_key_secret = config['twitter']['api_key_secret']
   access_token = config['twitter']['access_token']
   access_token_secret = config['twitter']['access_token_secret']

   tweepy_client = tweepy.Client(
      consumer_key= api_key,
      consumer_secret= api_key_secret,
      access_token= access_token,
      access_token_secret= access_token_secret,
      return_type = requests.Response 
   )

   tweets_raw = tweepy_client.search_recent_tweets(
      query=query,
      tweet_fields=["id","text","created_at"], 
      max_results=100,
      user_auth=True
   )

   return tweets_raw

# create function to put tweets in jsonl file 
def to_file(filename: str, query: str):
   '''Load Tweet objects to a JSONL file
    
    Args:
        filename: The name of the JSONL file to write to
        query: The search query to use to get tweets
            
    ''' 
   # convert tweets to dictionary and remove metadata
   tweets_raw = get_tweets(query) #datatype <class 'requests.models.Response'>
   tweets_dict = tweets_raw.json() #datatype <class 'dict'>
   tweets = tweets_dict['data'] #datatype <class 'list'>

   # write result to a JSONL file
   with open(filename, "w") as f:
      for line in tweets:
         f.write(json.dumps(line) + "\n") 
   
   print(f"File updated: {filename}")

# Create dataset if none exists
def create_dataset(dataset_name: str):
   '''Create the BigQuery Dataset if it does not already exist
    
    Args:
        dataset_name: The name of the dataset to create
        
    Returns:
        A Dataset Object
    '''
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
def create_table(table_name: str, dataset_name: str):
   '''Create the BigQuery Table if it does not already exist
    
    Args:
        table_name: The name of the table to create
        dataset_name: The name of the dataset containing the table
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
   '''Load Tweets in the JSONL file to the BigQuery Table
    
    Args:
        dataset_name: The name of the dataset containing the table
        table_name: The name of the table to create
        filename: JSONL file containing the tweets
    '''
   # Construct table reference
   dataset = create_dataset(dataset_name)
   table_id = dataset.table(table_name)

   # Define table schema
   job_config = bigquery.LoadJobConfig(
      autodetect=True,
      source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
      write_disposition='WRITE_APPEND' # {WRITE_TRUNCATE; WRITE_EMPTY}
   )

   # Upload JSONL to BigQuery
   with open(filename, "rb") as file:
      job = CLIENT.load_table_from_file(file, table_id, job_config=job_config)

   while job.state != 'DONE':
      job.reload()
      time.sleep(2)
   print(f"Job completed: {job.result()}")

if __name__ == '__main__':
    
    # set twitter query (requires experimentation)
    query = '("BI developer" OR "BI engineer" OR "ETL" OR "ELT" OR "data engineer" -senior -lead -sr OR "Business Intelligence" OR Analytics) (interim OR #interim OR contractor OR #contractor OR contract OR #contract OR freelance OR #freelance OR #freelancer OR parttime OR part-time OR "part time" OR #parttime OR #part-time OR flexible OR #flexible OR months OR hours) (context:131.1197909704803901440 OR #hiring) -is:retweet'
    
    # set data landing locations
    filename = 'tweets.jsonl'
    dataset_name = 'tweets_dataset' 
    table_name = 'raw_twitter_jobs'

   #finally, run functions
    get_tweets(query)
    to_file(filename, query)
    create_dataset(dataset_name)
    create_table(table_name, dataset_name)
    load_table(table_name, dataset_name, filename)





