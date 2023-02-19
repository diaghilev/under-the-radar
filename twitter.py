# This script ingests tweets from twitter API and loads them to BigQuery

# import packages
import tweepy
import configparser
import requests
import json
import os
import time

# create function to get tweets
def get_tweets(query):

   # read configs
   config = configparser.ConfigParser()
   config.read('config.ini')

   # set up authentication
   api_key = config['twitter']['api_key']
   api_key_secret = config['twitter']['api_key_secret']
   access_token = config['twitter']['access_token']
   access_token_secret = config['twitter']['access_token_secret']

   client = tweepy.Client(
      consumer_key= api_key,
      consumer_secret= api_key_secret,
      access_token= access_token,
      access_token_secret= access_token_secret,
      return_type = requests.Response 
   )

   tweets_raw = client.search_recent_tweets(
      query=query,
      tweet_fields=["id","text","created_at"], #Add entities later to get urls, leads to nested json
      max_results=100,
      user_auth=True
   )

   return tweets_raw

# create function to put tweets in jsonl file 
def to_file(filename, query):

   # convert tweets to dictionary and remove metadata
   tweets_raw = get_tweets(query) #datatype is <class 'requests.models.Response'>
   tweets_dict = tweets_raw.json() #datatype is <class 'dict'>
   tweets = tweets_dict['data'] #datatype is <class 'list'>

   # write result to a JSONL file
   with open(filename, "w") as f:
      for line in tweets:
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
      # schema=[
      #    bigquery.SchemaField("edit_history_tweet_ids", "INT64"),
      #    bigquery.SchemaField("id", "INT64"),
      #    bigquery.SchemaField("text", "STRING"),
      # ],
      source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
      write_disposition='WRITE_APPEND' # {WRITE_TRUNCATE; WRITE_EMPTY}
   )

   # Upload JSONL to BigQuery
   with open(filename, "rb") as file:
      job = client.load_table_from_file(file, table_id, job_config=job_config)

   while job.state != 'DONE':
      job.reload()
      time.sleep(2)
   print("Job completed: {}".format(job.result()))

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





