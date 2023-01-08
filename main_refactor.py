# This script ingests tweets from twitter API and loads them to BigQuery

# import packages
import tweepy
import configparser
import requests
import json
import os
import time

# define global variables to pass between functions
dataset_global = ''

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

   tweets = client.search_recent_tweets(
      query=query,
      # tweet_fields=['id','text'],
      max_results=10,
      user_auth=True
   )

   # function returns tweets
   return tweets

# create function to put tweets in jsonl file 
def to_file(filename, query):

   # convert result to dictionary (check this)
   tweets = get_tweets(query)
   tweets_dict_full = tweets.json() 
   tweets_dict = tweets_dict_full['data'] 

   # write result to a JSONL file
   with open(filename, "w") as f:
      for line in tweets_dict:
         f.write(json.dumps(line) + "\n") #Consider context management
   
   print("File created")

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

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    # Create dataset if none exists
    try:
        client.get_dataset(dataset_id) 
        print("Dataset exists")
    except NotFound:
        dataset = client.create_dataset(dataset, timeout=30)  
        print("Dataset created")

    return dataset

# Create table if none exists
def create_table(table_name, dataset_name):

   # Construct table reference
   dataset = create_dataset(dataset_name)
   table_ref = dataset.table(table_name)
   table = bigquery.Table(table_ref)

   # Check if table exists. If not, create table.
   try:
      client.get_table(table) #API request
      print("Table exists")
   except NotFound:
      table = client.create_table(table)
      print("Table created")

if __name__ == '__main__':
    get_tweets(query='analytics engineer #hiring')
    to_file(filename='refactor.jsonl', query='analytics engineer #hiring')
    create_dataset(dataset_name='tweets_dataset')
    create_table(table_name='tweets_table', dataset_name='tweets_dataset')