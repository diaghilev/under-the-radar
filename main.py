# This script will access twitter API to pull tweets based on a query

# import packages
import tweepy
import configparser
import requests
import json
import os

# packages up for deletion
import pandas as pd
import pprint
import time

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
   return_type = requests.Response #may be potential to just return a dict straight away
)

# define query
query = 'analytics engineer #hiring' #query = 'context:66.961961812492148736 lang:en #hiring #remote data engineer'

tweets = client.search_recent_tweets(
   query=query,
   #tweet_fields=['author_id','created_at','geo','lang','context_annotations'],
   max_results=10,
   user_auth=True
)

# convert result to dictionary
tweets_dict_full = tweets.json() 
tweets_dict = tweets_dict_full['data'] #extract "data" value from dict
pprint.pprint(tweets_dict[0])

# write to a JSONL file
with open("tweets.jsonl", "w") as f:
   for line in tweets_dict:
      f.write(json.dumps(line) + "\n")

#####

# import google cloud packages
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Set google application credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/laurenkaye/PycharmProjects/tweets/apikey.json"

# Construct BigQuery client object
client = bigquery.Client

bq_project_id = 'modular-terra-372321'
bq_dataset = 'tweets_dataset'

exit()

#(BigQuery official docs)

# Set dataset_id to the ID of the dataset to create.
dataset_id = {}.bq_dataset.format(client.project)

# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(dataset_id)

# Specify the geographic location where the dataset should reside.
dataset.location = "US"

# Send the dataset to the API for creation, with an explicit timeout.
# Raises google.api_core.exceptions.Conflict if the Dataset already exists within the project.
dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


# create table in bigquery 
# set schema

# load file to BigQuery


