# This script ingests tweets from twitter API and loads them to BigQuery

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

# # read configs
# config = configparser.ConfigParser()
# config.read('config.ini')

# # set up authentication
# api_key = config['twitter']['api_key']
# api_key_secret = config['twitter']['api_key_secret']
# access_token = config['twitter']['access_token']
# access_token_secret = config['twitter']['access_token_secret']

# client = tweepy.Client(
#    consumer_key= api_key,
#    consumer_secret= api_key_secret,
#    access_token= access_token,
#    access_token_secret= access_token_secret,
#    return_type = requests.Response #may be potential to just return a dict straight away
# )

# # define query
# query = 'analytics engineer #hiring' #query = 'context:66.961961812492148736 lang:en #hiring #remote data engineer'

# tweets = client.search_recent_tweets(
#    query=query,
#    #tweet_fields=['author_id','created_at','geo','lang','context_annotations'],
#    max_results=10,
#    user_auth=True
# )

# # convert result to dictionary
# tweets_dict_full = tweets.json() 
# tweets_dict = tweets_dict_full['data'] #extract "data" value from dict

# # write to a JSONL file
# with open("tweets.jsonl", "w") as f:
#    for line in tweets_dict:
#       f.write(json.dumps(line) + "\n")

#####

# Import google cloud packages
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Set google application credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/laurenkaye/PycharmProjects/tweets/apikey.json"

# Construct BigQuery client object
client = bigquery.Client() 

# Set dataset_id to the ID of the dataset to create.
dataset_id = "{}.tweets_dataset".format(client.project)

# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(dataset_id)

# Specify the geographic location where the dataset should reside.
dataset.location = "US"

# Create dataset if none exists
def create_dataset():

   # Check if dataset exists. If not, send the dataset to the API for creation 
   try:
      client.get_dataset(dataset_id) #API request
      print("Dataset exists")
   except NotFound:
      dataset = client.create_dataset(dataset, timeout=30)  # API request.
      print("Dataset created")

# Create table if none exists
def create_table():

   table_ref = dataset.table("tweets_table")
   table = bigquery.Table(table_ref)

   # Check if table exists. If not, create table
   try:
      client.get_table(table) #API request
      print("Table exists")
   except NotFound:
      table = client.create_table(table)
      print("Table created")

if __name__ == '__main__':
    create_dataset()
    create_table()

# create table in bigquery 
# set schema
# load file to BigQuery


