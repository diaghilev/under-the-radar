# This script ingests tweets from twitter API and loads them to BigQuery

# import packages
import tweepy
import configparser
import requests
import json
import os
import time

# define global query variable to pass between functions
query_global = ''

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

   # update the query_global variable
   global query_global
   query_global = query

   # function returns tweets
   return tweets

def to_file(filename):

   query = query_global

   # convert result to dictionary (check this)
   tweets = get_tweets(query)
   tweets_dict_full = tweets.json() 
   tweets_dict = tweets_dict_full['data'] 

   # write result to a JSONL file

   with open(filename, "w") as f:
      for line in tweets_dict:
         f.write(json.dumps(line) + "\n") #Consider context management
   
   print("File created")

if __name__ == '__main__':
    get_tweets('analytics engineer #hiring')
    to_file('refactor.jsonl')