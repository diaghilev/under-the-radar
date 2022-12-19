# This script will access twitter API to pull tweets based on a query

# import packages
import tweepy
import configparser
import requests
import pandas as pd
import sqlite3
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import pprint
import json

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
query = 'context:66.961961812492148736 lang:en #hiring #remote data engineer'

tweets = client.search_recent_tweets(
   query=query,
   tweet_fields=['author_id','created_at'],
   max_results=10,
   user_auth=True
)

# save data as dictionary
tweets_dictionary = tweets.json()
pprint.pprint(tweets_dictionary)

# # extract "data" value from dictionary
# tweets_data = tweets_dict['data']
# #pprint.pprint(tweets_dict)
#
# # transform to pandas dataframe
# tweets_df = pd.json_normalize(tweets_data)
# pd.set_option('display.max_columns', 10)
# print(tweets_df)
#
# # get column names from dataframe
# list_columns = list(tweets_df)
# #print(list_columns)
#
# # Load. Cursor is the way we send statements to the database
# DATABASE_LOCATION = "sqlite:////Users/laurenkaye/PycharmProjects/twitter_v1.0/job_tweets_v1.sqlite"
#
# engine = sqlalchemy.create_engine(DATABASE_LOCATION)
# conn = sqlite3.connect('job_tweets')
# cursor = conn.cursor()
#
# sql_query = """
# CREATE TABLE IF NOT EXISTS job_tweets(
#    id INT,
#    created_at TEXT,
#    text TEXT,
#    author_id TEXT,
#    edit_history_tweet_ids TEXT
# )
# """
#
# engine.execute(sql_query)
# print("Opened database successfully")
#
# try:
#     tweets_df.to_sql('job_tweets', conn, index=False, if_exists='replace')
#     print("Data has been added")
# except:
#     print("Data already exists in the database")
#
# conn.commit()
# conn.close()
# print("Close database")

