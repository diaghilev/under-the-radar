# 25 Custom Job Tweets a Day

## Introduction

The purpose of this project is to serve 25 job-related tweets a day using search parameters that are customizable by the user. 

## Project Files
- app
    - main.py - The main ETL script.
        - Fetches data from the Twitter API
        - Saves data to a JSONL file
        - Creates a dataset & table in BigQuery
        - Loads the data to BigQuery
    - tweets.jsonl - The JSONL file which stores tweets.
    - apikey.json - Stores Google Cloud credentials 
    - config.ini - Stores Twitter API credentials (not version controlled)

    (To add: Any additional files such as metabase.db , dbt files, docker.yml etc)

## Workflow

Image

Ingestion - Python script fetches data from the twitter API and saves to a JSONL file.
Transformation - TBD (Either python or dbt. Format tweets for readability.)
Storage - Python script creates a dataset & table in BigQuery and loads the data there.
Serving - TBD 
Orchestration - TBD




