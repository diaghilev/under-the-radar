## Under the Radar Jobs

### Introduction

This project stemmed from a desire to find under-the-radar job postings on Twitter with an emphasis on contract, freelance and parttime roles. I built an end-to-end data pipeline below to serve [Option 1: 25 fresh custom job-related tweets per day] [Option 2: job-related tweets based on user inputs].


### Project Files

- main.py - The main ETL script.
    - Fetches data from the Twitter API
    - Saves data to a JSONL file
    - Creates dataset & table in BigQuery
    - Loads the data to BigQuery
- tweets.jsonl - The JSONL file that stores tweets.
- apikey.json - Stores Google Cloud credentials (not version controlled)
- config.ini - Stores Twitter API credentials (not version controlled)


### Workflow

[Diagram of Workflow]

- **Ingestion** - Python script fetches data from the twitter API and saves to a JSONL file.
- **Transformation** - [Python or dbt]
- **Storage** - Python script creates a dataset & table in BigQuery and loads the data there.
- **Reporting** - [Streamlit, Looker Studio or Metabase]
- **Orchestration** - 
- **Deployment** - 




