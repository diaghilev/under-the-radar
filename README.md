## 25 Job Tweets A Day

### Introduction

Option #1: The purpose of this project is to serve 25 fresh job-related tweets a day. 

Option #2: The purpose of this project is to serve job-related tweets based on user inputs.


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
- **Transformation** - Python (Next step: Possibly dbt)
- **Storage** - Python script creates a dataset & table in BigQuery and loads the data there.
- **Reporting** - Looker Studio (Next step: Enable user inputs. Streamlit?) 
- **Orchestration** - 
- **Deployment** - 




