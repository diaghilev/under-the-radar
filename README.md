## Under the Radar Jobs

### Introduction

Automate a search for contract/part-time data jobs using unstructured data sources such as tweets and slack job channels.

### Workflow

[Diagram of Architecture to come]

- **Ingestion** 
    - [x] Python script fetches data from the twitter API and loads to a JSONL file.
    - [x] Zapier automation fetches data from several Slack #job channels and loads to a Google Sheet
- [x] **Storage** - Python script generates a dataset + tables in BigQuery and loads ingested data there.
- [ ] **Transformation** - dbt transforms source tables as described in 'Transformation' section, preparing them for a filterable reporting layer.
- [ ] **Reporting** - Looker Studio lists recent job posts/tweets in a filterable manner.
- [ ] **Orchestration** - Cron for once daily refresh of tweets data?
- [ ] **Deployment** - Docker 

### Project Files
(so far) 

- main.py - The main ETL script. (so far)
    - Fetches data from the Twitter API
    - Saves data to a JSONL file
    - Creates dataset & table in BigQuery
    - Loads the data to BigQuery
- tweets.jsonl - The JSONL file that stores tweets.
- apikey.json - Stores Google Cloud credentials (not version controlled)
- config.ini - Stores Twitter API credentials (not version controlled)

### Transformation