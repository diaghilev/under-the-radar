## Under the Radar Jobs


### Objective

The intent of this project is to build an end-to-end data pipeline that serves a list of under-the-radar job announcements from unstructured data sources (tweets and slack channels).


### Workflow

- [x] **Ingestion** 
    - [x] Python script fetches job-related tweets from the twitter API and loads to a JSONL file.
    - [x] Zapier automation fetches data from several Slack #job channels and loads to a Google Sheet
- [x] **Storage** - Python script generates a dataset + tables in BigQuery and loads ingested data there.
- [x] **Transformation** - dbt transforms source tables, preparing them for a filterable reporting layer.
- [ ] **Reporting** - Looker Studio lists job announcements with filters for part-time, contract and remote positions.
- [ ] **Deployment** - Docker to containerize the pipeline.


### Transformation


Current state of the DAG

![Image](img/dag.png)


Transformations performed in stg_jobs model:
- [x] Slack data consists of threaded messages. Our desired output excludes thread replies.
- [x] Tweet data includes duplicate tweets. Our desired output removes duplicates.
- [x] Data from multiple sources must be merged and presented in a single list.

Transformations performed in dim_jobs model
- [x] Identify which jobs are potentially part-time, contract or remote from unstructured text.
- [ ] Present unstructured, messy text in a more human-readable format.



