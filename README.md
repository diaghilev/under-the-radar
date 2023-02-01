## Under the Radar Jobs


### Objective

The intent of this project is to build a live end-to-end data pipeline that serves a single list of 'under-the-radar' job announcements from multiple unstructured data sources (tweets and slack channels).


### Workflow

- [x] **Ingestion** 
    - [x] Python script fetches job-related tweets from the twitter API and loads to a JSONL file.
    - [x] Zapier automation fetches data from several Slack #job channels and loads to a Google Sheet
- [x] **Storage** - Python script generates a dataset + tables in BigQuery and loads ingested data there.
- [x] **Transformation** - dbt transforms source tables, preparing them for a filterable reporting layer.
- [x] **Reporting** - Looker Studio lists job announcements with filters for part-time, contract and remote positions.
- [ ] **Deployment** - Docker to containerize the pipeline.


### Transformation


**Current state of the DAG**

![Image](img/dag.png)


Key transformations performed in stg_jobs model:
- [x] Slack data consists of _threaded messages_. Our desired output excludes thread replies.
- [x] Tweet data includes _duplicate tweets_. Our desired output removes duplicates.
- [x] Data from _multiple sources_ must be merged and presented in a single list.

Key transformations performed in dim_jobs model:
- [x] Identify which jobs are potentially part-time, contract or remote from unstructured text.
- [ ] Present unstructured, messy text in a more human-readable format.


### Reporting

My primary goal is to produce a single, live list of 'under the radar' job announcements (from unstructured sources).

![Image](img/lookerstudio.png)
Link coming soon.

But I also had a few questions, which will be answered over time in the wkly_metrics table:
- [x] In our sample, do we see an RTO trend in the form of decreasing remote jobs over time?
- [x] In our sample, is there an increase in contract jobs (relative to all jobs) during this economic downturn?
- [x] In our sample, is there an increase in parttime jobs (relative to all jobs) during this economic downturn?

