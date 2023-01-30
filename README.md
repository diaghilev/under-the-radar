## Under the Radar Jobs


### Objective

The intent of this project is to build an end-to-end data pipeline that serves a list of under-the-radar job announcements from unstructured data sources (tweets and slack channels).


### Workflow

- [x] **Ingestion** 
    - [x] Python script fetches job-related tweets from the twitter API and loads to a JSONL file.
    - [x] Zapier automation fetches data from several Slack #job channels and loads to a Google Sheet
- [x] **Storage** - Python script generates a dataset + tables in BigQuery and loads ingested data there.
- [x] **Transformation** - dbt transforms source tables, preparing them for a filterable reporting layer.
- [ ] **Reporting** - Looker Studio lists job announcements with filters for parttime and contract positions.
- [ ] **Deployment** - Docker to containerize the pipeline.


### Transformation

Here's what I am currently building, as a DAG.

![Image](img/dag.png)

Examples of transformations performed:
- [ ] Slack data consists of threaded messages. The data we want (the job announcement) is the parent message in any given thread. 
- [ ] Tweet data includes duplicate tweets. We need to remove those duplicates.
- [x] Data from multiple sources needs to be merged and presented in a single list.
- [x] We want to identify from unstructured text which jobs are potentially remote, contract, and/or part-time.
- [ ] We want to present unstructured, messy text in a more human-readable format.



