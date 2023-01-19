## Under the Radar Jobs


### Objective

The intent of this project is to build an end-to-end data pipeline that serves a list of under-the-radar job announcements from unstructured data sources (tweets and slack channels).


### Workflow

- **Ingestion** 
    - [x] Python script fetches job-related tweets from the twitter API and loads to a JSONL file.
    - [x] Zapier automation fetches data from several Slack #job channels and loads to a Google Sheet
- [x] **Storage** - Python script generates a dataset + tables in BigQuery and loads ingested data there.
- [ ] **Transformation** - dbt transforms source tables, preparing them for a filterable reporting layer.
- [ ] **Reporting** - Looker Studio lists job announcements with filters for parttime and contract positions.
- [ ] **Orchestration** - Once daily refresh of tweets data with (cron + google cloud functions?).
- [ ] **Deployment** - Docker to containerize the pipeline.


### Transformation

Here's what I plan to build in dbt. This is sort of a DAG / ERD mash-up, hopefully that is not too confusing.

![Image](transformation_plan.png)



### Outstanding Questions

1. Docker is new to me and I won't fully understand it til I get my hands dirty. That said, do you see any obvious barriers to 'dockerizing' this pipeline? For example, I think slack access requires my personal login, and so I'm wondering if that's going to be a problem. 
2. Is use of zapier for slack > sheets ingestion step frowned upon?
3. Any other major issues you see that you'd encourage me to work on first?