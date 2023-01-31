WITH raw_twitter_jobs AS (
  SELECT * FROM {{ source('bigquery', 'raw_twitter_jobs')}}
)
SELECT
  cast(id as string) as tweet_id, -- casting to enable the Union in the stg_jobs model
  text as tweet_text,
  cast(created_at as datetime) as timestamp
FROM raw_twitter_jobs