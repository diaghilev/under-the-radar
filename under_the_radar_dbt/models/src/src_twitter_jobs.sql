WITH raw_twitter_jobs AS (
  SELECT * FROM modular-terra-372321.tweets_dataset.raw_twitter_jobs
)
SELECT
  cast(id as string) as tweet_id, -- casting to enable the Union in the stg_jobs model
  text as tweet_text,
  created_at as timestamp
FROM raw_twitter_jobs