WITH raw_twitter_jobs AS (
  SELECT * FROM modular-terra-372321.tweets_dataset.raw_twitter_jobs
)
SELECT
  id as tweet_id,
  text as tweet_text,
  created_at as timestamp
FROM raw_twitter_jobs