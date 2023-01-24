WITH raw_listings AS (
  SELECT * FROM modular-terra-372321.tweets_dataset.raw_tweets
)
SELECT
  id as tweet_id,
  text as tweet_text,
  created_at as date
FROM raw_listings