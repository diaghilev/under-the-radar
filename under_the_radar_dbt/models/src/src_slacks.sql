WITH raw_slacks AS (
  SELECT * FROM modular-terra-372321.tweets_dataset.raw_slacks
)
SELECT
  text as slack_text,
  url as slack_url,
  date
FROM raw_slacks