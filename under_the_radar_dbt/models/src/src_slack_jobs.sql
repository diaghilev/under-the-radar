WITH raw_slack_jobs AS (
  SELECT * FROM modular-terra-372321.tweets_dataset.raw_slack_jobs
)
SELECT
   {{ dbt_utils.generate_surrogate_key(
      ['text']
  ) }} as slack_id,
  text as slack_text,
  url as slack_url,
  timestamp,
  thread_id,
  workspace
FROM raw_slack_jobs