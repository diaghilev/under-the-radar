WITH raw_slack_jobs AS (
  SELECT * FROM {{ source('bigquery', 'raw_slack_jobs')}}
)
SELECT
   {{ dbt_utils.generate_surrogate_key(
      ['text']
  ) }} as slack_id, -- generate primary key
  text as slack_text,
  url as slack_url,
  timestamp,
  thread_id,
  workspace
FROM raw_slack_jobs