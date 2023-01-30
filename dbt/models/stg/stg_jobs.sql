WITH src_slack_jobs AS (
  SELECT * FROM {{ ref('src_slack_jobs')}}
),

src_twitter_jobs AS (
  SELECT * FROM {{ ref('src_twitter_jobs')}}
),

-- limit slack data to parent messages (meaning: reply messages on a thread will be excluded)
slack_identify_parent AS (
  SELECT
    slack_id,
    slack_text,
    timestamp,
    thread_ts,
    ts,
    workspace,
    CASE WHEN thread_ts = ts THEN 1 ELSE 0 END is_parent 
  FROM src_slack_jobs
)

SELECT
  slack_id as job_id,
  slack_text as job_text,
  'slack' as source,
  timestamp as timestamp
FROM slack_identify_parent
WHERE is_parent = 1
UNION ALL
SELECT
  tweet_id as job_id,
  tweet_text as job_text,
  'twitter' as source,
  timestamp as timestamp
FROM src_twitter_jobs