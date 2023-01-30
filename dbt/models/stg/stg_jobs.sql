WITH src_slack_jobs AS (
  SELECT * FROM {{ ref('src_slack_jobs')}}
),

src_twitter_jobs AS (
  SELECT * FROM {{ ref('src_twitter_jobs')}}
),

-- limit slack data to job announcements by removing replies to message threads
slack_exclude_replies AS (
  SELECT
    slack_id,
    slack_text,
    timestamp,
    thread_ts,
    ts,
    workspace,
    CASE WHEN thread_ts = ts THEN 'no' ELSE 'yes' END is_reply 
  FROM src_slack_jobs
)

SELECT
  slack_id as job_id,
  slack_text as job_text,
  'slack' as source,
  timestamp as timestamp
FROM slack_exclude_replies
WHERE is_reply = 'no'
UNION ALL
SELECT
  tweet_id as job_id,
  tweet_text as job_text,
  'twitter' as source,
  timestamp as timestamp
FROM src_twitter_jobs