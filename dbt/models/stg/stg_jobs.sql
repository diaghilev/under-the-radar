WITH src_slack_jobs AS (
  SELECT * FROM {{ ref('src_slack_jobs')}}
),

src_twitter_jobs AS (
  SELECT * FROM {{ ref('src_twitter_jobs')}}
),

-- remove replies from slack data
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
),

-- remove duplicates from twitter data
twitter_exclude_dupes AS (
  SELECT
    tweet_id,
    tweet_text,
    timestamp,
    ROW_NUMBER() OVER (PARTITION BY replace(tweet_text,' ','') ORDER BY timestamp DESC) AS row_num  
  FROM src_twitter_jobs
)

-- merge slack and twitter data
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
FROM twitter_exclude_dupes
WHERE row_num = 1