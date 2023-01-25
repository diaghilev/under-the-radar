WITH src_slack_jobs AS (
  SELECT * FROM {{ ref('src_slack_jobs')}}
),

src_twitter_jobs AS (
  SELECT * FROM {{ ref('src_twitter_jobs')}}
)

SELECT
  slack_id as job_id,
  slack_text as job_text,
  'slack' as source,
  timestamp as timestamp
FROM src_slack_jobs
UNION ALL
SELECT
  tweet_id as job_id,
  tweet_text as job_text,
  'twitter' as source,
  timestamp as timestamp
FROM src_twitter_jobs