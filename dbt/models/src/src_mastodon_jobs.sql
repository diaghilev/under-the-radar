WITH raw_mastodon_jobs AS (
  SELECT * FROM {{ source('bigquery', 'raw_mastodon_jobs')}}
)
SELECT
  CAST(id AS string) AS mastodon_id, -- casting to enable the Union in the stg_jobs model
  content AS mastodon_text,
  CAST(created_at AS datetime) AS timestamp,
  url AS url,
  acct AS mastodon_account
FROM raw_mastodon_jobs