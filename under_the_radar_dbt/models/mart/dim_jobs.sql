WITH stg_jobs AS (
  SELECT * FROM {{ ref('stg_jobs')}}
)

-- I will be adding fields here
SELECT
  job_id,
  job_text,
  source,
  timestamp
FROM stg_jobs
