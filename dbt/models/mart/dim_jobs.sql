WITH stg_jobs AS (
  SELECT * FROM {{ ref('stg_jobs')}}
)

SELECT
   job_id,
  job_text,
  CASE 
    WHEN lower(job_text) LIKE '%contract%' 
    OR lower(job_text) LIKE '%freelance%' THEN CAST(1 AS boolean) ELSE CAST(0 AS boolean) END AS is_contract,
  CASE 
    WHEN lower(job_text) LIKE '%part-time%'
    OR lower(job_text) LIKE '%parttime%'
    OR lower(job_text) LIKE '%part time%' THEN CAST(1 AS boolean) ELSE CAST(0 AS boolean) END AS is_parttime,
  CASE WHEN lower(job_text) LIKE '%remote%' THEN CAST(1 AS boolean) ELSE CAST(0 AS boolean) END AS is_remote,
  source,
  timestamp
FROM stg_jobs
