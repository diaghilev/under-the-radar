WITH stg_jobs AS (
  SELECT * FROM {{ ref('stg_jobs')}}
)

SELECT
  job_id,
  job_text,
  CASE 
    WHEN lower(job_text) LIKE '%contract%' 
    OR lower(job_text) LIKE '%freelance%' THEN 'Yes' ELSE 'No' END AS is_contract,
  CASE 
    WHEN lower(job_text) LIKE '%part-time%'
    OR lower(job_text) LIKE '%parttime%'
    OR lower(job_text) LIKE '%part time%' THEN 'Yes' ELSE 'No' END AS is_parttime,
  CASE WHEN lower(job_text) LIKE '%remote%' THEN 'Yes' ELSE 'No' END AS is_remote,
  source,
  timestamp
FROM stg_jobs
