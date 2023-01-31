WITH dim_jobs AS (
  SELECT * FROM {{ ref('dim_jobs')}}
)

SELECT 
  DATE_TRUNC(timestamp, week) as week,
  COUNT(job_id) AS count_jobs,
  ROUND(SUM(CASE WHEN is_contract = 'Yes' THEN 1 ELSE 0 END)/COUNT(job_id)*100,2) AS percent_contract,
  ROUND(SUM(CASE WHEN is_parttime = 'Yes' THEN 1 ELSE 0 END)/COUNT(job_id)*100,2) AS percent_parttime,
  ROUND(SUM(CASE WHEN is_remote = 'Yes' THEN 1 ELSE 0 END)/COUNT(job_id)*100,2) AS percent_remote
 FROM dim_jobs
 GROUP BY week