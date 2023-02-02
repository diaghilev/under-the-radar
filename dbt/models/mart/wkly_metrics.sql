WITH dim_jobs AS (
  SELECT * FROM {{ ref('dim_jobs')}}
)

SELECT 
  DATE_TRUNC(timestamp, week) as week,
  COUNT(job_id) AS count_jobs,
  ROUND(SUM(CASE WHEN is_contract = true THEN 1 ELSE 0 END)/COUNT(job_id)*100,2) AS percent_contract, -- In our sample, do we see contract trends as a result of the economic downturn?
  ROUND(SUM(CASE WHEN is_parttime = true THEN 1 ELSE 0 END)/COUNT(job_id)*100,2) AS percent_parttime, -- In our sample, do we see parttime trends as a result of the economic downturn?
  ROUND(SUM(CASE WHEN is_remote = true THEN 1 ELSE 0 END)/COUNT(job_id)*100,2) AS percent_remote -- In our sample, do we see an increasing RTO trend?
 FROM dim_jobs
 GROUP BY week