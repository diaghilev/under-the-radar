WITH dim_jobs AS (
  SELECT * FROM {{ ref('dim_jobs')}}
)

SELECT 
  DATE_TRUNC(timestamp, week) as week,
  COUNT(job_id) AS count_jobs,
  
  -- In our small sample, do we see any trends related to flexible work?
  ROUND(SUM(CASE WHEN is_contract = true THEN 1 ELSE 0 END)/COUNT(job_id)*100,2) AS percent_contract, -- Hypothesis: % contract jobs increases after layoffs
  ROUND(SUM(CASE WHEN is_parttime = true THEN 1 ELSE 0 END)/COUNT(job_id)*100,2) AS percent_parttime, -- Hypothesis: % parttime jobs increases due to tighter budgets
  ROUND(SUM(CASE WHEN is_remote = true THEN 1 ELSE 0 END)/COUNT(job_id)*100,2) AS percent_remote -- Hypothesis: % remote jobs decreases due to RTO
 
 FROM dim_jobs
 GROUP BY week