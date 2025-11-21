{{
  config(
    materialized= 'table'
  )
}}

SELECT 
 stat.Periode,
 COALESCE(MAX(wd.TotalDays), 0) AS TotalDays,
 COALESCE(MAX(wd.TotalDaysMonth), 0) AS TotalDaysMonth,
 stat.Job,
  CASE 
    WHEN COUNT(CASE WHEN stat.JobStatus = 'Not Complete' THEN 1 END) = 0 AND (MAX(wd.TotalDays) = MAX(wd.TotalDaysMonth)) THEN 'Complete' 
  ELSE 'Not Complete' 
  END AS Status
FROM (
  SELECT 
    FORMAT_DATE(
      '%Y-%m', 
      CASE 
        WHEN EXTRACT(DAY FROM a.IncentiveDate) >= 21 THEN 
          DATE_ADD(DATE_TRUNC(DATE(a.IncentiveDate), MONTH), INTERVAL 1 MONTH)
        ELSE 
          DATE_TRUNC(DATE(a.IncentiveDate), MONTH)
      END
    ) AS Periode,
    CASE
      WHEN a.JobStatus = 'C' THEN 'Complete'
      ELSE 'Not Complete'
    END AS JobStatus,
    SUBSTR(a.Job, 1, 4) AS Job
  FROM {{ ref('MP80_IncentiveMP_v') }} a 
  WHERE (a.Gross > 0 AND a.TargetQty > 0 AND a.TotalHours > 0) 
    AND a.IncentiveDate >= '2025-01-21'
    AND SUBSTR(a.Job, 1, 4) IN ('JSFG','JSFJ','JSMJ')
) stat
  LEFT JOIN {{ ref('META_WorkingDays_v') }} wd ON stat.Periode = wd.Periode
GROUP BY stat.Periode, stat.Job
ORDER BY stat.Periode