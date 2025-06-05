{{ 
    config(
    materialized='table'
    )
}}

SELECT
DISTINCT
  FORMAT_DATE('%Y-%m-%d', a.IncentiveDate) AS IncentiveDate,
  a.Job,
  TRIM(a.EmpNum) as EmpNum,
  HD.is_halfday,
  double_job.count_job,
   a.TotalHours as MP80,
   JT.AHrs AS UJT,
  CASE
    WHEN double_job.count_job > 1 THEN a.TotalHours
    WHEN HD.is_halfday IS TRUE THEN a.TotalHours
    WHEN a.TotalHours = JT.AHrs THEN JT.AHrs
    WHEN a.TotalHours < JT.AHrs THEN a.TotalHours
    WHEN a.TotalHours > JT.AHrs THEN JT.AHrs
    ELSE JT.AHrs
  END AS TotalHours
FROM {{ source('mp_infor', 'mp80_incentives') }} a
  LEFT JOIN (select 
              Job,
              AHrs,
              jobtUf_MP55_EmployeeCount AS EmpCountJT
            from {{ source('mp_infor', 'job_transaction_mp')}} 
            where OperNum='10' ) JT ON a.Job = JT.job
  LEFT JOIN {{ source('mp_infor', 'META_EmployeeHalfDay_v') }} HD ON TRIM(a.EmpNum) = HD.nik AND DATE(a.IncentiveDate) = HD.absence_date 
  LEFT JOIN (SELECT 
                FORMAT_DATE('%Y-%m-%d', a.IncentiveDate) AS IncentiveDate,
                TRIM(a.EmpNum) AS NIK,
                COUNT(DISTINCT a.job) AS count_job
              FROM mp_infor.mp80_incentives a
              WHERE FORMAT_DATE('%Y-%m-%d', a.IncentiveDate) >= '2025-01-21'
                AND SUBSTR(a.Job, 1, 5) IN ('JSFG-','JSFJ-','JSMJ-')  -- JO Gianyar & Jembrana
                AND a.Job NOT IN (SELECT ue_Job FROM {{ source('mp_infor', 'JobExclude')}})
              GROUP BY 
                IncentiveDate,
                NIK
              HAVING COUNT(DISTINCT a.job) > 1
              ) double_job ON TRIM(a.EmpNum) = double_job.NIK
                           AND FORMAT_DATE('%Y-%m-%d', a.IncentiveDate) = double_job.IncentiveDate

WHERE a.IncentiveDate >= '2025-01-21'
  AND TotalHours > 0
  AND SUBSTR(a.Job, 1, 5) IN ('JSFG-','JSFJ-','JSMJ-')  -- JO Gianyar & Jembrana
  AND a.Job NOT IN (SELECT ue_Job FROM {{ source('mp_infor', 'JobExclude')}})
  -- AND TRIM(a.EmpNum) = '224195' 
  -- AND FORMAT_DATE('%Y-%m-%d', a.IncentiveDate)  = '2025-01-30'
ORDER BY IncentiveDate ASC

