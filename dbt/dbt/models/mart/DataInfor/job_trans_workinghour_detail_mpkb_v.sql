{{
  config(
    materialized= 'table'
  )
}}

SELECT 
    DISTINCT
    CASE
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('ACR') AND SUBSTR(a.JobrWc,1,6) not in ('ACR-AW')  THEN 'CNC'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('ACR-AW') THEN 'Workbench'
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('ASY')  THEN 'Assembly'
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('FIN')  THEN 'Packing'
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('PCK')  THEN 'Packing'
      ELSE 'NotIdentify'
    END AS WC,
    a.Job,
    a.AHrs,
    a.jobtUf_MP55_EmployeeCount,
    a.RESID,
    a.RESDescription,
    a.Whse,
    EXTRACT(DATE FROM a.TransDate) AS Date,
    EXTRACT(MONTH FROM a.TransDate) AS Month,
    EXTRACT(YEAR FROM a.TransDate) AS Year,
    sum(a.AHrs * a.jobtUf_MP55_EmployeeCount) as Working_Hours
FROM {{ source('mp_infor', 'job_transaction_mp') }} a
WHERE SUBSTR(a.RESID, 1, 4) NOT IN ('MAC-') 
  AND a.Whse in ('MPKB','KBPR')
GROUP BY WC, Job, AHrs, jobtUf_MP55_EmployeeCount, RESID, RESDescription, Whse, Date, Month, Year
-- Where Job = 'JSFT-06228'
UNION ALL
SELECT 
    DISTINCT
    CASE
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('ACR') AND SUBSTR(a.JobrWc,1,6) not in ('ACR-AW')  THEN 'CNC'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('ACR-AW') THEN 'Workbench'
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('ASY')  THEN 'Assembly'
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('FIN')  THEN 'Packing'
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('PCK')  THEN 'Packing'
      ELSE 'NotIdentify'
    END AS WC,
    a.Job,
    a.AHrs,
    a.jobtUf_MP55_EmployeeCount,
    a.RESID,
    a.RESDescription,
    a.Whse,
    EXTRACT(DATE FROM a.TransDate) AS Date,
    EXTRACT(MONTH FROM a.TransDate) AS Month,
    EXTRACT(YEAR FROM a.TransDate) AS Year,
    sum(a.AHrs * a.jobtUf_MP55_EmployeeCount) as Working_Hours
FROM {{ source('mp_infor', 'job_transaction_mpkb') }}  a
WHERE SUBSTR(a.RESID, 1, 4) NOT IN ('MAC-') 
  AND a.Whse in ('MPKB','KBPR')
GROUP BY WC, Job, AHrs, jobtUf_MP55_EmployeeCount, RESID, RESDescription, Whse, Date, Month, Year
-- Where Job = 'JSFT-06228'