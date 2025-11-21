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
    EXTRACT(MONTH FROM a.TransDate) AS Month,
    EXTRACT(YEAR FROM a.TransDate) AS Year,
    sum(a.AHrs * a.jobtUf_MP55_EmployeeCount) as Working_Hours,
    a.Whse,
FROM {{ source('mp_infor', 'job_transaction_mpkb') }} a
WHERE SUBSTR(a.RESID, 1, 4) NOT IN ('MAC-') 
  AND a.Whse in ('MPKB','KBPR')
group by WC, Month, Year, a.Whse
-- Where Job = 'JSFT-06228'
UNION ALL
SELECT 
    DISTINCT
    CASE
      WHEN SUBSTR(b.JobrWc, 1,3) IN ('ACR') AND SUBSTR(b.JobrWc,1,6) not in ('ACR-AW')  THEN 'CNC'
      WHEN SUBSTR(b.JobrWc, 1,6) IN ('ACR-AW') THEN 'Workbench'
      WHEN SUBSTR(b.JobrWc, 1,3) IN ('ASY')  THEN 'Assembly'
      WHEN SUBSTR(b.JobrWc, 1,3) IN ('FIN')  THEN 'Packing'
      WHEN SUBSTR(b.JobrWc, 1,3) IN ('PCK')  THEN 'Packing'
      ELSE 'NotIdentify'
    END AS WC,
    EXTRACT(MONTH FROM b.TransDate) AS Month,
    EXTRACT(YEAR FROM b.TransDate) AS Year,
    sum(b.AHrs * b.jobtUf_MP55_EmployeeCount) as Working_Hours,
    b.whse
FROM {{ source('mp_infor', 'job_transaction_mp') }} b
WHERE SUBSTR(b.RESID, 1, 4) NOT IN ('MAC-') 
  AND b.Whse in ('MPKB','KBPR')
group by WC, Month, Year, b.Whse
