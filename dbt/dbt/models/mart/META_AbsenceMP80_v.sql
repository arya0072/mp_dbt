{{
  config(
    materialized: 'table'
  )
}}

select
  total_abs.NIK,
  total_abs.EmpName,
  total_abs.PeriodeDate,
  total_abs.Periode,
  count(total_abs.IncentiveDate) AS CountAbsence
from (
select 
  trim(a.EmpNum) AS NIK,
  a.EmpName,
  DATE(
    CASE 
      WHEN EXTRACT(DAY FROM a.IncentiveDate) >= 21 THEN 
        DATE_ADD(DATE_TRUNC(DATE(a.IncentiveDate), MONTH), INTERVAL 1 MONTH)
      ELSE 
        DATE_TRUNC(DATE(a.IncentiveDate), MONTH)
    END
  ) AS PeriodeDate,
  FORMAT_DATE(
    '%Y-%m', 
    CASE 
      WHEN EXTRACT(DAY FROM a.IncentiveDate) >= 21 THEN 
        DATE_ADD(DATE_TRUNC(DATE(a.IncentiveDate), MONTH), INTERVAL 1 MONTH)
      ELSE 
        DATE_TRUNC(DATE(a.IncentiveDate), MONTH)
    END
  ) AS Periode,
  FORMAT_DATE('%Y-%m-%d', a.IncentiveDate) AS IncentiveDate
from {{ source('mp_infor', 'p80_incentives') }} a 
where (a.Gross > 0 AND a.TargetQty > 0 AND a.TotalHours > 0) 
  AND a.IncentiveDate >= '2025-01-21'
  AND SUBSTR(a.Job, 1, 4) IN ('JSFG','JSFJ','JSMJ')  
GROUP BY PeriodeDate, Periode,
nik,a.EmpName, IncentiveDate
ORDER BY IncentiveDate) total_abs
GROUP BY total_abs.nik,
  total_abs.EmpName,
  total_abs.PeriodeDate,
  total_abs.Periode