{{
  config(
    materialized= 'table'
  )
}}

select
  total_abs.NIK,
  total_abs.EmpName,
  total_abs.PeriodeDate,
  total_abs.Periode,
  SUM(total_abs.DayCount) AS CountAbsence
FROM (
SELECT 
  trim(a.EmpNum) AS NIK,
  a.EmployeeName AS EmpName,
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
  FORMAT_DATE('%Y-%m-%d', a.IncentiveDate) AS IncentiveDate,
  coalesce( absence.day_count,1) as DayCount
from {{ ref('MP80_IncentiveMP_v') }} a 
LEFT JOIN (SELECT 
              a.nik,
              a.employee_name,
              FORMAT_DATE('%Y-%m-%d', a.absence_date) AS absence_date,
              user_absence.mins_late as minutes_late,
              user_absence.id_user_leave,
              user_absence.is_ot,
              user_absence.day_count
            FROM {{ source('mp_infor', 'employee_absence') }} a
              LEFT JOIN {{ source('mp_infor', 'user_absence') }} user_absence ON a.id_user_absence =  user_absence.id_user_absence
            WHERE user_absence.is_ot = 0
              AND user_absence.id_user_leave IS NOT NULL
            ) absence ON TRIM(a.EmpNum) = absence.nik
                     AND FORMAT_DATE('%Y-%m-%d', a.IncentiveDate) = absence.absence_date
where (a.Gross > 0 AND a.TargetQty > 0 AND a.TotalHours > 0) 
  AND a.IncentiveDate >= '2025-01-21'
  AND SUBSTR(a.Job, 1, 4) IN ('JSFG','JSFJ','JSMJ')
GROUP BY PeriodeDate, Periode,
nik,a.EmployeeName, IncentiveDate, DayCount
) total_abs
GROUP BY total_abs.nik,
  total_abs.EmpName,
  total_abs.PeriodeDate,
  total_abs.Periode