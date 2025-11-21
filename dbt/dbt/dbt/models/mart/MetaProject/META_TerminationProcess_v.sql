{{
  config(
    materialized= 'table'
  )
}}

SELECT
  a.NIK,
  a.EmployeeName,
  DATE(
    CASE 
      WHEN EXTRACT(DAY FROM a.ApplyResignDate) >= 21 THEN 
        DATE_ADD(DATE_TRUNC(DATE(a.ApplyResignDate), MONTH), INTERVAL 1 MONTH)
      ELSE 
        DATE_TRUNC(DATE(a.ApplyResignDate), MONTH)
    END
  ) AS PeriodeDate, 
  FORMAT_DATE(
    '%Y-%m', 
    CASE 
      WHEN EXTRACT(DAY FROM a.ApplyResignDate) >= 21 THEN 
        DATE_ADD(DATE_TRUNC(DATE(a.ApplyResignDate), MONTH), INTERVAL 1 MONTH)
      ELSE 
        DATE_TRUNC(DATE(a.ApplyResignDate), MONTH)
    END
  ) AS Periode,
  COUNT(a.NIK) AS CountResign
FROM {{ source('mp_infor', 'UserResign') }} a
WHERE a.is_cancel = 0
  AND  a.is_approved = 2 --Pending
  -- AND a.NIK IN ('243313','231305')
GROUP BY PeriodeDate, Periode,
a.NIK,a.EmployeeName
ORDER BY PeriodeDate