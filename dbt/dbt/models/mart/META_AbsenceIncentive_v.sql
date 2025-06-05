{{
  config(
    materialized: 'table'
  )
}}

SELECT
  a.nik AS NIK,
  a.employee_name AS EmployeeName,
  DATE(
    CASE 
      WHEN EXTRACT(DAY FROM a.absence_date) >= 21 THEN 
        DATE_ADD(DATE_TRUNC(DATE(a.absence_date), MONTH), INTERVAL 1 MONTH)
      ELSE 
        DATE_TRUNC(DATE(a.absence_date), MONTH)
    END
  ) AS PeriodeDate, 
  FORMAT_DATE(
    '%Y-%m', 
    CASE 
      WHEN EXTRACT(DAY FROM a.absence_date) >= 21 THEN 
        DATE_ADD(DATE_TRUNC(DATE(a.absence_date), MONTH), INTERVAL 1 MONTH)
      ELSE 
        DATE_TRUNC(DATE(a.absence_date), MONTH)
    END
  ) AS Periode,
  COUNT(a.nik) AS CountAbsence
FROM {{ source('mp_infor', 'employee_absence' )}} a

GROUP BY PeriodeDate, Periode,
a.nik,a.employee_name
ORDER BY PeriodeDate;