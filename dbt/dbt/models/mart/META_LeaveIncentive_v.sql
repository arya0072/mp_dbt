{{
  config(
    materialized= 'table'
  )
}}

SELECT   
  a.NIK,
  a.employee_name AS EmployeeName,
  DATE(
    CASE 
      WHEN EXTRACT(DAY FROM a.leave_date) >= 21 THEN 
        DATE_ADD(DATE_TRUNC(DATE(a.leave_date), MONTH), INTERVAL 1 MONTH)
      ELSE 
        DATE_TRUNC(DATE(a.leave_date), MONTH)
    END
  ) AS PeriodeDate, 
  FORMAT_DATE(
    '%Y-%m', 
    CASE 
      WHEN EXTRACT(DAY FROM a.leave_date) >= 21 THEN 
        DATE_ADD(DATE_TRUNC(DATE(a.leave_date), MONTH), INTERVAL 1 MONTH)
      ELSE 
        DATE_TRUNC(DATE(a.leave_date), MONTH)
    END
  ) AS Periode,
  COUNT(a.nik) AS CountLeave
FROM {{ source('mp_infor', 'userleave_detail') }} a 
WHERE (a.id_m_leave_type_group IN (8,2) --included special leave, paid Leave, sick
OR a.id_m_leave_type IN (43)) -- included sakit berat)
-- AND a.NIK='225055'
-- AND a.leave_date between '2025-01-21' AND '2025-02-20'
GROUP BY PeriodeDate, Periode,
a.NIK,a.employee_name
ORDER BY PeriodeDate