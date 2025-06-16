{{
  config(
    materialized= 'table'
  )
}}

SELECT   
  a.NIK,
  a.employee_name AS EmployeeName,
  a.leave_date,
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
  a.LeaveTypeGroup,
  a.leave_type,
  user.Department,
  user.JobTitle
FROM {{ source('mp_infor', 'userleave_detail') }} a 
  LEFT JOIN {{ source('mp_infor', 'hris_user') }} user ON a.id_m_user = user.id_m_user
WHERE (a.id_m_leave_type_group IN (8,2) --included special leave, paid Leave, sick
OR a.id_m_leave_type IN (43)) -- included sakit berat)
-- AND a.NIK='225055'
-- AND a.leave_date between '2025-01-21' AND '2025-02-20'
ORDER BY PeriodeDate,a.leave_date