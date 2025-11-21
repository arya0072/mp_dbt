{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  CASE 
    WHEN sub_unit = 'Rolling Cones' AND emp_code = 'Production-DL' THEN 'Rolling Permanent'
    WHEN sub_unit = 'Rolling Cones' AND emp_code = 'Production - DL-C' THEN 'Rolling Permanent'
    WHEN sub_unit = 'Rolling Cones' AND emp_code = 'Internship' THEN 'Rolling Magang'
    WHEN sub_unit = 'Rolling Filter Tips' AND emp_code = 'Production-DL' THEN 'Rolling Permanent'
    WHEN sub_unit = 'Rolling Filter Tips' AND emp_code = 'Internship' THEN 'Rolling Magang'
    WHEN sub_unit = 'Cutting' AND emp_code = 'Production-DL' THEN 'Cutting'
    WHEN sub_unit = 'Cutting' AND emp_code = 'Internship' THEN 'Cutting Magang'
    WHEN sub_unit = 'Filter Remover'AND emp_code = 'Production-DL' THEN 'Cutting'
    WHEN sub_unit = 'Filter Remover'AND emp_code = 'Internship' THEN 'Cutting Magang'
    WHEN sub_unit = 'Packing' AND emp_code = 'Production-DL' THEN 'Packing'
    WHEN sub_unit = 'Packing' AND emp_code = 'Internship' THEN 'Packing Magang'
    ELSE 'Notidentify'
  END AS WC,
  nik,
  tap_in_location,
  EXTRACT(MONTH FROM absence_date) AS Month,
  EXTRACT(YEAR FROM absence_date) AS Year,
  SUM((TIMESTAMP_DIFF(time_out_schedule, time_in_schedule, MINUTE) - 30) / 60) AS Working_Hours,
  0 AS Overtime
FROM {{ source('mp_infor', 'employee_absence') }}  
GROUP BY WC,nik,tap_in_location, Month, Year
UNION ALL
SELECT 
  CASE 
    WHEN sub_unit = 'Rolling Cones' AND emp_code = 'Production-DL' AND is_overtime=1 THEN 'Overtime - Rolling Permanent'
    WHEN sub_unit = 'Rolling Cones' AND emp_code = 'Production - DL-C' AND is_overtime=1 THEN 'Overtime - Rolling Permanent'
    WHEN sub_unit = 'Rolling Filter Tips' AND emp_code = 'Production-DL' AND is_overtime=1 THEN 'Overtime - Rolling Permanent'  
    WHEN sub_unit = 'Cutting' AND emp_code = 'Production-DL' AND is_overtime=1 THEN 'Overtime - Cutting'
    WHEN sub_unit = 'Filter Remover'AND emp_code = 'Production-DL' AND is_overtime=1 THEN 'Overtime - Cutting'
    WHEN sub_unit = 'Packing' AND emp_code = 'Production-DL' AND is_overtime=1 THEN 'Overtime - Packing'
    ELSE 'Notidentify'
  END AS WC,
  nik,
  tap_in_location,
  EXTRACT(MONTH FROM absence_date) AS Month,
  EXTRACT(YEAR FROM absence_date) AS Year,
  0 AS Working_Hours,
  sum(Coalesce(ot_hour,0)) AS Overtime
FROM {{ source('mp_infor', 'employee_absence') }}
WHERE is_overtime=1
GROUP BY WC,nik,tap_in_location, Month, Year