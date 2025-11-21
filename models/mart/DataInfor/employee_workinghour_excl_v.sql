{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  CASE 
    WHEN job_title = 'Rolling Girl' AND emp_code = 'Production-DL' THEN 'Rolling Permanent'
    WHEN job_title = 'Rolling Girl' AND emp_code = 'Internship' THEN 'Rolling Magang'
    WHEN job_title = 'Rolling Girl - Cones' AND emp_code = 'Production-DL' THEN 'Rolling Permanent'
    WHEN job_title = 'Rolling Girl - Cones' AND emp_code = 'Internship' THEN 'Rolling Magang'
    WHEN job_title = 'Rolling Girl - Filter' AND emp_code = 'Production-DL' THEN 'Rolling Permanent'
    WHEN job_title = 'Rolling Girl - Filter' AND emp_code = 'Internship' THEN 'Rolling Magang'
    WHEN job_title = 'Cutting Operator' THEN 'Cutting'
    WHEN job_title = 'Plong Operator' THEN 'Cutting'
    WHEN job_title = 'Unwind Operator' THEN 'Cutting'
    WHEN job_title = 'Filter Remover' THEN 'Cutting'
    WHEN job_title = 'Packing Girl' AND emp_code = 'Production-DL' THEN 'Packing'
    WHEN job_title = 'Packing Girl' AND emp_code = 'Internship' THEN 'Packing Magang'
    WHEN job_title = 'Production Packer' THEN 'Packing'
    WHEN job_title = 'Product Packer' THEN 'Packing'
    WHEN job_title = 'Sample Product Packer' THEN 'Packing'
    ELSE 'Notidentify'
  END AS WC,
  nik,
  EXTRACT(MONTH FROM absence_date) AS Month,
  EXTRACT(YEAR FROM absence_date) AS Year,
  SUM(TIMESTAMP_DIFF(time_out_schedule, time_in_schedule, HOUR)) AS Working_Hours1,
  SUM((TIMESTAMP_DIFF(time_out_schedule, time_in_schedule, MINUTE) - 30) / 60) AS Working_Hours2,
  SUM((TIMESTAMP_DIFF(time_out_schedule, time_in_schedule, MINUTE) - 30) / 60) + sum(Coalesce(ot_hour,0)) AS Working_Hours
FROM {{ source('mp_infor', 'employee_absence') }}  
GROUP BY WC,nik, Month, Year