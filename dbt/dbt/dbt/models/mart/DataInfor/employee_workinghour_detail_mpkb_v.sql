{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  CASE 
    WHEN job_title = 'Laser CNC Operator'  THEN 'CNC'
    WHEN job_title = 'CNC/Machineries'  THEN 'CNC'
    WHEN job_title = 'Operator Laser Cutting'  THEN 'CNC'
    WHEN job_title = 'Operator Painting'  THEN 'CNC'
    WHEN job_title = 'Packaging Operator'  THEN 'Packing'
    WHEN job_title = 'Operator Packing'  THEN 'Packing'
    WHEN job_title = 'Workbench Operator'  THEN 'Workbench'
    WHEN job_title = 'Operator Kerja Bangku'  THEN 'Workbench'
    WHEN job_title = 'Benchworker'  THEN 'Workbench'
    WHEN job_title = 'Electronic Assembly Operator'  THEN 'Assembly'
    WHEN job_title = 'Accessories Assembly Operator' THEN 'Assembly'
    WHEN job_title = 'Assembling Machine' THEN 'Assembly'
    WHEN job_title = 'Assembling Filling Device' THEN 'Assembly'
    ELSE 'Notidentify'
  END AS WC,
  nik,
  employee_name,
  job_title,
  section,
  tap_in_location,
  COUNT(DISTINCT absence_date) AS Days,
  EXTRACT(DATE FROM absence_date) AS Date,
  EXTRACT(MONTH FROM absence_date) AS Month,
  EXTRACT(YEAR FROM absence_date) AS Year,
  SUM((TIMESTAMP_DIFF(time_out_schedule, time_in_schedule, MINUTE) - 30) / 60) AS Working_Hours,
  0 AS Overtime
FROM {{ source('mp_infor', 'employee_absence') }} 
GROUP BY WC,nik,employee_name,job_title,section,tap_in_location,absence_date, Month, Year
UNION ALL
SELECT 
  CASE 
    WHEN job_title = 'Laser CNC Operator'  THEN 'CNC'
    WHEN job_title = 'CNC/Machineries'  THEN 'CNC'
    WHEN job_title = 'Operator Laser Cutting'  THEN 'CNC'
    WHEN job_title = 'Operator Painting'  THEN 'CNC'
    WHEN job_title = 'Packaging Operator'  THEN 'Packing'
    WHEN job_title = 'Operator Packing'  THEN 'Packing'
    WHEN job_title = 'Workbench Operator'  THEN 'Workbench'
    WHEN job_title = 'Operator Kerja Bangku'  THEN 'Workbench'
    WHEN job_title = 'Benchworker'  THEN 'Workbench'
    WHEN job_title = 'Electronic Assembly Operator'  THEN 'Assembly'
    WHEN job_title = 'Accessories Assembly Operator' THEN 'Assembly'
    WHEN job_title = 'Assembling Machine' THEN 'Assembly'
    WHEN job_title = 'Assembling Filling Device' THEN 'Assembly'
    ELSE 'Notidentify'
  END AS WC,
  nik,
  employee_name,
  job_title,
  section,
  tap_in_location,
  COUNT(DISTINCT absence_date) AS Days,
  EXTRACT(DATE FROM absence_date) AS Date,
  EXTRACT(MONTH FROM absence_date) AS Month,
  EXTRACT(YEAR FROM absence_date) AS Year,
  0 AS Working_Hours,
  sum(Coalesce(ot_hour,0)) AS Overtime
FROM {{ source('mp_infor', 'employee_absence') }}  
WHERE is_overtime=1 
GROUP BY WC,nik,employee_name,job_title,section,tap_in_location,absence_date, Month, Year