{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  CASE 
    WHEN job_title = 'Rolling Girl - Cones' AND emp_code = 'Production-DL' THEN 'Rolling Permanent'
    WHEN job_title = 'Rolling Girl - Cones' AND emp_code = 'Production - DL-C' THEN 'Rolling Permanent'
    WHEN job_title = 'Rolling Girl - Cones' AND emp_code = 'Internship' THEN 'Rolling Magang'
    WHEN job_title = 'Rolling Girl - Filter' AND emp_code = 'Production-DL' THEN 'Rolling Permanent'
    WHEN job_title = 'Rolling Girl - Filter' AND emp_code = 'Internship' THEN 'Rolling Magang'
    WHEN job_title = 'Control Girl' AND emp_code = 'Production-DL' THEN 'Rolling Permanent'
    WHEN job_title = 'Control Girl' AND emp_code = 'Internship' THEN 'Rolling Magang'
    WHEN job_title = 'Glue Girl' AND emp_code ='Production-DL' THEN 'Rolling Permanent'
    WHEN job_title = 'Glue Girl' AND emp_code ='Internship' THEN 'Rolling Magang'
    WHEN job_title = 'Rolling Machine Operator' AND emp_code ='Production-DL' THEN 'Rolling Permanent'
    WHEN job_title = 'Rolling Machine Operator' AND emp_code ='Internship' THEN 'Rolling Magang'
    WHEN job_title = 'HL Filter Reefer' AND emp_code ='Production-DL' THEN 'Rolling Permanent'
    WHEN job_title = 'HL Filter Reefer' AND emp_code ='Internship' THEN 'Rolling Magang'
    WHEN job_title = 'Cutting Helper' THEN 'Cutting'
    WHEN job_title = 'Cutting Operator' THEN 'Cutting'
    WHEN job_title = 'Operator Assistant' THEN 'Cutting'
    WHEN job_title = 'Operator Mesin Plong' THEN 'Cutting'
    WHEN job_title = 'Operator Mesin Unwind' THEN 'Cutting'
    When job_title = 'Production Helper' AND sub_unit = 'Filter Remover' Then 'Cutting'
    When job_title = 'Production Helper' AND (section LIKE '%RMFT%' OR section LIKE '%M-%') THEN 'Cutting'
    WHEN job_title = 'Filter Remover' THEN 'Cutting'
    WHEN job_title = 'Filter Removal' THEN 'Cutting'
    WHEN job_title = 'Production Packer' THEN 'Packing'
    ELSE 'Notidentify'
  END AS WC,
  CASE
    WHEN tap_in_location IN ('Jembrana') THEN 'JEMBRANA'
    WHEN tap_in_location IN ('K1') THEN 'MPKB'
    WHEN tap_in_location IN ('BB','Gedung Pabean') THEN 'MP'
    ELSE 'Not Indentify'
  END AS whse,
  EXTRACT(MONTH FROM absence_date) AS Month,
  EXTRACT(YEAR FROM absence_date) AS Year,
  SUM((TIMESTAMP_DIFF(time_out_schedule, time_in_schedule, MINUTE) - 30) / 60) AS Working_Hours,
  0 AS Overtime
FROM {{ source('mp_infor', 'employee_absence') }} 
GROUP BY WC,tap_in_location, Month, Year
UNION ALL
SELECT 
  CASE 
    WHEN job_title = 'Rolling Girl - Cones' AND emp_code = 'Production-DL' AND is_overtime=1 THEN 'Overtime - Rolling Permanent'
    WHEN job_title = 'Rolling Girl - Cones' AND emp_code = 'Production - DL-C' AND is_overtime=1 THEN 'Overtime - Rolling Permanent'
    WHEN job_title = 'Rolling Girl - Filter' AND emp_code = 'Production-DL' AND is_overtime=1 THEN 'Overtime - Rolling Permanent'  
    WHEN job_title = 'Control Girl' AND emp_code = 'Production-DL' AND is_overtime=1 THEN 'Overtime - Rolling Permanent'
    WHEN job_title = 'Glue Girl' AND emp_code ='Production-DL' AND is_overtime=1 THEN 'Overtime - Rolling Permanent'
    WHEN job_title = 'Rolling Machine Operator' AND emp_code ='Production-DL' AND is_overtime=1 THEN 'Overtime - Rolling Permanent'
    WHEN job_title = 'HL Filter Reefer' AND emp_code ='Production-DL' AND is_overtime=1 THEN 'Overtime - Rolling Permanent'
    WHEN job_title = 'Cutting Helper' AND is_overtime=1 THEN 'Overtime - Cutting'
    WHEN job_title = 'Cutting Operator' AND is_overtime=1 THEN 'Overtime - Cutting'
    WHEN job_title = 'Operator Assistant' AND is_overtime=1  THEN 'Overtime - Cutting'
    WHEN job_title = 'Operator Mesin Plong' AND is_overtime=1 THEN 'Overtime - Cutting'
    WHEN job_title = 'Filter Remover' AND is_overtime=1 THEN 'Overtime - Cutting'
    WHEN job_title = 'Filter Removal' AND is_overtime=1 THEN 'Overtime - Cutting'
    WHEN job_title = 'Operator Mesin Unwind' AND is_overtime=1 THEN 'Overtime - Cutting'
    When job_title = 'Production Helper' AND sub_unit IN ('Filter Remover','Cutting') AND is_overtime=1 THEN 'Overtime - Cutting'
    When job_title = 'Production Helper' AND (section LIKE '%RMFT%' OR section LIKE '%M-%') AND is_overtime=1 THEN 'Overtime - Cutting'
    WHEN job_title = 'Production Packer' AND is_overtime=1 THEN 'Overtime - Packing'
    ELSE 'Notidentify'
  END AS WC,
  CASE
    WHEN tap_in_location IN ('Jembrana') THEN 'JEMBRANA'
    WHEN tap_in_location IN ('K1') THEN 'MPKB'
    WHEN tap_in_location IN ('BB','Gedung Pabean') THEN 'MP'
    ELSE 'Not Indentify'
  END AS whse,
  EXTRACT(MONTH FROM absence_date) AS Month,
  EXTRACT(YEAR FROM absence_date) AS Year,
  0 AS Working_Hours,
  sum(Coalesce(ot_hour,0)) AS Overtime
FROM {{ source('mp_infor', 'employee_absence') }} 
WHERE is_overtime=1 
GROUP BY WC,tap_in_location, Month, Year