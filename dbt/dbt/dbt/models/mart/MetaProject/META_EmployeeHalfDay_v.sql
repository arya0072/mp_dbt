{{
  config(
    materialized= 'table'
  )
}}

select 
  nik,
  DATE(absence_date) AS absence_date,
  work_hour,
  TRUE AS is_halfday
from {{ source('mp_infor', 'employee_absence') }}
where absence_date >= '2025-01-21' AND work_hour <= 4.50  	
