{{
  config(
    materialized= 'table'
  )
}}

select
  a.nik,
  a.employee_name,
  a.gender,
  a.birth_date,
  a.age,
  a.range_age,
  a.employee_code,
  a.division,
  a.department,
  a.unit,
  a.jobs,
  a.employee_status,
  a.status_mcu,
  a.total_mcu,
  a.group_year_service,
  a.join_date,
  a.effective_date,
  mcu.mcu_date
from {{ source('mp_infor', 'HRIS_McuAll') }} a
  LEFT JOIN (SELECT
                NIK,
                max(mcu_date) as mcu_date
            FROM {{ source('mp_infor', 'HRIS_Mcu') }}
            GROUP BY NIK) mcu ON a.NIK = mcu.nik