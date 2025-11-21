{{
  config(
    materialized= 'table'
  )
}}

SELECT DISTINCT
 a.nik,
 a.ktp,
 c.job_title,
 a.contract,
 c.first_day,
 a.start_date,
 a.end_date,
 b.status_after,
 b.reason_resign,
 c.current_status
 FROM {{ source('mp_infor', 'ListUserContract') }} a
 LEFT JOIN {{ source('mp_infor', 'UserHistoryEmployeeStatus') }} b ON a.ktp = b.ktp
 LEFT JOIN {{ source('mp_infor', 'EmployeeContractHistory') }} c ON c.nik = b.nik 
