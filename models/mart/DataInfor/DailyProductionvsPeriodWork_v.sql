{{
  config(
    materialized= 'table'
  )
}}

SELECT *
FROM {{ source('mp_infor', 'EmployeeContractHistory') }} AS ech
LEFT JOIN {{ source('mp_infor', 'mp80_incentives') }}  AS mi  ON ech.nik = TRIM(mi.EmpNum)