{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  c.ktp,
  c.full_name,
  c.job,
  Date(c.start_date) AS start_date_magang,
  Date(c.end_date) AS end_date_magang,
  c.location,
  c.kategori,
  a.contract,
  DATE(a.start_date) AS start_date_contract,
  DATE(a.end_date) As end_date_contract,
  COUNT(DISTINCT a.start_date) AS HC
 FROM {{ source('mp_infor', 'TurnOverMagang') }} c
 LEFT JOIN {{ source('mp_infor', 'ListUserContract') }} a ON c.ktp = a.ktp
 GROUP BY 
  c.ktp, c.full_name, c.job, c.start_date, c.end_date, 
  c.location, c.kategori, a.contract, a.start_date, a.end_date
 