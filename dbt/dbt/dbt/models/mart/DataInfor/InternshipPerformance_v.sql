{{
  config(
    materialized= 'table'
  )
}}

SELECT 
    a.id,
    a.id_m_user,
    a.nik as NIK,
    user.FullName,
    a.id_m_user_uploader,
    DATE(a.date) AS date,
    a.total_gross,
    a.total_reject,
    a.total_net,
    a.work_hours,
    a.target_per_hours,
    DATE(a.created_at) AS created_at,
    user.floor,
    user.Location
FROM {{ source('mp_infor', 'InternshipPerformance') }} a
  LEFT JOIN {{ source('mp_infor', 'hris_user') }} user ON a.id_m_user = CAST(user.id_m_user AS STRING)