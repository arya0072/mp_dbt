{{
  config(
    materialized= 'table'
  )
}} 

SELECT * FROM {{ source('mp_infor', 'movement_value_mp_new') }} a 
UNION ALL
SELECT * FROM {{ source('mp_infor', 'movement_value_mpkb_new') }} a
UNION ALL
SELECT * FROM {{ source('mp_infor', 'movement_value_jmbr_new') }} a