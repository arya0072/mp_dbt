{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  DISTINCT *  
FROM {{ source('mp_infor', 'ledger') }} 
UNION ALL
SELECT
  DISTINCT *  
FROM {{ source('mp_infor', 'ledger_mpkb') }} 
