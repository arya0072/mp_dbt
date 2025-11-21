{{
  config(
    materialized= 'table'
  )
}}

SELECT
 DISTINCT *
FROM {{ source('mp_infor', 'ledger_notes') }}  
UNION ALL
SELECT
 DISTINCT *
FROM {{ source('mp_infor', 'ledger_notes_mpkb') }} 
