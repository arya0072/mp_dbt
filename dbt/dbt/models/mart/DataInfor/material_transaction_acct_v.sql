{{
  config(
    materialized= 'table'
  )
}}

select
  'MP' as source,
  MP.*
FROM  
(SELECT DISTINCT * FROM {{ source('mp_infor', 'material_tran_acct') }}) MP 
UNION ALL
select
  'MPKB' as source,
  MPKB.*
FROM  
(SELECT DISTINCT * FROM {{ source('mp_infor', 'material_tran_acct_mpkb') }}) MPKB 
