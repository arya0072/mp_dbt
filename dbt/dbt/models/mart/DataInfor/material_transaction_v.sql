{{
  config(
    materialized= 'table'
  )
}}

select 
  DISTINCT mattrans_mp.*,
  'MP' as  site,
  transtype_mp.RefType 
from {{ source('mp_infor', 'material_tran_1') }} mattrans_mp
  LEFT JOIN (select
              a.TransNum,
              a.RefType
            from {{ source('mp_infor', 'transtype_MP') }} a) transtype_mp ON mattrans_mp.TransNum = transtype_mp.TransNum
UNION ALL
select 
  DISTINCT mattrans_mpkb.*,
    'MPKB' as  site,
    transtype_mpkb.RefType  
from {{ source('mp_infor', 'material_tran_1_mpkb') }} mattrans_mpkb
  LEFT JOIN (select
              a.TransNum,
              a.RefType
            from {{ source('mp_infor', 'transtype_MPKB') }} a) transtype_mpkb ON mattrans_mpkb.TransNum = transtype_mpkb.TransNum