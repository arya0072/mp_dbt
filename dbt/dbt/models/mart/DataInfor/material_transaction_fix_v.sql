{{
  config(
    materialized= 'table'
  )
}} 

select DISTINCT 
  mattrans_mp.TransDate,
  mattrans_mp.Item,
  mattrans_mp.TransType,
  mattrans_mp.RefNum,
  mattrans_mp.TransNum,
  mattrans_mp.ItmUM,
  mattrans_mp.Qty,
  mattrans_mp.Whse,
  'MP' as  site,
  transtype_mp.RefType,
  mattrans_mp.MatlCost AS MaterialCost,
  mattrans_mp.LbrCost AS LaborCost,
  mattrans_mp.FovhdCost,
  mattrans_mp.VovhdCost,
  mattrans_mp.OutCost AS OutsideCost,
  item.Description AS ItemDescription 
from {{ source('mp_infor', 'material_tran_1') }} mattrans_mp
  LEFT JOIN {{ source('mp_infor', 'items') }} item ON mattrans_mp.Item = item.Item
  LEFT JOIN (select
              a.TransNum,
              a.RefType
            from {{ source('mp_infor', 'transtype_MP') }} a) transtype_mp ON mattrans_mp.TransNum = transtype_mp.TransNum
-- Where mattrans_mp.Item = '511800029'
UNION ALL
select DISTINCT 
  mattrans_mpkb.TransDate,
  mattrans_mpkb.Item,
  mattrans_mpkb.TransType,
  mattrans_mpkb.RefNum,
  mattrans_mpkb.TransNum,
  mattrans_mpkb.ItmUM,
  mattrans_mpkb.Qty,
  mattrans_mpkb.Whse,
  'MPKB' as  site,
  transtype_mpkb.RefType,
    mattrans_mpkb.MatlCost AS MaterialCost,
  mattrans_mpkb.LbrCost AS LaborCost,
  mattrans_mpkb.FovhdCost,
  mattrans_mpkb.VovhdCost,
  mattrans_mpkb.OutCost AS OutsideCost,
  item.Description AS ItemDescription  
from {{ source('mp_infor', 'material_tran_1_mpkb') }} mattrans_mpkb
  LEFT JOIN {{ source('mp_infor', 'items') }} item ON mattrans_mpkb.Item = item.Item
  LEFT JOIN (select
              a.TransNum,
              a.RefType
            from {{ source('mp_infor', 'transtype_MPKB') }} a) transtype_mpkb ON mattrans_mpkb.TransNum = transtype_mpkb.TransNum