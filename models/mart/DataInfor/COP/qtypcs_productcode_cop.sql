{{
  config(
    materialized= 'table'
  )
}}

SELECT a. ue_Job,
a.ue_ItemType,
a.ue_ProdCodeDesc,
a.ue_WCDesc,
a.ue_Item,
a.ue_ItemDesc
 FROM {{ source('mp_infor', 'cost_of_production_mp_new') }} a
 WHERE ue_ItemType = 'Item' AND ue_whse = 'MP'