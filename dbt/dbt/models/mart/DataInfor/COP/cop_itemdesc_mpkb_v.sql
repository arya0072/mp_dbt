{{
  config(
    materialized= 'table'
  )
}}


SELECT DISTINCT
  ue_Job,
  CASE
    WHEN ue_ItemType = 'Item' THEN ue_ProdCodeDesc
    ELSE (
      SELECT MAX(a.ue_ProdCodeDesc) 
      FROM {{ source('mp_infor', 'cost_of_production_mpkb_new') }}  a
      WHERE a.ue_Job = t.ue_Job
      AND a.ue_ItemType = 'Item'
    )
  END AS ue_ProdCodeDesc,
  CASE
    WHEN ue_ItemType = 'Item' THEN ue_ItemDesc
    ELSE (
      SELECT MAX(a.ue_ItemDesc) 
      FROM {{ source('mp_infor', 'cost_of_production_mpkb_new') }} a
      WHERE a.ue_Job = t.ue_Job
      AND a.ue_ItemType = 'Item'
    )
  END AS Item_Desc,
  CASE
    WHEN ue_ItemType = 'Item' THEN ue_Qty
    ELSE (
      SELECT SUM(b.ue_Qty)
      FROM {{ source('mp_infor', 'cost_of_production_mpkb_new') }} b
      WHERE b.ue_Job = t.ue_Job
      AND b.ue_ItemType = 'Item'
    )
  END AS Qty_PcsItem
FROM {{ source('mp_infor', 'cost_of_production_mpkb_new') }} t
-- WHERE ue_Job IN ('JACN-00222','JACN-00223')
UNION ALL
SELECT DISTINCT
  ue_Job,
  CASE
    WHEN ue_ItemType = 'Item' THEN ue_ProdCodeDesc
    ELSE (
      SELECT MAX(a.ue_ProdCodeDesc) 
      FROM {{ source('mp_infor', 'cost_of_production_mp_new') }}  a
      WHERE a.ue_Job = t.ue_Job
      AND a.ue_ItemType = 'Item'
    )
  END AS ue_ProdCodeDesc,
  CASE
    WHEN ue_ItemType = 'Item' THEN ue_ItemDesc
    ELSE (
      SELECT MAX(a.ue_ItemDesc) 
      FROM {{ source('mp_infor', 'cost_of_production_mp_new') }} a
      WHERE a.ue_Job = t.ue_Job
      AND a.ue_ItemType = 'Item'
    )
  END AS Item_Desc,
  CASE
    WHEN ue_ItemType = 'Item' THEN ue_Qty
    ELSE (
      SELECT SUM(b.ue_Qty)
      FROM {{ source('mp_infor', 'cost_of_production_mp_new') }}  b
      WHERE b.ue_Job = t.ue_Job
      AND b.ue_ItemType = 'Item'
    )
  END AS Qty_PcsItem
FROM {{ source('mp_infor', 'cost_of_production_mp_new') }} t
WHERE ue_whse IN ('KBPR')