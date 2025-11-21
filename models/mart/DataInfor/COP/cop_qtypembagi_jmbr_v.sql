{{
  config(
    materialized= 'table'
  )
}}

SELECT DISTINCT
  t.ue_Job,
  t.ue_ItemType,
  CASE
    WHEN t.ue_ItemType = 'Item' THEN t.ue_ProdCodeDesc
    ELSE (
      SELECT MAX(a.ue_ProdCodeDesc) 
      FROM {{ source('mp_infor', 'cost_of_production_jmbr_new') }} a
      WHERE a.ue_Job = t.ue_Job
      AND a.ue_ItemType = 'Item'
    )
  END AS ue_ProdCodeDesc,
  CASE
    WHEN t.ue_ItemType = 'Item' THEN t.ue_Item
    ELSE (
      SELECT MAX(a.ue_Item) 
      FROM {{ source('mp_infor', 'cost_of_production_jmbr_new') }} a
      WHERE a.ue_Job = t.ue_Job
      AND a.ue_ItemType = 'Item'
    )
  END AS Item_fg,
  CASE
    WHEN t.ue_ItemType = 'Item' THEN t.ue_ItemDesc
    ELSE (
      SELECT MAX(a.ue_ItemDesc) 
      FROM {{ source('mp_infor', 'cost_of_production_jmbr_new') }} a
      WHERE a.ue_Job = t.ue_Job
      AND a.ue_ItemType = 'Item'
    )
  END AS Item_Desc,
  CASE
    WHEN t.ue_ItemType = 'Item' THEN t.ue_Qty * COALESCE(d.DecimalValue, 1)
    WHEN t.ue_ItemType <> 'Item'
    THEN (
      SELECT SUM(c.ue_Qty * coalesce( d.DecimalValue, 1))
      FROM {{ source('mp_infor', 'cost_of_production_jmbr_new') }}  c
      JOIN {{ ref('item_attributeall_jembrana_v') }} d ON c.ue_Item = d.Item
      WHERE c.ue_Job = t.ue_Job
      AND c.ue_ItemType = 'Item'
    )
  END AS Qty_PcsItem
FROM {{ source('mp_infor', 'cost_of_production_jmbr_new') }} t
LEFT JOIN {{ ref('item_attributeall_jembrana_v') }} d ON t.ue_Item = d.Item

WHERE CASE
    WHEN t.ue_ItemType = 'Item' THEN t.ue_Qty * COALESCE(d.DecimalValue, 1)
    WHEN t.ue_ItemType <> 'Item'
    THEN (
      SELECT SUM(c.ue_Qty * coalesce( d.DecimalValue, 1))
      FROM {{ source('mp_infor', 'cost_of_production_jmbr_new') }} c
      JOIN {{ ref('item_attributeall_jembrana_v') }} d ON c.ue_Item = d.Item
      WHERE c.ue_Job = t.ue_Job
      AND c.ue_ItemType = 'Item'
    )
  END IS NOT NULL 
  -- AND t.ue_Job = 'JOS-000763';
UNION ALL
SELECT 
  a.ue_Job,
  a.ue_ItemType,
  a.ue_ProdCodeDesc,
  a.ue_Item,
  a.ue_ItemDesc,
  CASE
    WHEN a.ue_ItemType = 'Item' THEN a.ue_Qty * COALESCE(b.DecimalValue, 1)
    ELSE (
      SELECT SUM(c.ue_Qty * COALESCE(d.DecimalValue, 1))
      FROM {{ source('mp_infor', 'cost_of_production_mp_new') }} c
      JOIN {{ ref('item_attributeall_v') }} d 
        ON c.ue_Item = d.Item
      WHERE c.ue_Job = a.ue_Job
        AND c.ue_ItemType = 'Item'
    )
  END AS Qty_PcsItem
FROM {{ source('mp_infor', 'cost_of_production_mp_new') }} a
LEFT JOIN {{ ref('item_attributeall_v') }} b 
  ON a.ue_Item = b.Item
WHERE a.ue_ItemType = 'Item' 
  AND a.ue_whse = 'PRJM'
