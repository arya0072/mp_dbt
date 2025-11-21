{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  'Absorbed' AS type,
  actual.type AS type_actual,
  COALESCE(TIMESTAMP(absorbed.ue_TransDate), actual.ue_TransDate) AS trans_date,
  absorbed.ue_item,
  absorbed.ue_ItemDesc,
  absorbed.ue_ItemType,
  absorbed.ue_Job,
  absorbed.ue_WCDesc,
  standard.itmUf_MP123_StandartCost,
  absorbed.ue_Qty AS ue_qty_absorbed,
  actual.ue_Qty AS ue_qty_actual,
  absorbed.ue_TotalMaterialCost AS total_material_cost_absorbed,
  actual.ue_TotalMaterialCost AS total_material_cost_actual,
  actual.item_fg AS item_fg,
  actual.itemdesc_fg AS itemdesc_fg,
  absorbed.ue_whse
FROM {{ source('mp_infor', 'material_usage_mp_new') }} AS absorbed
LEFT JOIN (
  SELECT 
    'Actual' AS type,
    ue_TransDate,
    ue_item,
    ue_ItemDesc,
    ue_ItemType,
    ue_Job,
    ue_WCDesc,
    ue_Qty,
    ue_TotalMaterialCost,
    CASE
        WHEN ue_ItemType = 'Item' THEN ue_item
        WHEN ue_ItemType <> 'Item'
        THEN (
            SELECT MAX(ue_item)
            FROM {{ source('mp_infor', 'cost_of_production_mp_new') }} a
            WHERE b.ue_Job = a.ue_Job
            AND a.ue_ItemType = 'Item'
            AND a.ue_whse = 'MP'
        )
    END AS item_fg,
    CASE
        WHEN ue_ItemType = 'Item' THEN ue_ItemDesc
        WHEN ue_ItemType <> 'Item'
        THEN (
            SELECT MAX(ue_ItemDesc)
            FROM {{ source('mp_infor', 'cost_of_production_mp_new') }} a
            WHERE b.ue_Job = a.ue_Job
            AND a.ue_ItemType = 'Item'
            AND a.ue_whse = 'MP'
        )
    END AS itemdesc_fg
FROM {{ source('mp_infor', 'cost_of_production_mp_new') }} b
) actual ON absorbed.ue_item = actual.ue_item AND absorbed.ue_Job = actual.ue_job
LEFT JOIN (
  SELECT
    itmUf_MP123_StandartCost,
    Item
  FROM {{ source('mp_infor', 'standard_cost') }}
) standard ON absorbed.ue_item = standard.Item
-- WHERE absorbed.ue_job='JSFG-35358' 
-- and absorbed.ue_item='Labor'
