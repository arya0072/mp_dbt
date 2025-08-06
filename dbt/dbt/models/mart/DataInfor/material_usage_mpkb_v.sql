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
  absorbed.ue_ItemType,
  absorbed.ue_WCDesc,
  absorbed.ue_Job,
  standard.itmUf_MP123_StandartCost,
  absorbed.ue_Qty AS ue_qty_absorbed,
  actual.ue_Qty AS ue_qty_actual,
  absorbed.ue_TotalMaterialCost AS total_material_cost_absorbed,
  actual.ue_TotalMaterialCost AS total_material_cost_actual,
  absorbed.ue_whse
FROM {{ source('mp_infor', 'material_usage_mp_new') }} AS absorbed 
LEFT JOIN (
  SELECT 
    'Actual' AS type,
    ue_TransDate,
    ue_item,
    ue_ItemType,
    ue_WCDesc,
    ue_Job,
    ue_Qty,
    ue_TotalMaterialCost
  FROM {{ source('mp_infor', 'cost_of_production_mp_new') }} 
  where ue_Qty <> 0 and ue_whse IN ('KBPR','MPKB')
) actual ON absorbed.ue_item = actual.ue_item AND absorbed.ue_Job = actual.ue_job 
LEFT JOIN (
  SELECT
    itmUf_MP123_StandartCost,
    Item
  FROM {{ source('mp_infor', 'standard_cost') }}
) standard ON absorbed.ue_item = standard.Item
where ue_whse IN ('KBPR','MPKB')
UNION ALL
SELECT 
  'Absorbed' AS type,
  actual.type AS type_actual,
  COALESCE(TIMESTAMP(absorbed.ue_TransDate), actual.ue_TransDate) AS trans_date,
  absorbed.ue_item,
  absorbed.ue_ItemType,
  absorbed.ue_WCDesc,
  absorbed.ue_Job,
  standard.itmUf_MP123_StandartCost,
  absorbed.ue_Qty AS ue_qty_absorbed,
  actual.ue_Qty AS ue_qty_actual,
  absorbed.ue_TotalMaterialCost AS total_material_cost_absorbed,
  actual.ue_TotalMaterialCost AS total_material_cost_actual,
  absorbed.ue_whse
FROM {{ source('mp_infor', 'material_usage_mpkb') }} AS absorbed
LEFT JOIN (
  SELECT 
    'Actual' AS type,
    ue_TransDate,
    ue_item,
    ue_ItemType,
    ue_WCDesc,
    ue_Job,
    ue_Qty,
    ue_TotalMaterialCost
  FROM {{ source('mp_infor', 'cost_of_production_mpkb') }} 
) actual ON absorbed.ue_item = actual.ue_item AND absorbed.ue_Job = actual.ue_job
LEFT JOIN (
  SELECT
    itmUf_MP123_StandartCost,
    Item
  FROM {{ source('mp_infor', 'standard_cost_mpkb') }} 
) standard ON absorbed.ue_item = standard.Item
-- WHERE absorbed.ue_job='JSFG-35358' 
-- and absorbed.ue_item='Labor'
