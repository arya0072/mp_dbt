{{
  config(
    materialized= 'table'
  )
}}

select 
  'Absorbed' as type,
  TIMESTAMP(ue_TransDate) as ue_TransDate,
  ue_item,
  ue_Job,
  ue_Qty,
  ue_TotalMaterialCost
from {{ source('mp_infor', 'material_usage') }} 
UNION ALL
select 
  'Actual' as type,
  ue_TransDate,
  ue_item,
  ue_Job,
  ue_Qty,
  ue_TotalMaterialCost
from {{ source('mp_infor', 'cost_of_production') }} 