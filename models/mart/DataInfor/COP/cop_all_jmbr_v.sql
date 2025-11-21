{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  a.ue_Qty,
  a.ue_Item,
  b.Item_Desc,
  a.ue_ItemType,
  a.ue_WCDesc,
  a.ue_ProductCode,
  a.ue_ProdCodeDesc,
  a.ue_Job,
  Sum(a.ue_TotalMaterialCost) AS ue_TotalMaterialCost ,
  sum(a.ue_TotalLaborCost) AS ue_TotalLaborCost ,
  a.ue_TransDate,
  CASE
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('PCK') AND a.ue_ItemType = 'Material' THEN 'Packaging'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SUN') AND a.ue_ItemType = 'Material' THEN 'Packaging'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD','SCN','SCM') AND a.ue_ItemType = 'Material' AND a.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD','SCN','SCM') AND a.ue_ItemType = 'Material' AND a.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD','SCN','SCM') AND a.ue_ItemType = 'Material' AND a.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD') AND a.ue_ItemType = 'Item' THEN '*Finished Goods'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SCN','SCM') AND a.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFT') AND a.ue_ItemType = 'Material' THEN 'Filter'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('UFT') AND a.ue_ItemType = 'Material' THEN 'Filter'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('UFT') AND a.ue_ItemType = 'Item' THEN '*HRF Filter'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFT') AND a.ue_ItemType = 'Item' THEN '*HRF Filter'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFM') AND a.ue_ItemType = 'Material' THEN 'Paper'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFM') AND a.ue_ItemType = 'Item' THEN '*Paper'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('UCN','SCM') AND a.ue_ItemType = 'Material' THEN 'HRC - Cones'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('UCN') AND a.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('PCK') AND a.ue_ItemType = 'Item' THEN '*Rubber'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('RAW') AND a.ue_ItemType = 'Material' THEN 'Material'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD') AND a.ue_ItemType = 'Material' AND a.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
        ELSE 'Labor'
    END AS category,
  b.Qty_PcsItem,
  a.site
 FROM {{ source('mp_infor', 'cost_of_production_jmbr') }} a
 JOIN {{ ref('cop_qtypembagi_jmbr_v') }} b On a.ue_Job = b.ue_Job
--  where a.ue_Job = 'JOT-000004'
 group by a.ue_Qty,
          a.ue_Item,
          b.Item_Desc,
          a.ue_ItemType,
          a.ue_WCDesc,
          a.ue_ProductCode,
          a.ue_ProdCodeDesc,
          a.ue_Job,
          a.ue_TransDate,
          category,
          b.Qty_PcsItem,
          a.site