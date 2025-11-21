{{
  config(
    materialized= 'table'
  )
}}

select 
    gabung.ue_Qty,
    gabung.ue_Item,
    gabung.ue_ItemType,
    gabung.ue_WCDesc,
    gabung.ue_ProductCode,
    gabung.ue_Job,
    gabung.ue_TotalMaterialCost,
    gabung.ue_TotalLaborCost,
    gabung.ue_TransDate,
    gabung.ue_ConesPerSC,
    gabung.category,
    avg(gabung.Qty_PcsItem) as Qty_PcsItem
from
(
SELECT 
    a.ue_Qty,
    a.ue_Item,
    a.ue_ItemType,
    a.ue_WCDesc,
    a.ue_Job,
    a.ue_ProdCodeDesc,
    a.ue_ProductCode,
    a.ue_TotalMaterialCost,
    a.ue_TotalLaborCost,
    a.ue_TransDate,
    b.ue_ConesPerSC,
    CASE
    WHEN a.ue_ItemType = 'Item' THEN a.ue_Qty * b.ue_ConesPerSC
    WHEN a.ue_ItemType <> 'Item' 
        THEN (select SUM(c.ue_Qty * d.ue_ConesPerSC) 
              from {{ source('mp_infor', 'cost_of_production') }} c 
                JOIN  {{ source('mp_infor', 'item_atrribute') }} d ON  c.ue_Item = d.ue_Item 
              where c.ue_Job = a.ue_Job
                AND c.ue_ItemType = 'Item'                
                )
END AS Qty_PcsItem,
case
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('PCK') AND a.ue_ItemType = 'Material' THEN 'Packaging'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SUN') AND a.ue_ItemType = 'Material' THEN 'Packaging'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND a.ue_ItemType = 'Material' AND a.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND a.ue_ItemType = 'Material' AND a.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones' 
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD') AND a.ue_ItemType = 'Item' THEN '*Finish Goods'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SCN') AND a.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFT') AND a.ue_ItemType = 'Material' THEN 'Filter'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFM') AND a.ue_ItemType = 'Material' THEN 'Paper'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFM') AND a.ue_ItemType = 'Item' THEN '*Paper'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('UCN') AND a.ue_ItemType = 'Material' THEN 'Paper'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('UCN') AND a.ue_ItemType = 'Item' THEN '*Paper'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('PCK') AND a.ue_ItemType = 'Item' THEN '*Rubber'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('RAW') AND a.ue_ItemType = 'Material' THEN 'Material'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD') AND a.ue_ItemType = 'Material' AND a.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter' 
  else 'Labor'
 END AS category
FROM {{ source('mp_infor', 'cost_of_production') }} a
FULL JOIN {{ source('mp_infor', 'item_atrribute') }} b
    ON a.ue_Item = b.ue_Item
) gabung
group by
gabung.ue_Qty,
    gabung.ue_Item,
    gabung.ue_ItemType,
    gabung.ue_WCDesc,
    gabung.ue_ProductCode,
    gabung.ue_Job,
    gabung.ue_TotalMaterialCost,
    gabung.ue_TotalLaborCost,
    gabung.ue_TransDate,
    gabung.ue_ConesPerSC,
    gabung.category