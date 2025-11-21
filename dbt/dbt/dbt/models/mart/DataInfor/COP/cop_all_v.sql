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
    gabung.ue_ProdCodeDesc,
    gabung.ue_Job,
    gabung.ue_TotalMaterialCost,
    gabung.ue_TotalLaborCost,
    gabung.ue_TransDate,
    gabung.category,
    AVG(gabung.Qty_PcsItem) AS Qty_PcsItem,
    AVG(gabung.Qty_SC) AS Qty_SC,
    gabung.site
from
(SELECT 
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
    a.site,
    CASE
    WHEN a.ue_ItemType = 'Item' THEN a.ue_Qty * coalesce(item.DecimalValue,1)
    WHEN a.ue_ItemType <> 'Item'
    THEN (
        SELECT COALESCE(SUM(c.ue_Qty * d.DecimalValue), 0)
        FROM {{ source('mp_infor', 'cost_of_production') }} c
        left JOIN {{ ref('item_attributeall_v') }} d ON c.ue_Item = d.Item
        WHERE c.ue_Job = a.ue_Job
        AND c.ue_ItemType = 'Item'
    )
    END AS Qty_PcsItem,
    CASE
    WHEN a.ue_ItemType = 'Item' THEN a.ue_Qty
    WHEN a.ue_ItemType <> 'Item'
    THEN (
        SELECT COALESCE(SUM(c.ue_Qty), 0)
        FROM {{ source('mp_infor', 'cost_of_production') }} c
        left JOIN {{ ref('item_attributeall_v') }} d ON c.ue_Item = d.Item
        WHERE c.ue_Job = a.ue_Job
        AND c.ue_ItemType = 'Item'
    )
    END AS Qty_SC,
    CASE
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('PCK') AND a.ue_ItemType = 'Material' THEN 'Packaging'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SUN') AND a.ue_ItemType = 'Material' THEN 'Packaging'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND a.ue_ItemType = 'Material' AND a.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND a.ue_ItemType = 'Material' AND a.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND a.ue_ItemType = 'Material' AND a.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD') AND a.ue_ItemType = 'Item' THEN '*Finished Goods'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SCN') AND a.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFT') AND a.ue_ItemType = 'Material' THEN 'Filter'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('UFT') AND a.ue_ItemType = 'Material' THEN 'Filter'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('UFT') AND a.ue_ItemType = 'Item' THEN '*HRF Filter'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFT') AND a.ue_ItemType = 'Item' THEN '*HRF Filter'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFM') AND a.ue_ItemType = 'Material' THEN 'Paper'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFM') AND a.ue_ItemType = 'Item' THEN '*Paper'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('UCN') AND a.ue_ItemType = 'Material' THEN 'Cones'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('UCN') AND a.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('PCK') AND a.ue_ItemType = 'Item' THEN '*Rubber'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('RAW') AND a.ue_ItemType = 'Material' THEN 'Material'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD') AND a.ue_ItemType = 'Material' AND a.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
        ELSE 'Labor'
    END AS category
FROM {{ source('mp_infor', 'cost_of_production') }} a
LEFT JOIN (SELECT 
            DISTINCT
            item.Item,
            item_attribute.DecimalValue
        FROM {{ source('mp_infor', 'items') }} item
        LEFT JOIN {{ source('mp_infor', 'item_attribute_all') }} item_attribute ON item.RowPointer = item_attribute.RefRowPointer
        WHERE item_attribute.AttributeLabel = 'Cones per SC'
    ) item ON a.ue_Item = item.Item
--  where a.ue_Job = 'JFG-012465'
) gabung
 WHERE (gabung.ue_TotalMaterialCost <> 0 OR gabung.ue_TotalLaborCost <>0)
GROUP BY
    gabung.ue_Qty,
    gabung.ue_Item,
    gabung.ue_ItemType,
    gabung.ue_WCDesc,
    gabung.ue_ProductCode,
    gabung.ue_ProdCodeDesc,
    gabung.ue_Job,
    gabung.ue_TotalMaterialCost,
    gabung.ue_TotalLaborCost,
    gabung.ue_TransDate,
    gabung.category,
    gabung.Qty_SC,
    gabung.site