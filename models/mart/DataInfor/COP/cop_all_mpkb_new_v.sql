{{
  config(
    materialized= 'table'
  )
}}

select
    gabung.ue_Qty,
    gabung.ue_Item,
    gabung.ue_ItemDesc AS Item_Desc,
    gabung.ue_ItemType,
    gabung.ue_WCDesc,
    gabung.ue_ProductCode,
    gabung.ue_ProdCodeDesc,
    gabung.ue_Job,
    gabung.ue_TotalMaterialCost,
    gabung.ue_TotalLaborCost,
    gabung.ue_TotalVovhdCost,
    gabung.ue_TotalFovhdCost,
    gabung.ue_TransDate,
    gabung.category,
    AVG(gabung.Qty_PcsItem) AS Qty_PcsItem,
    gabung.site,
    AVG(gabung.Qty_SC) AS Qty_SC,
    gabung.ue_ItemDesc,
    gabung.ue_um
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
    a.ue_TotalVovhdCost,
    a.ue_TotalFovhdCost,
    a.ue_TransDate,
    a.site,
    a.ue_whse,
    CASE
    WHEN a.ue_ItemType = 'Item' THEN a.ue_Qty * coalesce(item.DecimalValue,1)
    WHEN a.ue_ItemType <> 'Item'
    THEN (
        SELECT COALESCE(SUM(c.ue_Qty * d.DecimalValue), SUM(c.ue_Qty * 1))
        FROM {{ source('mp_infor', 'cost_of_production_mp_new') }}  c
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
        FROM {{ source('mp_infor', 'cost_of_production_mp_new') }} c
        left JOIN {{ ref('item_attributeall_v') }} d ON c.ue_Item = d.Item
        WHERE c.ue_Job = a.ue_Job
        AND c.ue_ItemType = 'Item'
    )
    END AS Qty_SC,
    CASE
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('PCK') AND a.ue_ItemType = 'Material' THEN 'Packaging'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SUN') AND a.ue_ItemType = 'Material' THEN 'Packaging'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD') AND a.ue_ItemType = 'Material' THEN 'Material' 
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD') AND a.ue_ItemType = 'Item' THEN '*Finished Goods'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFG','SFM','SKB') AND a.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFG','SFM','SKB') AND a.ue_ItemType = 'Material' THEN 'Material'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('RAW') AND a.ue_ItemType = 'Material' THEN 'Material'
        ELSE 'Labor'
  END AS category,
    a.ue_ItemDesc,
    a.ue_um
FROM {{ source('mp_infor', 'cost_of_production_mp_new') }} a
LEFT JOIN (SELECT 
            DISTINCT
            item.Item,
            item_attribute.DecimalValue
        FROM {{ source('mp_infor', 'items') }} item
        LEFT JOIN {{ source('mp_infor', 'item_attribute_all') }} item_attribute ON item.RowPointer = item_attribute.RefRowPointer
        WHERE item_attribute.AttributeLabel = 'Cones per SC'
    ) item ON a.ue_Item = item.Item
--  where a.ue_Job = 'JSM-000840'
) gabung
 WHERE (gabung.ue_TotalMaterialCost <> 0 OR gabung.ue_TotalLaborCost <>0)
  AND gabung.ue_whse LIKE '%KB%' -- hanya lokasi MPKB
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
    gabung.ue_TotalVovhdCost,
    gabung.ue_TotalFovhdCost,
    gabung.ue_TransDate,
    gabung.category,
    gabung.Qty_SC,
    gabung.site,
    gabung.ue_ItemDesc,
    gabung.ue_um,
    gabung.ue_whse
---------------------------------------------------------------------------------------------------------------------------------------
UNION ALL
---------------------------------------------------------------------------------------------------------------------------------------
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
  sum(a.ue_TotalVovhdCost) AS ue_TotalVovhdCost ,
  sum(a.ue_TotalFovhdCost) AS ue_TotalFovhdCost,
  a.ue_TransDate,
  CASE
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('PCK') AND a.ue_ItemType = 'Material' THEN 'Packaging'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SUN') AND a.ue_ItemType = 'Material' THEN 'Packaging'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD') AND a.ue_ItemType = 'Material' THEN 'Material' 
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('FGD') AND a.ue_ItemType = 'Item' THEN '*Finished Goods'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFG','SFM','SKB') AND a.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('SFG','SFM','SKB') AND a.ue_ItemType = 'Material' THEN 'Material'
            when SUBSTR(a.ue_ProductCode, 1, 3) IN ('RAW') AND a.ue_ItemType = 'Material' THEN 'Material'
        ELSE 'Labor'
  END AS category,
  b.Qty_PcsItem,
  a.site,
  CASE
    WHEN a.ue_ItemType = 'Item' THEN a.ue_Qty
    WHEN a.ue_ItemType <> 'Item'
    THEN (
        SELECT COALESCE(SUM(c.ue_Qty), 0)
        FROM {{ source('mp_infor', 'cost_of_production_mp_new') }} c
        left JOIN {{ ref('item_attributeall_v') }} d ON c.ue_Item = d.Item
        WHERE c.ue_Job = a.ue_Job
        AND c.ue_ItemType = 'Item'
    )
    END AS Qty_SC,
  a.ue_ItemDesc,
  a.ue_um
 FROM {{ source('mp_infor', 'cost_of_production_mpkb_new') }} a
 JOIN {{ ref('cop_itemdesc_mpkb_v') }} b On a.ue_Job = b.ue_Job
--  where a.ue_Job = 'JACN-00222'
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
          a.site,
           a.ue_ItemDesc,
    a.ue_um