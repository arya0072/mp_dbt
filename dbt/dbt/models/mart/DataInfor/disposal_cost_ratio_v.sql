{{
  config(
    materialized= 'table'
  )
}}

SELECT
  a.Month,
  a.Year,
  a.ue_AcctDesc,
  a.ue_Item,
  a.ue_ItemDesc,
  a.Site,
  SUM(a.ue_matl_QtyEnding + a.ue_Lbr_QtyEnding + a.ue_Matl_Beginning + a.ue_Lbr_Beginning) / 2 AS Average_inventory,
  SUM(COALESCE(b.disposal_value, 0)+ COALESCE(c.disposal_value,0)) AS disposal_value
FROM {{ ref('movement_value_new_v') }} a
LEFT JOIN (
  SELECT
    'MP' as Site,
    b.Item,
    CASE 
      WHEN EXTRACT(MONTH FROM b.TransDate) = 1 THEN 'JAN'
      WHEN EXTRACT(MONTH FROM b.TransDate) = 2 THEN 'FEB'
      WHEN EXTRACT(MONTH FROM b.TransDate) = 3 THEN 'MAR'
      WHEN EXTRACT(MONTH FROM b.TransDate) = 4 THEN 'APR'
      WHEN EXTRACT(MONTH FROM b.TransDate) = 5 THEN 'MAY'
      WHEN EXTRACT(MONTH FROM b.TransDate) = 6 THEN 'JUN'
      WHEN EXTRACT(MONTH FROM b.TransDate) = 7 THEN 'JUL'
      WHEN EXTRACT(MONTH FROM b.TransDate) = 8 THEN 'AUG'
      WHEN EXTRACT(MONTH FROM b.TransDate) = 9 THEN 'SEP'
      WHEN EXTRACT(MONTH FROM b.TransDate) = 10 THEN 'OCT'
      WHEN EXTRACT(MONTH FROM b.TransDate) = 11 THEN 'NOV'
      WHEN EXTRACT(MONTH FROM b.TransDate) = 12 THEN 'DEC'
    END AS  Month,
    EXTRACT(YEAR FROM b.TransDate) AS Year,
    SUM(b.MatlTranViewTotalPost) AS disposal_value
FROM {{ ref('inventory_reasoncode_v') }} b 
WHERE b.ReasonCode = 'DPS'
-- AND b.Item ='202800192'
GROUP BY EXTRACT(YEAR FROM b.TransDate), Month, b.Item, Site
) b ON CAST(a.Month AS STRING) = CAST(b.Month AS STRING)
   AND CAST(a.Year AS STRING) = CAST(b.Year AS STRING)
   AND a.ue_Item = b.Item
   AND a.Site = b.Site
-- WHERE a.Year = '2023'
LEFT JOIN (
  SELECT
    'MPKB' as Site,
    c.Item,
    CASE 
      WHEN EXTRACT(MONTH FROM c.TransDate) = 1 THEN 'JAN'
      WHEN EXTRACT(MONTH FROM c.TransDate) = 2 THEN 'FEB'
      WHEN EXTRACT(MONTH FROM c.TransDate) = 3 THEN 'MAR'
      WHEN EXTRACT(MONTH FROM c.TransDate) = 4 THEN 'APR'
      WHEN EXTRACT(MONTH FROM c.TransDate) = 5 THEN 'MAY'
      WHEN EXTRACT(MONTH FROM c.TransDate) = 6 THEN 'JUN'
      WHEN EXTRACT(MONTH FROM c.TransDate) = 7 THEN 'JUL'
      WHEN EXTRACT(MONTH FROM c.TransDate) = 8 THEN 'AUG'
      WHEN EXTRACT(MONTH FROM c.TransDate) = 9 THEN 'SEP'
      WHEN EXTRACT(MONTH FROM c.TransDate) = 10 THEN 'OCT'
      WHEN EXTRACT(MONTH FROM c.TransDate) = 11 THEN 'NOV'
      WHEN EXTRACT(MONTH FROM c.TransDate) = 12 THEN 'DEC'
    END AS  Month,
    EXTRACT(YEAR FROM c.TransDate) AS Year,
    SUM(c.MatlTranViewTotalPost) AS disposal_value
FROM {{ ref('inventory_reasoncode_mpkb_v') }} c 
WHERE c.ReasonCode = 'DPS'
-- AND c.Item ='202800192'
GROUP BY EXTRACT(YEAR FROM c.TransDate), Month, c.Item, Site
) c ON CAST(a.Month AS STRING) = CAST(c.Month AS STRING)
   AND CAST(a.Year AS STRING) = CAST(c.Year AS STRING)
   AND a.ue_Item = c.Item
   AND a.Site = c.Site
-- WHERE a.Year = '2023'
-- WHERE a.ue_Item='202800192' AND a.Month='SEP'
GROUP BY a.Month, a.Year, a.ue_AcctDesc, a.ue_Item,a.ue_ItemDesc, a.Site, b.disposal_value
