{{
  config(
    materialized= 'table'
  )
}}

SELECT
  a.trans_date,
  EXTRACT(YEAR FROM a.trans_date) AS year,
  EXTRACT(MONTH FROM a.trans_date) AS month,
  a.ue_item,
  b.ProductCode,
  a.ue_ItemType,
  a.type,
  a.ue_Job,
  c.scrap
FROM {{ ref('material_usage_mp_fix') }} a
  JOIN {{ ref('item_productcode_v') }} b ON a.ue_item = b.Item
  JOIN {{ source('mp_infor', 'mp123_scraps') }} c ON b.ProductCode = c.ProductCode
                                AND EXTRACT(YEAR FROM a.trans_date) = CAST(c.Years AS INT64)
