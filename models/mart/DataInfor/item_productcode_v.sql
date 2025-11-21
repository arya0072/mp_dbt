{{
  config(
    materialized= 'table'
  )
}}

SELECT 
 a.Item,
 a.Description,
 a.Charfld1 AS Customer_Code,
 a.Overview,
 a.ProductCode,
 a.UM,
 a.ue_MP102_GetProdCategory,
 b.Description AS ProductcodeDescription,
 b.prodcodeUf_MP10_ItemPrefix
FROM {{ source('mp_infor', 'items_new') }} a
  LEFT JOIN {{ source('mp_infor', 'product_codes_BQ') }} b ON a.ProductCode = b.ProductCode
-- Where Item = '530100121'

