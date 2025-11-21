{{
  config(
    materialized= 'table'
  )
}}

SELECT DISTINCT
 a.Item,
 a.description,
 a.overview,
 a.ProductCode as Product_codes,
 prod_code.Description AS ProductcodeDescription,
 a.UM as u_m,
 a.charfld1,
 b.DecimalValue,
FROM {{ source('mp_infor', 'items') }} a
JOIN {{ source('mp_infor', 'item_attribute_all') }} b ON a.RowPointer = b.RefRowPointer
LEFT JOIN {{ source('mp_infor', 'product_codes_BQ') }}  prod_code ON a.ProductCode = prod_code.ProductCode
WHERE b.AttributeLabel = 'Unit / Bulk per SC' 
GROUP BY
 a.Item,
 a.description,
 a.overview,
 a.ProductCode,
  prod_code.Description,
 a.UM,
 a.charfld1,
 b.DecimalValue
