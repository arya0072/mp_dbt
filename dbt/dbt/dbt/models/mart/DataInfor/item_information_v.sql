{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  'MP' as site,
  a.Item,
  a.Description,
  a.Charfld1,
  a.Overview,
  a.ProductCode,
  a.um,
  b.AttributeLabel,
  b.AttributeValue,
  b.DerMessage,
  b.CharValue,
  b.DecimalValue,
  b.LogicalValue,
  b.ValColName,
  b.Type,
  b.AttrGroup,
  b.AttrGroupClass,
  c.CustNum,
  c.AdrName,
  c.CustItem,
  d.Description as ProductCodeDescription
FROM {{ source('mp_infor', 'items_new') }} a
  JOIN {{ source('mp_infor', 'item_attribute_all') }} b ON a.RowPointer = b.RefRowPointer
  LEFT JOIN {{ source('mp_infor', 'customer_data') }} c ON a.Item = c.Item
  LEFT JOIN {{ source('mp_infor', 'product_codes_BQ') }} d ON a.ProductCode = d.ProductCode
-- Where a.Item = '500500904'
UNION ALL
SELECT 
  'JEMBRANA' as site,
  a.Item,
  a.Description,
  a.Charfld1,
  a.Overview,
  a.ProductCode,
  a.um,
  b.AttributeLabel,
  b.AttributeValue,
  b.DerMessage,
  b.CharValue,
  b.DecimalValue,
  b.LogicalValue,
  b.ValColName,
  b.Type,
  b.AttrGroup,
  b.AttrGroupClass,
  c.CustNum,
  c.AdrName,
  c.CustItem,
  d.Description as ProductCodeDescription
FROM {{ source('mp_infor', 'items_jembrana') }} a
  JOIN {{ source('mp_infor', 'itemsAttribute_jembrana') }} b ON a.RowPointer = b.RefRowPointer
  LEFT JOIN {{ source('mp_infor', 'customer_data') }} c ON a.Item = c.Item
  LEFT JOIN {{ source('mp_infor', 'product_codes_jembrana') }} d ON a.ProductCode = d.ProductCode
-- Where a.Item = '500500904'