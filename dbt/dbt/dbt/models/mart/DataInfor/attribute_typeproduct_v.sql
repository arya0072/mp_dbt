{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  itm.Item, 
  att.*
FROM {{ source('mp_infor', 'item_attribute_all') }} att
  LEFT JOIN {{ source('mp_infor', 'items') }} itm ON itm.RowPointer = att.RefRowPointer
WHERE att.AttributeLabel IN ('Type', 'Cone Type')