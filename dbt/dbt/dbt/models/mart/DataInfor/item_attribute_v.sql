{{
  config(
    materialized= 'table'
  )
}}

select
 item_att.AttributeValue,
 item_att.AttributeLabel,
 item_att.DerMessage,
 item_att.CharValue,
 item_att.DecimalValue,
 item_att.LogicalValue,
 item_att.ValColName,
 item_att.Type,
 item_att.AttrGroup,
 item_att.AttrGroupClass,
 item.description,
 item.ProductCode as Product_codes,
 prod_codes.Description as ProductcodeDescription
from {{ source('mp_infor', 'item_attribute_all') }} item_att
  LEFT JOIN {{ source('mp_infor', 'items') }} item ON item_att.RefRowPointer = item.RowPointer
  LEFT JOIN {{ source('mp_infor', 'product_codes_BQ') }} prod_codes ON item.ProductCode = prod_codes.ProductCode