{{
  config(
    materialized= 'table'
  )
}}

SELECT DISTINCT
  a.Item,
  a.Description,
  a.Charfld1,
  a.Overview,
  a.ProductCode,
  a.um,
  d.Description as ProductCodeDescription
FROM {{ source('mp_infor', 'items') }} a
  LEFT JOIN {{ source('mp_infor', 'customer_data') }} c ON a.Item = c.Item
  LEFT JOIN {{ source('mp_infor', 'product_codes_BQ') }} d ON a.ProductCode = d.ProductCode
UNION ALL
SELECT DISTINCT
  a.Item,
  a.Description,
  a.Charfld1,
  a.Overview,
  a.ProductCode,
  a.um,
  d.Description as ProductCodeDescription
FROM {{ source('mp_infor', 'items_jembrana') }} a
  LEFT JOIN {{ source('mp_infor', 'product_codes_jembrana') }} d ON a.ProductCode = d.ProductCode
  -- Where a.Item = '570200002'
UNION ALL
SELECT distinct
  a.Item,
  a.Description,
  a.Charfld1,
  a.Overview,
  a.ProductCode,
  a.um,
  d.Description as ProductCodeDescription
FROM {{ source('mp_infor', 'items_mpkb') }} a
  LEFT JOIN {{ source('mp_infor', 'product_codes_mpkb') }} d ON a.ProductCode = d.ProductCode
-- Where a.Item = '560200024'