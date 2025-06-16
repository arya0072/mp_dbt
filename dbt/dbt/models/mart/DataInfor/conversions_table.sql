{{
  config(
    materialized= 'table'
  )
}}

WITH Ranked AS (
  SELECT 
    a.Item,
    a.description,
    a.overview,
    a.ProductCode AS Product_codes,
    prod_code.Description AS ProductcodeDescription,
    a.UM AS UOM,
    a.charfld1 AS Cust_Id,
    b.DecimalValue AS Convertion,
    ROW_NUMBER() OVER (PARTITION BY a.Item ORDER BY b.DecimalValue DESC) AS rn
  FROM {{ source('mp_infor', 'items') }} a
  JOIN {{ source('mp_infor', 'item_attribute_all') }} b ON a.RowPointer = b.RefRowPointer
  LEFT JOIN {{ source('mp_infor', 'product_codes_BQ') }} prod_code ON a.ProductCode = prod_code.ProductCode
)
SELECT Item, description, overview, Product_codes, ProductcodeDescription, UOM, Cust_Id, Convertion
FROM Ranked
WHERE rn = 1

