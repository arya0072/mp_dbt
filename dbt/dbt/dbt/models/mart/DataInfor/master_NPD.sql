{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  DISTINCT
  a.cust_item AS NPD,
  a.ProductcodeDescription AS Product_Code,
  a.CustName AS Customer
FROM {{ source('mp_infor', 'salestransaction') }} a
WHERE a.cust_item <> '' 
UNION ALL
SELECT 
  DISTINCT
  a.NPD AS NPD,
  a.Product_Description AS Product_Code,
  a.Customer AS Customer
FROM {{ ref('sales_budget_v_fix') }} a
WHERE a.NPD IS NOT NULL
UNION ALL
SELECT 
  DISTINCT
  a.NPD AS NPD,
  a.PCDescription AS Product_Code,
  a.CustName AS Customer
FROM {{ ref('MP_Budget_Sales_v') }} a
WHERE  a.NPD IS NOT NULL
