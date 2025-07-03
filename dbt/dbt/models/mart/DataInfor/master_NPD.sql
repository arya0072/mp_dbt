{{
  config(
    materialized= 'table'
  )
}}

SELECT DISTINCT
  COALESCE(a.cust_item, b.NPD) AS NPD,
  COALESCE(a.ProductcodeDescription, b.Product_Description) AS Product_Code,
  COALESCE(a.CustName, b.Customer ) AS Customer,
FROM {{ source('mp_infor', 'salestransaction') }} a
  FULL JOIN {{ ref('sales_budget_v_fix') }}  b  ON a.cust_item = b.NPD
                                               AND a.ProductcodeDescription = b.Product_Description
  FULL JOIN {{ ref('MP_Budget_Sales_v') }} c  ON a.cust_item = c.NPD
                                              AND a.ProductcodeDescription = c.PCDescription
WHERE a.cust_item <> '' OR b.NPD IS NOT NULL OR c.NPD IS NOT NULL