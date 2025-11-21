{{
  config(
    materialized= 'table'
  )
}}

SELECT DISTINCT
  COALESCE(a.CustName, b.Customer) AS Customer
FROM {{ source('mp_infor', 'salestransaction') }} a
  FULL JOIN {{ ref('sales_budget_v_fix') }} b ON a.CustName = b.Customer
