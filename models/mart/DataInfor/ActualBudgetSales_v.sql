{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  'BUDGET' AS Source,
  CAST(bud.Year AS INT64) AS Year,
  CAST(bud.Period AS INT64) AS Month,
  bud.CustNum,
  bud.CustName,
  SUM(bud.QtyPcs) AS QtyPcs
FROM {{ source('mp_infor', 'budget_sales') }} bud
WHERE CAST(bud.Year AS INT64) >= 2024
  -- AND bud.CustNum = 'HBI-001' 
GROUP BY 
  CAST(bud.Year AS INT64),
  CAST(bud.Period AS INT64),
  bud.CustNum,
  bud.CustName

UNION ALL

SELECT 
  'ACTUAL' AS Source,
  CAST(FORMAT_DATE('%Y', act.ue_inv_date) AS INT64) AS Year,
  CAST(FORMAT_DATE('%-m', act.ue_inv_date) AS INT64) AS Month,
  act.ue_cust_num AS CustNum,
  act.ue_cust_name AS CustName,
  SUM(act.ue_QtyShippedPcs) AS QtyPcs
FROM {{ ref('shipment_ItemAttributeCGS_v') }} act
WHERE act.ue_inv_date >= '2024-01-01'
  -- AND act.ue_cust_num = 'HBI-001'
GROUP BY
 CAST(FORMAT_DATE('%Y', act.ue_inv_date) AS INT64),
  CAST(FORMAT_DATE('%-m', act.ue_inv_date) AS INT64),
  act.ue_cust_num,
  act.ue_cust_name

