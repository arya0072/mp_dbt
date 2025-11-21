{{
  config(
    materialized= 'table'
  )
}}

SELECT
  'Outstanding' AS source,
  a.ue_cust_item AS CustItem,
  a.ue_item AS Item,
  a.ue_cust_num,
  EXTRACT(YEAR FROM a.ue_PromiseDate) as Year,
  EXTRACT(MONTH FROM a.ue_PromiseDate) as Month,
  SUM(a.ue_qtyorderedinpcs) as QtyOrdered
FROM {{ ref('outstanding_itemattribute_v') }} a
GROUP BY 
  source,
  a.ue_cust_item,
  a.ue_item,
  a.ue_cust_num,
  EXTRACT(YEAR FROM a.ue_PromiseDate),
  EXTRACT(MONTH FROM a.ue_PromiseDate)
UNION ALL
SELECT
  'BudgetSales' AS source,
  a.npd,
  a.Item,
  a.CustNum,
  CAST(a.Year AS INT64),
  CAST(a.period AS INT64),
  SUM(a.QtyPcs)
FROM {{ source('mp_infor', 'budget_sales') }} a
GROUP BY 
  source,
  a.npd,
  a.Item,
  a.CustNum,
  CAST(a.Year AS INT64),
  CAST(a.period AS INT64)