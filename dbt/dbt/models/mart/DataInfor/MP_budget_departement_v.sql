{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  TotalPrice,
  Acct,
  AcctUnit1,
  AcctUnit4,
  BudgetCatDesc,
  ChartDescription,
  CurrCode,
  ExchRate,
  NonItmDescription,
  Note,
  Qty,
  UnitPrice,
  Year,
  Period,
  BudgetCat,
  Item,
  Status,
  (Qty * UnitPrice * ExchRate) AS Total_Budget
FROM {{ source('mp_infor', 'budget_departement') }} 
GROUP BY 
  TotalPrice,
  Acct,
  AcctUnit1,
  AcctUnit4,
  BudgetCatDesc,
  ChartDescription,
  CurrCode,
  ExchRate,
  NonItmDescription,
  Note,
  Qty,
  UnitPrice,
  Year,
  Period,
  BudgetCat,
  Item,
  Status