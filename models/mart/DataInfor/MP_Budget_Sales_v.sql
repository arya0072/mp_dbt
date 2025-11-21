{{
  config(
    materialized= 'table'
  )
}}

SELECT  
  ProductType,
  QtyPcs,
  TotalDisc,
  TotalPrice,
  ChartDescription,
  CurrCode,
  CustName,
  CustNum,
  DiscAcct,
  DiscAcctUnit1,
  DiscAcctUnit2,
  DiscChaDescription,
  Discount,
  ExchRate,
  (Discount * ExchRate * Qty) AS Total_Discount,
  Item,
  ItemDescription,
  NPD,
  PCDescription,
  ProductCode,
  Qty,
  SalesAcct,
  (UnitPrice * ExchRate * Qty) AS Total_SalesGross,
  SalesAcctUnit1,
  SalesAcctUnit2,
  UM,
  UnitPrice,
  Period,
  Year,
  Status,
  PARSE_DATE('%d %b %Y', 
    CONCAT(
      '1 ', 
      CASE 
        WHEN CAST(Period AS STRING) = '1' THEN 'Jan'
        WHEN CAST(Period AS STRING) = '2' THEN 'Feb'
        WHEN CAST(Period AS STRING) = '3' THEN 'Mar'
        WHEN CAST(Period AS STRING) = '4' THEN 'Apr'
        WHEN CAST(Period AS STRING) = '5' THEN 'May'
        WHEN CAST(Period AS STRING) = '6' THEN 'Jun'
        WHEN CAST(Period AS STRING) = '7' THEN 'Jul'
        WHEN CAST(Period AS STRING) = '8' THEN 'Aug'
        WHEN CAST(Period AS STRING) = '9' THEN 'Sep'
        WHEN CAST(Period AS STRING) = '10' THEN 'Oct'
        WHEN CAST(Period AS STRING) = '11' THEN 'Nov'
        WHEN CAST(Period AS STRING) = '12' THEN 'Dec'
      END,
      ' ', CAST(Year AS STRING)
    )
  ) AS Date
FROM {{ source('mp_infor', 'budget_sales') }} 
