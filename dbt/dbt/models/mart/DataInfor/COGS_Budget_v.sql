{{
  config(
    materialized= 'table'
  )
}}

SELECT  
  a.ProductType,
  a.QtyPcs,
  a.TotalDisc,
  a.TotalPrice,
  a.ChartDescription,
  a.CurrCode,
  a.CustName,
  a.CustNum,
  a.DiscAcct,
  a.DiscAcctUnit1,
  a.DiscAcctUnit2,
  a.DiscChaDescription,
  a.Discount,
  a.ExchRate,
  (Discount * ExchRate * a.Qty) AS Total_Discount,
  a.Item,
  a.ItemDescription,
  a.NPD,
  a.PCDescription,
  a.ProductCode,
  a.Qty,
  a.SalesAcct,
  (UnitPrice * ExchRate * a.Qty) AS Total_SalesGross,
  a.SalesAcctUnit1,
  a.SalesAcctUnit2,
  a.UM,
  a.UnitPrice,
  a.Period,
  a.Year,
  a.Status,
  b.MatlCost,
  b.LaborCost,
  b.FovhdCost,
  b.VovhdCost,
  b.OutCost,
  PARSE_DATE('%d %b %Y', 
    CONCAT(
      '1 ', 
      CASE 
        WHEN CAST(a.Period AS STRING) = '1' THEN 'Jan'
        WHEN CAST(a.Period AS STRING) = '2' THEN 'Feb'
        WHEN CAST(a.Period AS STRING) = '3' THEN 'Mar'
        WHEN CAST(a.Period AS STRING) = '4' THEN 'Apr'
        WHEN CAST(a.Period AS STRING) = '5' THEN 'May'
        WHEN CAST(a.Period AS STRING) = '6' THEN 'Jun'
        WHEN CAST(a.Period AS STRING) = '7' THEN 'Jul'
        WHEN CAST(a.Period AS STRING) = '8' THEN 'Aug'
        WHEN CAST(a.Period AS STRING) = '9' THEN 'Sep'
        WHEN CAST(a.Period AS STRING) = '10' THEN 'Oct'
        WHEN CAST(a.Period AS STRING) = '11' THEN 'Nov'
        WHEN CAST(a.Period AS STRING) = '12' THEN 'Dec'
      END,
      ' ', CAST(a.Year AS STRING)
    )
  ) AS Date
FROM {{ source('mp_infor', 'budget_sales') }} a
  LEFT JOIN (SELECT
              DISTINCT
              Item,
              Year,
              Period,
              AVG(MatlCost) AS MatlCost,
              AVG(LaborCost) AS LaborCost,
              AVG(FovhdCost) AS FovhdCost,
              AVG(VovhdCost) AS VovhdCost,
              AVG(OutCost) AS OutCost
              FROM {{ source('mp_infor', 'budget_cogs') }} 
              GROUP BY 
                Item,
                Year,
                Period 
              ) b ON a.Item = b.Item 
                  AND a.Period = b.Period
                  AND a.Year = b.Year
