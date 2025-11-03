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
  b.COGSCost,
  b.TotalMatlCost,
  b.TotalLaborCost,
  b.TotalFovhdCost,
  b.TotalVovhdCost,
  b.TotalOutCost,
  b.TotalCOGSCost,
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
      ' ', CAST(a.Year AS STRING)
    )
  ) AS Date
FROM {{ source('mp_infor', 'budget_sales') }} a
  LEFT JOIN (SELECT
              DISTINCT
              Item,
              Year,
              AVG(MatlCost) AS MatlCost,
              AVG(LaborCost) AS LaborCost,
              AVG(FovhdCost) AS FovhdCost,
              AVG(VovhdCost) AS VovhdCost,
              AVG(OutCost) AS OutCost,
              SUM(TotalMatlCost) AS TotalMatlCost,
              SUM(TotalLbrCost) AS TotalLaborCost,
              SUM(TotalFovhdCost) AS TotalFovhdCost,
              SUM(TotalVovhdCost) AS TotalVovhdCost,
              SUM(TotalOutCost) AS TotalOutCost,
              AVG(MatlCost+LaborCost+FovhdCost+VovhdCost+OutCost) AS COGSCost,
              SUM(TotalMatlCost+TotalLbrCost+TotalFovhdCost+TotalVovhdCost+TotalOutCost) AS TotalCOGSCost
              FROM {{ source('mp_infor', 'budget_cogs') }} 
              GROUP BY Item,
              Year ) b ON a.Item = b.Item 
              AND a.Year = b.Year
