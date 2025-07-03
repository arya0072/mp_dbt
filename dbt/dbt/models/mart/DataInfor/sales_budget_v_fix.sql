{{
  config(
    materialized= 'table'
  )
}}

SELECT DISTINCT
  Tgl,
  Customer,
  Item_Code,
  NPD,
  Product_Code AS Product_Description,
  sum(Qty_Pcs) AS QtyPcs,
  Sum(Qty_Inv) AS QtyInv,
  AVG(Price) AS UnitPrice,
  sum(COALESCE(Total_Sales,0)) / sum(COALESCE(Qty_Pcs,0)) AS Price_pcs,
  AVG(Rate) AS Rate,
  sum(Total_Sales) AS Total_Sales,
  sum(Net_Sales) AS Net_Sales,
  SUM(COALESCE(Total_COGS_Material,0) + COALESCE(Total_COGS_Labor,0) + COALESCE(Total_COGS_FOvh,0) + COALESCE(Total_COGS_VOvh,0)) AS TotalCOGS,
  AVG(COALESCE(COGS_Material,0) + COALESCE(COGS_Labor,0) + COALESCE(COGS_FOvh,0) + COALESCE(COGS_VOvh,0)) AS COGS,
  SUM(COALESCE(COGS_Material,0)) AS COGSMat,
  SUM(COALESCE(COGS_Labor,0)) AS COGSLab,
  SUM(COALESCE(COGS_FOvh,0)) AS COGSFOvh,
  SUM(COALESCE(COGS_VOvh,0)) AS COGSVovh
FROM {{ source('mp_infor', 'Budget Sales Qty') }}  
WHERE Qty_Pcs > 0 
GROUP BY
    Tgl,
    Customer,
    Item_Code,
    NPD,
    Product_Code  
UNION ALL
SELECT DISTINCT
  b.Date,
  b.CustName AS Customer,
  b.Item,
  b.NPD,
  b.PCDescription AS Product_Description,
  AVG(b.QtyPcs) AS QtyPcs,
  AVG(b.Qty) QtyInv,
  AVG(b.UnitPrice) AS UnitPrice,
  sum(COALESCE(b.Total_SalesGross,0))/sum(COALESCE(b.QtyPcs,0)) As Price_pcs,
  AVG(b.ExchRate) AS Rate,
  Sum(b.Total_SalesGross) AS Total_Sales,
  sum(COALESCE(b.Total_SalesGross,0))-sum(COALESCE(b.Total_Discount,0)) AS Net_Sales,
  a.TotalCOGS,
  a.COGS,
  a.COGSMat,
  a.COGSLab,
  a.COGSFOvh,
  a.COGSVovh
FROM {{ ref('MP_Budget_Sales_v') }} b
LEFT JOIN (SELECT DISTINCT
              CustName,
              PCDescription,
              NPD,
              Date,
              AVG(COGSCost) * sum(Qty) AS TotalCOGS,
              sum(COGSCost) AS COGS,
              sum(MatlCost) AS COGSMat,
              sum(LaborCost) AS COGSLab,
              sum(FovhdCost) AS COGSFOvh,
              sum(VovhdCost) AS COGSVovh
          FROM {{ ref('COGS_Budget_v') }} 
          GROUP BY  CustName,
              PCDescription,
              NPD,
              Date
          ) a ON a.CustName = b.CustName 
                AND a.PCDescription = b.PCDescription
                AND a.NPD = b.NPD 
                AND a.Date = b.Date                                         
WHERE b.QtyPcs > 0 
group by
  b.Date, 
  b.CustName, 
  b.NPD, 
  b.Item,
  b.PCDescription,a.TotalCOGS,
  a.COGS,
  a.COGSMat,
  a.COGSLab,
  a.COGSFOvh,
  a.COGSVovh