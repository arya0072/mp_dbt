{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  EXTRACT(MONTH FROM InvDate) AS Month_A,
  EXTRACT(YEAR FROM InvDate) AS Year_A,
  SUM(DomesticExtendedPrice) AS Net_Sales,
  CASE
  WHEN COALESCE(QtyInvoiced,0)>0 THEN SUM(COALESCE(CgsMatlTotal,0)*COALESCE(QtyInvoiced,0))
  ELSE SUM(COALESCE(CgsMatlTotal,0)*COALESCE(QtyInvoiced,0)*-1)
  END AS COGS_Material,
  CASE
  WHEN COALESCE(QtyInvoiced,0)>0 THEN SUM(COALESCE(CgsLbrTotal,0)*COALESCE(QtyInvoiced,0))
  ELSE SUM(COALESCE(CgsLbrTotal,0)*COALESCE(QtyInvoiced,0)*-1)
  END AS COGS_LABOR,
  CASE
  WHEN COALESCE(QtyInvoiced,0)>0 THEN SUM(COALESCE(CgsFovhdTotal,0)*COALESCE(QtyInvoiced,0))
  ELSE SUM(COALESCE(CgsFovhdTotal,0)*COALESCE(QtyInvoiced,0)*-1)
  END AS COGS_FOvh,
  CASE
  WHEN COALESCE(QtyInvoiced,0)>0 THEN SUM(COALESCE(CgsVovhdTotal,0)*COALESCE(QtyInvoiced,0))
  ELSE SUM(COALESCE(CgsVovhdTotal,0)*COALESCE(QtyInvoiced,0)*-1)
  END AS COGS_VOvh,
  CASE
  WHEN COALESCE(QtyInvoiced,0)>0 THEN SUM(COALESCE(CgsOutTotal,0)*COALESCE(QtyInvoiced,0))
  ELSE SUM(COALESCE(CgsOutTotal,0)*COALESCE(QtyInvoiced,0)*-1)
  END AS COGS_Out,
  InvDate,
  InvNum,
  ProductcodeDescription,
  CustName,
  cust_item,
  overview,
  ConvFactor,
  ConvUn,
  SUM(QtyInvoiced) AS Qty_inv,
  Item
FROM {{ source('mp_infor', 'salestransaction') }} 
GROUP BY Month_A, Year_A, InvDate,InvNum, ProductcodeDescription, CustName, cust_item, overview, ConvFactor, ConvUn, Item, QtyInvoiced, CgsMatlTotal,CgsLbrTotal
