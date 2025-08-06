{{
  config(
    materialized= 'table'
  )
}}

SELECT DISTINCT
  EXTRACT(MONTH FROM a.Tgl) AS Month,
  SUM(a.Qty_Pcs) AS Budget,
  SUM(b.Qty_Shipped) AS Actual
FROM {{ source('mp_infor', 'Budget Sales Qty') }} as a
LEFT JOIN {{ ref('ActualQtySales_v') }} as b
ON 
  EXTRACT(MONTH FROM a.Tgl) = b.Month
WHERE 
    a.Product_Code IN ('FGD - Pre-rolled Paper Consumer Pack', 'FGD - Pre-rolled Paper Bulk') 
  OR 
    b.Categories IN ('Consumer Pack','Bulk')
GROUP BY 
  EXTRACT(MONTH FROM a.Tgl),
  b.Month
