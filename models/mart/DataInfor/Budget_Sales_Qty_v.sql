{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  DISTINCT
  EXTRACT(YEAR FROM a.Tgl) AS Year,
  EXTRACT(MONTH FROM a.Tgl) AS Periode,
  a.Customer,
  a.NPD,
  a.Product_Code,
  a.Item_Code,
  a.Rate,
  a.Conv_Factor,
  a.Qty_Inv,
  a.Qty_Pcs,
  a.Price,
  a.Price_pcs,
  a.Value,
  a.COGS_Material,
  a.COGS_Labor
FROM {{ source('mp_infor', 'Budget Sales Qty') }} a