{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  a.InvDate,
  a.InvNum,
  a.ShipDate,
  a.CustNum,
  a.CustName,
  a.Item,
  a.ItemDesc,
  a.Um,
  b.ProductcodeDescription,
  a.ConvFactor,
  b.DecimalValue AS ConvUn,
  CASE
   WHEN COALESCE(a.ConvFactor,0)>0 THEN COALESCE(a.ConvFactor,0)
   WHEN COALESCE(a.ConvFactor,0)=0 THEN COALESCE(b.DecimalValue,0)
  END AS Conv,
  a.QtyInvoiced,
  (CASE
   WHEN COALESCE(a.ConvFactor,0)>0 THEN COALESCE(a.ConvFactor,0)
   WHEN COALESCE(a.ConvFactor,0)=0 THEN COALESCE(b.DecimalValue,0)
  END)* a.QtyInvoiced AS Qty_Pcs, 
  a.CurrCode,
  a.CoiPrice,
  a.DiscAmt,
  a.ExtendedPrice,
  a.ExtendedNetPrice,
  a.ExchRate,
  a.DomesticExtendedPrice,
  a.CgsMatlTotal,
  a.CgsLbrTotal,
  a.CgsFovhdTotal,
  a.CgsVovhdTotal,
  a.CgsOutTotal,
  a.CgsTotal,
  a.DomesticExtendedCogs
 FROM {{ source('mp_infor', 'MP34_SalesTransaction') }} a
 left join {{ ref('item_conversion_unit_v') }} b ON a.Item = b.Item
--  Where a.Item = '900400037'
 