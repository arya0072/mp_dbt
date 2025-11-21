{{
  config(
    materialized= 'table'
  )
}}

SELECT
  'sales' as tipe,
  InvDate,
  InvNum,
  CustName,
  Item,
  ItemDesc,
  cust_item,
  ProductcodeDescription,
  QtyInvoiced,
  (coalesce(ExtendedPrice,0) * coalesce(ExchRate,0)) AS sales_gross,
  NULL AS TotalMatl,
  NULL AS TotalLbr
FROM {{ source('mp_infor', 'salestransaction') }}  
where QtyInvoiced > 0 
union all
SELECT
  'cogs' as tipe,
  InvDate,
  InvNum,
  CustName,
  Item,
  ItemDesc,
  cust_item,
  ProductcodeDescription,
  QtyInvoiced,
  Null AS sales_gross,
  CASE 
    WHEN QtyInvoiced > 0 THEN CgsMatlTotal * QtyInvoiced
    ELSE CgsMatlTotal * QtyInvoiced * -1
  end as TotalMatl,
  CASE 
    WHEN QtyInvoiced > 0 THEN CgsLbrTotal  * QtyInvoiced
    ELSE CgsLbrTotal  * QtyInvoiced * -1
  end as TotalLbr
FROM {{ source('mp_infor', 'salestransaction') }} 
