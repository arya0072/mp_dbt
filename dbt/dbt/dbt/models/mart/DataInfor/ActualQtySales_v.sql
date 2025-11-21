{{
  config(
    materialized= 'table'
  )
}}

SELECT
  EXTRACT(MONTH FROM InvDate) AS Month,
  EXTRACT(YEAR FROM InvDate) AS Year,
  CustNum AS Cust_ID,
  InvNum,
  Item,
  (CASE WHEN ConvFactor > 0  THEN ConvFactor ELSE ConvUn END)* QtyInvoiced AS Qty_Shipped,
    CASE
      WHEN ProductcodeDescription='SM - FGD - Consumer Pack' then 'Consumer Pack'
      WHEN ProductcodeDescription='SM - FGD - Bulk' then 'Bulk'
      WHEN ProductcodeDescription='FGD - Pre-rolled Paper Consumer Pack' then 'Consumer Pack'
      WHEN ProductcodeDescription='FGD - Pre-rolled Paper Bulk' then 'Bulk'
      WHEN ProductcodeDescription= 'FGD - Pre-rolled Paper Bulk Machine' then "Bulk"
      WHEN Item LIKE '%9003%' and overview LIKE '%21pack%' then 'Filter Tip 21'
      WHEN Item LIKE '%9003%' and overview LIKE '%100pack%' then 'Filter Tip 100'
      WHEN ProductcodeDescription='FGD - Accesories Knock Box' then 'Acc'
      WHEN overview LIKE '%KNOCKBOX 300%' then "KB300"
      WHEN overview LIKE '%KNOCKBOX 500%' then "KB500"
      WHEN overview LIKE '%KNOCKBOX 100%' then "KB100"
      WHEN overview LIKE '%KNOCKBOX 50%' then "KB50"
      WHEN overview LIKE '%KNOCKOUT 50%' then "KO50"
      WHEN overview LIKE '%THUMPER%' then "Thumper"
      
    END
  AS Categories
 FROM {{ source('mp_infor', 'salestransaction') }} 
 WHERE InvDate > '2022-12-31' 