{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  b.RefNum,
  b.Item,
  b.ItemDescription,
  b.TransDate,
  b.Acct,
  b.ChaDescription,
  c.ProductcodeDescription,
  b.DerDomAmountDebit,
  b.DerDomAmountCredit
 FROM {{ ref('DetailExpenses_v') }}  b
 LEFT JOIN {{ ref('item_productcode_v') }} c ON b.Item = c.Item
 Where c.ProductcodeDescription is not null 
