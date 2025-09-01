{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  a.TransNum,
  a.TransDate,
  a.Item,
  a.ItemDescription,
  a.ProductCodeDescription,
  a.Qty,
  a.MatlTranViewTotalPost,
  a.Whse,
  b.ReasonCode
FROM {{ source('mp_infor', 'material_tran_jmbr') }} a
LEFT JOIN {{ source('mp_infor', 'material_transaction_reasoncode_jmbr') }} b ON a.TransNum = b.TransNum