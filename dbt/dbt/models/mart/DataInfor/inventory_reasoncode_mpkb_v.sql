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
FROM {{ source('mp_infor', 'material_tran_1_mpkb') }} a
LEFT JOIN {{ source('mp_infor', 'material_transaction_reasoncode_mpkb') }} b ON a.TransNum = b.TransNum
