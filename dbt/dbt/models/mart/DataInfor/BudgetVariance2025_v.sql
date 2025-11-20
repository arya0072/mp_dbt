{{
  config(
    materialized= 'table'
  )
}}

SELECT
  Acct,
  ChaDescription as Description,
  DerCustVendName as Customer,
  TransDate as Date,
  CheckNum,
  Ref,
  CurrCode as Currency,
  DerForAmountDebit as Debit,
  DerForAmountCredit as Credit,
FROM {{ source('mp_infor', 'ledger') }}  
WHERE ChaDescription in ('BNI USD', 'BNI EUR','MANDIRI USD','MANDIRI EUR') AND TransDate > '2021-12-31'
ORDER BY ChaDescription desc