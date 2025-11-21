{{
  config(
    materialized= 'table'
  )
}}

SELECT DISTINCT
Acct,
ChaDescription as Acct_Desc,
VendNum,
    (CASE 
        WHEN DerCustVendName IS NOT NULL THEN DerCustVendName 
        WHEN LEFT(Ref, 14) = "PAJE - Reclass" THEN "Futurola LLC"
        WHEN LEFT(Ref, 4) = "PAJE" THEN "PAJE"
        WHEN REGEXP_CONTAINS(Ref, r"(?i)((FX)|(Forex)|(Reval)|(Rate)).*") THEN "Reclass"
        ELSE "Reclass"
    END) as Cust_Name,
CheckDate,
CheckNum,
TransDate,
 CONCAT(CAST(EXTRACT(YEAR FROM TransDate) AS STRING), '-', 
           LPAD(CAST(EXTRACT(MONTH FROM TransDate) AS STRING), 2, '0')) AS YearMonth,
CurrCode,
    (CASE 
        WHEN CurrCode<>'IDR' THEN DerForAmountDebit 
        ELSE 0
    END) as For_Debit,
        (CASE 
        WHEN CurrCode<>'IDR' THEN DerForAmountCredit 
        ELSE 0
    END) as For_Credit,
((CASE WHEN CurrCode<>'IDR' THEN COALESCE(DerForAmountDebit,0) ELSE 0 END)-(CASE WHEN CurrCode<>'IDR' THEN COALESCE(DerForAmountCredit,0) ELSE 0 END)) as for_bal,
COALESCE(DerDomAmountDebit,0) as Debit,
COALESCE(DerDomAmountCredit,0) as Credit,
(COALESCE(DerDomAmountDebit,0)-COALESCE(DerDomAmountCredit,0)) as dom_bal,
Voucher,
Ref,
TransNum
FROM {{ ref('ledger_v') }}
WHERE Acct in('1-1320','4-1200','4-1300')
ORDER BY Acct ASC