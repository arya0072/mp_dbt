{{
  config(
    materialized= 'table'
  )
}}

SELECT
  'Expenses' AS Source,
  a.Acct,
  a.AcctUnit1,
  a.AcctUnit4,
  FORMAT_DATE('%m', a.TransDate) AS Month,
  FORMAT_DATE('%Y', a.TransDate) AS Year,
  a.ChaDescription,
  CASE 
    WHEN SUBSTR(a.Acct, 1, 1) = "5" THEN "Biaya Produksi"
    WHEN SUBSTR(a.Acct, 1, 1) = "6" THEN "Biaya non Produksi"
    WHEN SUBSTR(a.Acct, 1, 1) = "1" THEN "Asset"
    ELSE "Lainnya"
  END AS Category,
  COALESCE(a.DerDomAmountDebit,0)-COALESCE(a.DerDomAmountCredit,0) AS Total
FROM {{ ref('DetailExpenses_v') }}  a
WHERE a.AcctUnit4='NGR'
UNION ALL
SELECT
  'Budget' AS Source,
  b.Acct,
  b.AcctUnit1,
  b.AcctUnit4,
  b.Period AS Month,
  b.Year,
  b.ChartDescription,
  NULL as category,
  b.Total_Budget AS Total
FROM {{ ref('MP_budget_departement_v') }}  b
WHERE b.AcctUnit4 = 'NGR'