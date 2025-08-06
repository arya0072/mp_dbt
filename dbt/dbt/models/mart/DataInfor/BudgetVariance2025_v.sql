{{
  config(
    materialized= 'table'
  )
}}

SELECT
  DISTINCT
  a.Acct,
  a.Description,
  b.AcctUnit4,
  b.TransDate,
  b.AcctUnit1,
  b.ChaDescription,
  CASE 
    WHEN SUBSTR(b.Acct, 1, 1) = "5" THEN "Biaya Produksi"
    WHEN SUBSTR(b.Acct, 1, 1) = "6" THEN "Biaya non Produksi"
    WHEN SUBSTR(b.Acct, 1, 1) = "1" THEN "Asset"
    ELSE "Lainnya"
  END AS Category,
  FORMAT_DATE('%Y', b.TransDate) AS Year,
  Extract (MONTH FROM b.TransDate) AS Month,
  c.BudgetCatDesc,
  c.Total_Budget,
  c.Qty,
  b.transnum,
  c.note,
  (b.DerDomAmountDebit - b.DerDomAmountCredit) as TotalExpense
FROM {{ ref('DetailExpenses_v') }} b
  JOIN {{ source('mp_infor', 'COA') }} a ON a.Acct = b.Acct
  JOIN {{ ref('MP_budget_departement_v') }} c ON a.Acct = c.Acct
                                                AND b.AcctUnit1 = c.AcctUnit1
                                                AND b.AcctUnit4 = c.AcctUnit4
                                                AND FORMAT_DATE('%Y', b.TransDate) = c.Year
                                                AND EXTRACT(MONTH FROM b.TransDate) = CAST(c.Period AS INT64)
                                                AND c.Total_Budget = (b.DerDomAmountDebit - b.DerDomAmountCredit)
WHERE FORMAT_DATE('%Y', b.TransDate) = '2025'
AND b.AcctUnit4='NGR'
AND b.Acct= '6-2317'
