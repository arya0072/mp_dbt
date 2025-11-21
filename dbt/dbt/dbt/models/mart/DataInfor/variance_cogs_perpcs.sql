{{
  config(
    materialized= 'table'
  )
}}

WITH Budget AS (
  SELECT
    b.Product_Description AS Product_Code,
    b.NPD,
    b.Customer,
    EXTRACT(YEAR FROM b.Tgl) AS Year,
    EXTRACT(MONTH FROM b.Tgl) AS Month,
    SUM(b.QtyPcs) AS Qty_Budget,
    SUM(b.TotalCogs) AS COGS_Budget
  FROM {{ ref('sales_budget_v_fix') }} b
  -- WHERE EXTRACT(YEAR FROM b.Tgl) = 2025
  GROUP BY Product_Code, NPD, Customer, Year, Month
),

Actual AS (
  SELECT
    c.ProductcodeDescription AS Product_Code,
    c.cust_item AS NPD,
    c.CustName AS Customer,
    EXTRACT(YEAR FROM c.InvDate) AS Year,
    EXTRACT(MONTH FROM c.InvDate) AS Month,
    SUM(c.QtyInvoiced * COALESCE(NULLIF(c.ConvFactor, 0), c.ConvUn, 1)) AS Qty_Actual,
    SUM(c.DomesticExtendedCogs) AS COGS_Actual
  FROM {{ source('mp_infor', 'salestransaction') }} c
  -- WHERE EXTRACT(YEAR FROM c.InvDate) = 2025
  GROUP BY Product_Code, NPD, Customer, Year, Month
),

Combined AS (
  SELECT
    COALESCE(b.Product_Code, a.Product_Code) AS Product_Code,
    COALESCE(b.NPD, a.NPD) AS NPD,
    COALESCE(b.Customer, a.Customer) AS Customer,
    COALESCE(b.Year, a.Year) AS Year,
    COALESCE(b.Month, a.Month) AS Month,
    COALESCE(b.Qty_Budget, 0) AS Qty_Budget,
    COALESCE(a.Qty_Actual, 0) AS Qty_Actual,
    COALESCE(b.COGS_Budget, 0) AS COGS_Budget,
    COALESCE(a.COGS_Actual, 0) AS COGS_Actual
  FROM Budget b
  FULL OUTER JOIN Actual a
    ON b.Product_Code = a.Product_Code
    AND b.NPD = a.NPD
    AND b.Customer = a.Customer
    AND b.Year = a.Year
    AND b.Month = a.Month
)

SELECT
  Product_Code,
  NPD,
  Customer,
  Year,
  Month,
  SUM(Qty_Budget) AS Qty_Budget,
  SUM(Qty_Actual) AS Qty_Actual,
  SUM(Qty_Budget) - SUM(Qty_Actual) AS Selisih_Qty,
  SUM(COGS_Budget) AS Total_COGS_Budget,
  SUM(COGS_Actual) AS Total_COGS_Actual,
  SAFE_DIVIDE(SUM(COGS_Budget), COALESCE(SUM(Qty_Budget), 0)) AS COGS_Pcs_Budget,
  SAFE_DIVIDE(SUM(COGS_Actual), COALESCE(SUM(Qty_Actual), 0)) AS COGS_Pcs_Actual,
  SAFE_DIVIDE(SUM(COGS_Budget), COALESCE(SUM(Qty_Budget), 0)) -
  SAFE_DIVIDE(SUM(COGS_Actual), COALESCE(SUM(Qty_Actual), 0)) AS COGS_Variance_Pcs
FROM Combined
GROUP BY Product_Code, NPD, Customer, Year, Month
ORDER BY Product_Code, Year, Month
