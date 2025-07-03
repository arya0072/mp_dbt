{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  a.Month_A, 
  a.Year_A, 
  a.COGS_LABOR, 
  b.LABOR_EXCES,
  b.LABOR_EXCES / a.COGS_LABOR AS COGS_PER_EXCES
FROM
  (SELECT 
    EXTRACT(MONTH FROM InvDate) AS Month_A,
    EXTRACT(YEAR FROM InvDate) AS Year_A,
    SUM(CgsLbrTotal * QtyInvoiced) AS COGS_LABOR       
   FROM {{ source('mp_infor', 'salestransaction') }}
   GROUP BY Month_A, Year_A) a
JOIN (SELECT 
        EXTRACT(MONTH FROM Date) AS Month_B,
        EXTRACT(YEAR FROM Date) AS Year_B,
        SUM(MP_Value) AS LABOR_EXCES
   FROM {{ source('mp_infor', 'Labor_Exces') }}
   GROUP BY Month_B, Year_B) b ON a.Month_A = b.Month_B AND a.Year_A = b.Year_B