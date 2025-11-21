{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  cust_po,
  cust_name,
  cust_num,
  SUM(DepositAmount) AS PI_Amount,
  SUM(paid_amount) AS Paid,
  ROUND(CASE WHEN SUM(DepositAmount)-SUM(paid_amount) <=0 THEN 0
  ELSE SUM(DepositAmount)-SUM(paid_amount) END) as Due_Amt
FROM {{ source('mp_infor', 'MP76_PI_Payment') }} 
GROUP BY cust_po, cust_name, cust_num,DueDate