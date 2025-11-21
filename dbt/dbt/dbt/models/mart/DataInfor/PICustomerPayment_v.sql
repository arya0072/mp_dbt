{{
  config(
    materialized= 'table'
  )
}}

SELECT
  cust_po,
  cust_name,
  cust_num,
  DepositAmount,
  paid_amount,
  PaymentDate,
  ROUND(COALESCE(DepositAmount, 0) - COALESCE(paid_amount, 0)) AS Outstanding
FROM {{ source('mp_infor', 'MP76_PI_Payment') }}
GROUP BY
  cust_po,
  PIDate,
  cust_name,
  cust_num,
  paid_amount,
  PaymentDate,
  DepositAmount