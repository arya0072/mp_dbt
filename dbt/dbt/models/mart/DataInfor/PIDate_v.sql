{{
  config(
    materialized= 'table'
  )
}}

SELECT
  cust_po,
  cust_name,
  cust_num,
  PIDate,
  DueDate
FROM {{ source('mp_infor', 'MP76_PI_Payment') }} 