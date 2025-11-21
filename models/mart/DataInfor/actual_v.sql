{{
  config(
    materialized= 'table'
  )
}}

SELECT
  1 AS month,
  Cust_ID,
  SUM(qty_shipped) as Actual,
  Categories,
  Year
FROM {{ ref('ActualQtySales_v') }} 
WHERE month = 1
GROUP BY month,Categories, Cust_ID, Year
UNION ALL
SELECT 
  2 AS month,
  Cust_ID,
  SUM(qty_shipped),
  Categories,
  Year
FROM {{ ref('ActualQtySales_v') }} 
WHERE month in (1,2)
GROUP BY month,Categories, Cust_ID,Year
UNION ALL
SELECT
  3 AS month,
  Cust_ID,
  SUM(qty_shipped),
  Categories,
  Year
FROM {{ ref('ActualQtySales_v') }} 
WHERE month in (1,2,3)
GROUP BY month,Categories, Cust_ID, Year
UNION ALL
SELECT
  4 AS month,
  Cust_ID,
  SUM(qty_shipped),
  Categories,
  Year
FROM {{ ref('ActualQtySales_v') }} 
WHERE month in (1,2,3,4)
GROUP BY month,Categories, Cust_ID, Year
UNION ALL
SELECT
  5 AS month,
  Cust_ID,
  SUM(qty_shipped),
  Categories,
  Year
FROM {{ ref('ActualQtySales_v') }} 
WHERE month in (1,2,3,4,5)
GROUP BY month,Categories, Cust_ID, Year
 UNION ALL
SELECT
  6 AS month,
  Cust_ID,
  SUM(qty_shipped),
  Categories,
  Year
FROM {{ ref('ActualQtySales_v') }} 
WHERE month in (1,2,3,4,5,6)
GROUP BY month,Categories, Cust_ID, Year
UNION ALL
SELECT
  7 AS month,
  Cust_ID,
  SUM(qty_shipped),
  Categories,
  Year
FROM {{ ref('ActualQtySales_v') }} 
WHERE month in (1,2,3,4,5,6,7)
GROUP BY month,Categories, Cust_ID, Year
UNION ALL
SELECT
  8 AS month,
  Cust_ID,
  SUM(qty_shipped),
  Categories,
  Year
FROM {{ ref('ActualQtySales_v') }} 
WHERE month in (1,2,3,4,5,6,7,8)
GROUP BY month,Categories, Cust_ID, Year
UNION ALL
SELECT
  9 AS month,
  Cust_ID,
  SUM(qty_shipped),
  Categories,
  Year
FROM {{ ref('ActualQtySales_v') }} 
WHERE month in (1,2,3,4,5,6,7,8,9)
GROUP BY month,Categories, Cust_ID, Year
UNION ALL
SELECT
  10 AS month,
  Cust_ID,
  SUM(qty_shipped),
  Categories,
  Year
FROM {{ ref('ActualQtySales_v') }} 
WHERE month in (1,2,3,4,5,6,7,8,9,10)
GROUP BY month,Categories, Cust_ID,Year
UNION ALL
SELECT
  11 AS month,
  Cust_ID,
  SUM(qty_shipped),
  Categories,
  Year
FROM {{ ref('ActualQtySales_v') }} 
WHERE month in (1,2,3,4,5,6,7,8,9,10,11)
GROUP BY month,Categories, Cust_ID, Year
UNION ALL
SELECT
  12 AS month,
  Cust_ID,
  SUM(qty_shipped),
  Categories, Year
FROM {{ ref('ActualQtySales_v') }} 
WHERE month in (1,2,3,4,5,6,7,8,9,10,11,12)
GROUP BY month, Categories, Cust_ID, Year