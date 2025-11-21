{{
  config(
    materialized= 'table'
  )
}}

SELECT
  a.CoNum AS co_num,
  a.SiteRef AS site_ref,
  a.CoLine AS co_line,
  a.Item AS item,
  a.CustItem AS cust_item,
  a.Whse AS whse,
  CASE 
    WHEN a.Stat='C' THEN 'Complete'
    WHEN a.Stat='F' THEN 'Fill'
    WHEN a.Stat='O' THEN 'Ordered'
    WHEN a.Stat='P' THEN 'Planning'
    END as status,
  a.DueDate AS due_date,
  a.DueDate AS due_date_plus_8_hours,
  a.Description AS description,
  NULL AS DLDeleteIndicator,
  'ACTIVE' AS is_deleted,
  a.PromiseDate AS promise_date
FROM {{ source('mp_infor', 'COItems') }} a
WHERE  LEFT(a.item, 4) IN ('9005','9006')