{{
  config(
    materialized= 'table'
  )
}}

SELECT
  a.*,
  move_qty.Cust,
  move_qty.CustName
FROM {{ source('mp_infor', 'mp83_joborder_progress') }} a
  LEFT JOIN (SELECT
              DISTINCT
              Year,
              Month,
              ue_Customer AS Cust,
              ue_CustName AS CustName,
              ue_Item AS Item
            FROM {{ source('mp_infor', 'movement_qty_mp') }}) move_qty ON a.Item = move_qty.Item
                                                                       AND FORMAT_DATE('%Y', DATE(a.StartDate)) = move_qty.Year
                                                                       AND UPPER(FORMAT_DATE('%b', DATE(a.StartDate))) = move_qty.Month
-- where a.item='500200231' and a.StartDate='2025-06-25'
