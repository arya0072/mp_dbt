{{
  config(
    materialized= 'table'
  )
}}

SELECT
  a.RcvdDate,
  a.PoOrderDate,
  a.VenadrName,
  a.PoCurrCode,
  a.PoNum,
  a.Item,
  a.Description,
  b.Description AS product_code,
  a.UM,
  CASE
      WHEN a.stat = 'C' THEN 'Complete'
      WHEN a.stat = 'P' THEN 'Planned'
      WHEN a.stat = 'O' THEN 'Ordered'
      WHEN a.stat = 'F' THEN 'Filled'
      ELSE NULL
  END AS po_line_status,
  avg(a.ItemCostConv) AS item_cost,
  sum(a.QtyOrderedConv) AS qty_order,
  sum(a.QtyOrderedConv*a.ItemCostConv) AS PO_value,
  sum(a.DerQtyReceivedConv) AS qty_receipt,
  sum(a.DerQtyReceivedConv*a.ItemCostConv) AS receipt_value,
  sum(a.DerQtyRejectedConv) AS qty_rejected,
  sum(a.DerQtyRejectedConv*a.ItemCostConv) AS reject_value
FROM {{ source('mp_infor', 'po_detail') }} a
LEFT JOIN {{ source('mp_infor', 'product_codes_BQ') }} b ON a.itmproductcode = b.ProductCode
-- where a.PoNum = 'PO-0004024'
group by a.PoOrderDate, a.RcvdDate, a.VenadrName,a.PoCurrCode,a.PoNum, a.Item, a.Description, b.Description,a.UM, po_line_status
