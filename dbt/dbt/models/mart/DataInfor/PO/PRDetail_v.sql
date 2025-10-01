{{
  config(
    materialized= 'table'
  )
}}

 SELECT
  a.ReqNum as req_num,
  CASE
    WHEN a.Stat = 'C' THEN 'CONVERT'
    WHEN a.Stat = 'H' THEN 'HISTORY'
    WHEN a.Stat = 'R' THEN 'REQUESTED'
  END AS status_header,
  a.ReqDate as req_date,
  a.whse as whse,
  a.ReqCost AS req_cost, 
	a.Requester AS requester,
  b.ReqLine AS req_line,
	b.item AS item,
	b.Description as description,
	CASE 
		WHEN b.stat = 'C' THEN 'CONVERT'
		WHEN b.stat = 'H' THEN 'HISTORY'
		WHEN b.stat = 'R' THEN 'REQUESTED'
	END AS status_detail,
  b.UM AS u_m,
  b.PODueDate AS due_date,
  b.ponum AS po_num,
  b.qtyorderedconv as qty_ordered_conv,
  b.PlanCostConv AS plan_cost_conv,
  b.UnitMatCostConv AS unit_mat_cost_conv
FROM {{ source('mp_infor', 'pr_header') }} a
  LEFT JOIN {{ source('mp_infor', 'pr_detail') }} b ON a.ReqNum =b.ReqNum