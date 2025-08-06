{{
  config(
    materialized= 'table'
  )
}}

select 
  a.ReqNum,
  a.InvNum,
  a.VendNum,
  a.vendName,
  a.ReqDate,
  a.DueDate,
  a.Requester,
  a.Approver,
  a.Approver2,
  a.TotalCost,
  a.Stat as StatusHeader,
  a.RecordDate,
  detail.ReqLine,
  detail.Item,
  detail.Description,
  detail.ItemDescription,
  detail.NonItemDescription,
  detail.UM,
  CASE
    WHEN detail.Stat = 'A' THEN 'Approved'
    WHEN detail.Stat = 'R' THEN 'Request'
    ELSE 'Reject'
   END AS StatusLine,
  detail.Dept,
  detail.QtyOrdered,
  detail.UnitMatCost,
  detail.Totalcost AS TotalCostDetail,
  detail.NonInvAcct,
  detail.NonInvAcctUnit1
from {{ source('mp_infor', 'ReqPayHeader') }} a
  LEFT JOIN {{ source('mp_infor', 'ReqPayDetail') }} detail ON a.ReqNum = detail.ReqNum
-- where a.ReqNum='RFP-005752'