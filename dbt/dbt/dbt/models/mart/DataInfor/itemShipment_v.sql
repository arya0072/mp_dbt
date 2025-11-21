{{
  config(
    materialized= 'table'
  )
}}

select 
  a.RmaNum,
  a.RmaLine,
  a.RmahdrCustNum,
  a.DerCus0Name,
  a.Item,
  a.Description,
  a.CustItem,
  a.QtyToReturnConv,
  a.UM,
  a.CoNum,
  a.CoLine,
  a.ReasonText,
  a.OrigInvNum,
  b.ShipDate,
  a.RmahdrRmaDate as RmaDate
from {{ source('mp_infor', 'RmaItems') }} a
  LEFT JOIN {{ source('mp_infor', 'm_shipment') }} b ON a.OrigInvNum = b.shiUf_MP08_InvNum
