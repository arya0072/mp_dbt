{{
  config(
    materialized= 'table'
  )
}}

select 
  x.ue_Item,
  x.ue_CustName,
  x.ue_ConvFactor,
  x.ue_UM,
  x.month_year,
  sum(x.ue_QtyBeginning) as qty_begining,
  sum(x.Buy1) AS Buy1,
  sum(x.Buy2) AS Buy2,
  sum(x.Make_Convert) AS Make_Convert,
  sum(x.Sell1) AS Sell1,
  sum(x.Sell2) AS Sell2,
  sum(x.Use1) AS Use1,
  sum(x.Use2) AS Use2,
  sum(x.Other1) AS Other1,
  sum(x.Other2) AS Other2,
  sum(x.Other3) AS Other3,
  sum(x.Adjust1) AS Adjust1,
  sum(x.Adjust2) AS Adjust2,
  sum(x.Adjust3) AS Adjust3,
  sum(x.Adjust4) AS Adjust4,
  sum(x.Adjust5) AS Adjust5
from (
select
  mq_mpkb.ue_Item,
  mq_mpkb.ue_CustName,
  mq_mpkb.ue_ConvFactor,
  mq_mpkb.ue_UM,
  mq_mpkb.month_year,
  mq_mpkb.ue_QtyBeginning,
  0 AS Buy1,
  0 AS Buy2,
  0 AS Make_Convert,
  0 AS Sell1,
  0 AS Sell2,
  0 AS Use1,
  0 AS Use2,
  0 AS Other1,
  0 AS Other2,
  0 AS Other3,
  0 AS Adjust1,
  0 AS Adjust2,
  0 AS Adjust3,
  0 AS Adjust4,
  0 AS Adjust5
from {{ source('mp_infor', 'saldo_awal_qty_mpkb_bq') }} mq_mpkb
UNION ALL
select
  mat.Item,
  c.ue_CustName,
  c.ue_ConvFactor,
  c.ue_UM,
  FORMAT_DATE('%Y-%m', mat.TransDate) AS month_year,
  0  as ue_QtyBeginning,
  SUM(CASE WHEN mat.TransType = 'R' AND b.RefType = 'P' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Buy1,
  SUM(CASE WHEN mat.TransType = 'W' AND b.RefType = 'P' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Buy2,
  SUM(CASE WHEN mat.TransType = 'F' AND b.RefType = 'J' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Make_Convert,
  SUM(CASE WHEN mat.TransType = 'S' AND b.RefType = 'O' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Sell1,
  SUM(CASE WHEN mat.TransType = 'W' AND b.RefType = 'R' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Sell2,
  SUM(CASE WHEN mat.TransType = 'I' AND b.RefType = 'J' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Use1,
  SUM(CASE WHEN mat.TransType = 'W' AND b.RefType = 'J' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Use2,
  SUM(CASE WHEN mat.TransType = 'M' AND b.RefType = 'I' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Other1,
  SUM(CASE WHEN mat.TransType = 'T' AND b.RefType = 'T' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Other2,
  SUM(CASE WHEN mat.TransType = 'L' AND b.RefType = 'T' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Other3,
  SUM(CASE WHEN mat.TransType = 'G' AND b.RefType = 'I' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Adjust1,
  SUM(CASE WHEN mat.TransType = 'H' AND b.RefType = 'I' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Adjust2,
  SUM(CASE WHEN mat.TransType = 'B' AND b.RefType = 'I' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Adjust3,
  SUM(CASE WHEN mat.TransType = 'P' AND b.RefType = 'I' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Adjust4,
  SUM(CASE WHEN mat.TransType = 'A' AND b.RefType = 'I' THEN COALESCE(mat.Qty, 0) ELSE 0 END) AS Adjust5
from {{ ref('material_transaction_fix_v') }} mat
LEFT JOIN (select 
              tt.RefType,
              tt.TransNum
            from {{ source('mp_infor', 'transtype_MPKB') }} tt
            where tt.RefType not in ('C','N')
            group by  tt.RefType,
              tt.TransNum) b on mat.TransNum = b.TransNum
LEFT JOIN (select 
              saldo_pr.ue_Item,
              saldo_pr.month_year,
              saldo_pr.ue_CustName,
              saldo_pr.ue_ConvFactor,
              saldo_pr.ue_UM
          from {{ source('mp_infor', 'saldo_awal_qty_mpkb_bq') }} saldo_pr
          group by saldo_pr.ue_Item,
              saldo_pr.month_year,
              saldo_pr.ue_CustName,
              saldo_pr.ue_ConvFactor,
              saldo_pr.ue_UM) c ON mat.Item = c.ue_Item and FORMAT_DATE('%Y-%m', mat.TransDate) = c.month_year
where site = 'MPKB'
GROUP BY mat.Item, mat.ItmUM,site, month_year,  c.ue_CustName,
  c.ue_ConvFactor,
  c.ue_UM

) x
-- where x.ue_Item='100100001' and x.month_year='2023-08'
 group by x.ue_Item, 
  x.ue_CustName,
  x.ue_ConvFactor,
  x.ue_UM,
  x.month_year
 order by x.month_year asc