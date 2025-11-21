{{
  config(
    materialized= 'table'
  )
}}

SELECT
  a.TransDate,
  c.ProductcodeDescription,
  a.JobItem,
  a.JobDescription,
  a.Job,
  a.JobType,
  a.QtyPcs,
  sum(b.MatlTranViewTotalPost) AS MatlTranViewTotalPost
FROM {{ ref('job_trans_fix') }} a
JOIN {{ source('mp_infor', 'material_tran_1') }}  b on a.Job = b.RefNum
JOIN {{ ref('item_productcode_v') }} c on a.JobItem = c.Item
Where b.TransType = 'F' AND b.MatlTranViewTotalCost <> 0
-- WHERE b.TransType = 'F' AND b.RefNum = 'JSFO-04103' AND b.MatlTranViewTotalCost <> 0
Group by a.TransDate,
  c.ProductcodeDescription,
  a.JobItem,
  a.JobDescription,
  a.Job,
  a.JobType,
  a.QtyPcs