{{
  config(
    materialized= 'table'
  )
}}

SELECT
  'ALL' as condition,
  a.Job,
  a.JobrWc,
  a.Whse,
  a.OperNum,
  a.qty_complete_new,
  a.jobtUf_MP55_EmployeeCount,
  a.AHrs,
  a.JobType,
  a.TransDate,
  a.JobItem,
  a.JobDescription,
  a.ItemUM,
  a.RESID,
  a.RESDescription,
  a.JobRate,
  a.TransType,
  a.DowntimeCode,
  a.Shift,
  a.Convertion,
  a.ProductcodeDescription,
  a.QtyPcs
FROM {{ ref('job_trans_v') }} a
  WHERE a.JobType <> 'NotIdentify'
UNION ALL
SELECT
  'CUTTING' as condition,
  b.Job,
  b.JobrWc,
  b.Whse,
  b.OperNum,
  b.qty_complete_new,
  b.jobtUf_MP55_EmployeeCount,
  b.AHrs,
  b.JobType,
  b.TransDate,
  b.JobItem,
  b.JobDescription,
  b.ItemUM,
  CAST(RESID AS STRING) as RESID,
  CAST(RESDescription AS STRING) as RESDescription,
  b.JobRate,
  CAST(TransType AS STRING) as TransType,
  b.DowntimeCode,
  b.Shift,
  b.Convertion,
  b.ProductcodeDescription,
  b.QtyPcs
FROM {{ ref('job_trans_cutting_v') }}  b
  WHERE b.JobType <> 'NotIdentify'