{{
  config(
    materialized= 'table'
  )
}}

SELECT 
    jp.Job,
    jp.EmpCount,
    jp.Item,
    jp.ItDesc,
    jp.TotalHour,
    jp.WC,
    jp.TargetQty,
    MAX(jp.QtyExpected) AS QtyExpected,
    jp.QtyCompleted,
    jt.TransDate,
    jt.JobType,
    jt.RESID,
    jt.RESDescription,
    jt.QtyPcs,
    MAX(jp.QtyExpected) / MAX(jp.TargetQty) AS TargetJam
FROM  {{ ref('mp83_joborder_progress_v') }} jp
LEFT JOIN {{ ref('job_trans_v') }} jt
ON jp.JOB = jt.JOB
WHERE jp.TargetQty <> 0
GROUP BY jp.Job,
    jp.EmpCount,
    jp.Item,
    jp.ItDesc,
    jp.TotalHour,
    jp.WC,
    jp.TargetQty,
    jp.QtyCompleted,
    jt.TransDate,
    jt.JobType,
    jt.RESID,
    jt.RESDescription,
    jt.QtyPcs