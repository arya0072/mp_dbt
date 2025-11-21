{{
  config(
    materialized= 'table'
  )
}}

SELECT
  DISTINCT
  mp83_job_progress.EmpCount,
  mp83_job_progress.ItDesc,
  mp83_job_progress.Item,
  mp83_job_progress.Job,
  mp83_job_progress.JOProgress,
  mp83_job_progress.Location,
  mp83_job_progress.LocDescription,
  mp83_job_progress.ProdCode,
  mp83_job_progress.QtyCompleted,
  mp83_job_progress.QtyExpected,
  mp83_job_progress.QtyScrapped,
  mp83_job_progress.RejectRate,
  mp83_job_progress.Resource,
  mp83_job_progress.ResourceDescription,
  mp83_job_progress.StartDate,
  mp83_job_progress.Status,
  mp83_job_progress.TotalHour,
  uom_conv.FromDesc,
  uom_conv.FromUM,
  uom_conv.ToDesc,
  uom_conv.ToUM,
  uom_conv.ConvFactor,
  mp83_job_progress.ConeType,
  MAX(mp83_job_progress.QtyExpectedConv) as QtyExpectedConv,
  MAX(mp83_job_progress.QtyCompletedConv) as QtyCompletedConv,
  mp83_job_progress.QtyScrappedConv,
  mp83_job_progress.WC,
  mp83_job_progress.LstTrxDate,
  mp83_job_progress.TargetQty
FROM {{ source('mp_infor', 'mp83_joborder_progress') }} AS mp83_job_progress 
  LEFT JOIN {{ source('mp_infor', 'uom_conversion') }} AS uom_conv ON mp83_job_progress.Item = uom_conv.Item
where uom_conv.ToUM NOT IN ('UNT') AND mp83_job_progress.item LIKE '900%' 
GROUP BY mp83_job_progress.EmpCount,
  mp83_job_progress.ItDesc,
  mp83_job_progress.Item,
  mp83_job_progress.Job,
  mp83_job_progress.JOProgress,
  mp83_job_progress.Location,
  mp83_job_progress.LocDescription,
  mp83_job_progress.ProdCode,
  mp83_job_progress.QtyCompleted,
  mp83_job_progress.QtyExpected,
  mp83_job_progress.QtyScrapped,
  mp83_job_progress.RejectRate,
  mp83_job_progress.Resource,
  mp83_job_progress.ResourceDescription,
  mp83_job_progress.StartDate,
  mp83_job_progress.Status,
  mp83_job_progress.TotalHour,
  uom_conv.FromDesc,
  uom_conv.FromUM,
  uom_conv.ToDesc,
  uom_conv.ToUM,
  uom_conv.ConvFactor,
  mp83_job_progress.ConeType,
  mp83_job_progress.QtyScrappedConv,
  mp83_job_progress.WC,
  mp83_job_progress.LstTrxDate,
   mp83_job_progress.TargetQty