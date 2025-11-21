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
  mp83_job_progress.QtyExpectedConv,
  mp83_job_progress.QtyCompletedConv,
  mp83_job_progress.QtyScrappedConv,
  mp83_job_progress.WC,
  mp83_job_progress.LstTrxDate,
  CASE 
    WHEN mp83_job_progress.Resource='KK2R302' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-01-05') AND TIMESTAMP('2023-01-12') THEN 'Magang Jembrana'
    WHEN mp83_job_progress.Resource='KK2R301' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-01-09') AND TIMESTAMP('2023-01-12') THEN 'Magang Jembrana'
    WHEN mp83_job_progress.Resource='BB2R018' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-01-16') AND TIMESTAMP('2023-04-13') THEN 'Magang Jembrana'
    WHEN mp83_job_progress.Resource='BB1R007' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-04-25') AND TIMESTAMP('2023-09-16') THEN 'Magang Jembrana'
    WHEN mp83_job_progress.Resource IN ('BB1R018','BB1R019','BB1R070','BB1R071') AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-10-30') AND CURRENT_TIMESTAMP() THEN 'Magang join 16 Oktober 2023'
    WHEN mp83_job_progress.Resource IN ('BB1R022','BB1R023','BB1R060','BB1R061') AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2024-01-03') AND CURRENT_TIMESTAMP() THEN 'Magang join 18 Desember 2023'
    WHEN mp83_job_progress.Resource IN ('BB1R057','BB1R058','BB2R019','BB2R022') AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-10-02') AND CURRENT_TIMESTAMP() THEN 'Magang join 18 September 2023'
    WHEN mp83_job_progress.Resource='BB2R020' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-10-02') AND TIMESTAMP('2023-10-14') THEN 'Magang join 18 September 2023'
    WHEN mp83_job_progress.Resource='BB2R021' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-10-02') AND TIMESTAMP('2023-12-23') THEN 'Magang join 18 September 2023'
    WHEN mp83_job_progress.Resource IN ('GP3R072','GP3R073','GP3R063','GP3R074') AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-12-04') AND CURRENT_TIMESTAMP() THEN 'Magang join 20 November 2023'
    WHEN mp83_job_progress.Resource='GP3R072' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-09-04') AND TIMESTAMP('2023-12-02') THEN 'Magang join 21 Agustus 2023'
    WHEN mp83_job_progress.Resource='GP3R073' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-09-04') AND TIMESTAMP('2023-09-09') THEN 'Magang join 21 Agustus 2023'
    WHEN mp83_job_progress.Resource IN ('BB1R015','BB1R016','BB1R017','GP3R009') AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-09-04') AND CURRENT_TIMESTAMP() THEN 'Magang join 21 Agustus 2023'
    WHEN mp83_job_progress.Resource='BB1R004' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-07-24') AND CURRENT_TIMESTAMP() THEN 'Resource Ex-packing'
    WHEN mp83_job_progress.Resource='BB1R005' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-10-23') AND CURRENT_TIMESTAMP() THEN 'Resource Ex-packing'
    WHEN mp83_job_progress.Resource='BB1R063' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-06-12') AND TIMESTAMP('2023-11-18') THEN 'Resource Ex-packing'
    WHEN mp83_job_progress.Resource='BB1R069' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-08-28') AND CURRENT_TIMESTAMP() THEN 'Resource Ex-packing'
    WHEN mp83_job_progress.Resource='BB2R016' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-10-18') AND TIMESTAMP('2023-12-16') THEN 'Resource Ex-packing'
    WHEN mp83_job_progress.Resource='GP3R061' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-12-04') AND CURRENT_TIMESTAMP() THEN 'Resource Ex-packing'
    WHEN mp83_job_progress.Resource='BB1R062' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-11-20') AND CURRENT_TIMESTAMP() THEN 'Resource Re-hire'
    WHEN mp83_job_progress.Resource='BB2R017' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-09-04') AND CURRENT_TIMESTAMP() THEN 'Resource Re-hire'
    WHEN mp83_job_progress.Resource='BB2R018' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-09-04') AND TIMESTAMP('2023-12-09') THEN 'Resource Re-hire'
    WHEN mp83_job_progress.Resource='BB2R067' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-07-24') AND TIMESTAMP('2023-10-14') THEN 'Resource Re-hire'
    WHEN mp83_job_progress.Resource='BB2R068' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-07-24') AND TIMESTAMP('2023-10-14') THEN 'Resource Re-hire'
    WHEN mp83_job_progress.Resource='BB2R069' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-07-24') AND CURRENT_TIMESTAMP() THEN 'Resource Re-hire'
    WHEN mp83_job_progress.Resource='BB2R070' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-07-24') AND CURRENT_TIMESTAMP() THEN 'Resource Re-hire'
    WHEN mp83_job_progress.Resource='GP3R203' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-09-04') AND CURRENT_TIMESTAMP() THEN 'Resource Re-hire'
    WHEN mp83_job_progress.Resource='GP3R204' AND TIMESTAMP(StartDate) BETWEEN TIMESTAMP('2023-09-04') AND TIMESTAMP('2023-12-23') THEN 'Resource Re-hire'
END AS type_resource,
mp83_job_progress.TargetQty
FROM {{ source('mp_infor', 'mp83_joborder_progress') }}  AS mp83_job_progress 
  LEFT JOIN (select
  a.Item,
  a.FromDesc,
  a.FromUM,
  a.ToDesc,
  a.ToUM,
  a.ConvFactor
from {{ source('mp_infor', 'uom_conversion') }} a
  JOIN (select
          a1.Item,
          max(a1.ConvFactor) as ConvFactor
        from {{ source('mp_infor', 'uom_conversion') }} a1
        group by a1.Item) max_conv ON a.Item = max_conv.Item 
                                   AND a.ConvFactor = max_conv.ConvFactor) AS uom_conv ON mp83_job_progress.Item = uom_conv.Item AND uom_conv.ToUM NOT IN ('UNT')
-- where mp83_job_progress.Job='JFOS-00110'