{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  a.TransDate,
  a.RecordDate,
  a.Job,
  a.JobItem,
  a.JobDescription,
  b.ProductcodeDescription,
  b.DecimalValue,
  a.ItemUM,
  COALESCE(b.DecimalValue,1)*a.QtyComplete AS QtyPcs,
  a.QtyComplete,
  a.QtyMoved,
  a.QtyScrapped,
  a.OperNum,
  a.AHrs,
  a.RESID,
  a.RESDescription,
  a.jobtUf_MP55_EmployeeCount,
  a.JobRate,
  a.JobrWc,
  CASE
    when SUBSTR(a.Job, 1,4) IN ('JSFO') AND SUBSTR(a.JobrWc, 1,2) IN ('R-') AND SUBSTR(a.JobItem, 1,2) IN ('51') THEN 'Cones - Overtime Rolling'
    when SUBSTR(a.Job, 1,4) IN ('JSFO') AND SUBSTR(a.JobrWc, 1,4) IN ('R-OT') AND SUBSTR(a.JobItem, 1,4) IN ('5301') THEN 'HRF - Overtime Rolling Filter'
    when SUBSTR(a.Job, 1,4) IN ('JSFO') AND SUBSTR(a.JobrWc, 1,2) IN ('F-') AND SUBSTR(a.JobItem, 1,4) IN ('5301') THEN 'HRF - Overtime Rolling Filter'
    when SUBSTR(a.Job, 1,4) IN ('JSFO') AND SUBSTR(a.JobrWc, 1,4) IN ('R-OT') AND SUBSTR(a.JobItem, 1,4) IN ('5302') THEN 'Tip - Overtime Rolling Filter Tip'
    when SUBSTR(a.Job, 1,4) IN ('JSFO') AND SUBSTR(a.JobrWc, 1,2) IN ('F-') AND SUBSTR(a.JobItem, 1,4) IN ('5302') THEN 'Tip - Overtime Rolling Filter Tip'
    when SUBSTR(a.Job, 1,4) IN ('JFG-','Jfg-') AND SUBSTR(a.JobrWc, 1,6) IN ('PCKBLK') AND SUBSTR(a.JobItem, 1,4) IN ('9005') THEN 'Packing Bulk'
    when SUBSTR(a.Job, 1,4) IN ('JFG-','JFGO') AND SUBSTR(a.JobrWc, 1,6) IN ('PCOSBL') AND SUBSTR(a.JobItem, 1,4) IN ('9005') THEN 'Packing Bulk - Outsource'
    when SUBSTR(a.Job, 1,4) IN ('JFG-') AND SUBSTR(a.JobrWc, 1,6) IN ('PCKCSP') AND SUBSTR(a.JobItem, 1,4) IN ('9006') THEN 'Packing Consumer Pack'
    when SUBSTR(a.Job, 1,4) IN ('JFG-') AND SUBSTR(a.JobrWc, 1,6) IN ('PCKCSP') AND SUBSTR(a.JobItem, 1,4) IN ('9005') THEN 'Packing Bulk'
    when SUBSTR(a.Job, 1,4) IN ('Jfg-') AND SUBSTR(a.JobrWc, 1,6) IN ('PCKCSP') AND SUBSTR(a.JobItem, 1,4) IN ('9006') THEN 'Packing Consumer Pack'
    when SUBSTR(a.Job, 1,4) IN ('JFG-') AND SUBSTR(a.JobrWc, 1,6) IN ('PCKFTT') AND SUBSTR(a.JobItem, 1,4) IN ('9003') THEN 'Packing Filter Tip'
    when SUBSTR(a.Job, 1,4) IN ('JSFT') AND SUBSTR(a.JobrWc, 1,2) IN ('T-','R-') AND SUBSTR(a.JobItem, 1,2) IN ('51') THEN 'Cones - Magang Rolling'
    when SUBSTR(a.Job, 1,4) IN ('JSFG') AND SUBSTR(a.JobItem, 1,2) IN ('51','54') THEN 'Cones - Rolling'
    when SUBSTR(a.Job, 1,4) IN ('JSFG') AND SUBSTR(a.JobItem, 1,2) IN ('66') THEN 'Sample Cones - Rolling'
    when SUBSTR(a.Job, 1,4) IN ('JSFG') AND SUBSTR(a.JobItem, 1,4) IN ('5301') THEN 'HRF - Rolling Filter'
    when SUBSTR(a.Job, 1,4) IN ('JSFG','jsfg') AND SUBSTR(a.JobItem, 1,4) IN ('5302') THEN 'Tip - Rolling Filter Tip'
    when SUBSTR(a.Job, 1,3) IN ('JRM') AND SUBSTR(a.JobItem, 1,4) IN ('2024') THEN 'Rubber'
    when SUBSTR(a.Job, 1,3) IN ('JCP') AND SUBSTR(a.JobItem, 1,2) IN ('50') THEN 'Cutting'
    when SUBSTR(a.Job, 1,3) IN ('JCP') AND SUBSTR(a.JobItem, 1,2) IN ('65') THEN 'Sample Cutting'
    when SUBSTR(a.Job, 1,3) IN ('JPU') AND SUBSTR(a.JobItem, 1,2) IN ('50') THEN 'Unwind'
    when SUBSTR(a.Job, 1,3) IN ('JFP') AND SUBSTR(a.JobItem, 1,2) IN ('50') THEN 'Filter'
    when SUBSTR(a.Job, 1,3) IN ('JPP') AND SUBSTR(a.JobItem, 1,2) IN ('50') THEN 'Plano'
    when SUBSTR(a.Job, 1,3) IN ('JSM','jsm') THEN 'Sample'
    when SUBSTR(a.Job, 1,3) IN ('JFC','JPR') THEN 'Printing Vendor'
    when SUBSTR(a.Job, 1,3) IN ('JPM') THEN 'Stick'
    ELSE 'NotIdentify'
    END as JobType,
  AVG(COALESCE(jobtUf_MP55_EmployeeCount,1)) AS HC
FROM {{ source('mp_infor', 'job_transaction_mp') }} a
  LEFT JOIN {{ ref('item_attributeall_v') }}  b ON a.JobItem = b.Item
-- WHERE a.Job = 'JSFG-39502'
GROUP BY
a.TransDate,
  a.RecordDate,
  a.Job,
  a.JobItem,
  a.JobDescription,
  b.ProductcodeDescription,
  b.DecimalValue,
  a.ItemUM,
  a.QtyComplete,
  a.QtyMoved,
  a.QtyScrapped,
  a.OperNum,
  a.AHrs,
  a.RESID,
  a.RESDescription,
  a.jobtUf_MP55_EmployeeCount,
  a.JobRate,
  a.JobrWc