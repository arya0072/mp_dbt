{{
  config(
    materialized= 'table'
  )
}}

SELECT 
    a.TransDate,
    a.JobItem,
    a.JobrWc,
    a.JobType,
    c.ProductcodeDescription,
    a.Whse,
    a.jobtUf_MP55_EmployeeCount,
    a.AHrs,
    a.QtyPcs,
    a.AHrs * a.jobtUf_MP55_EmployeeCount AS total_hour,
    (a.AHrs * a.jobtUf_MP55_EmployeeCount) * b.Output_Hour AS qty_standard,
    b.Output_Hour
FROM {{ ref('job_trans_fix') }} a
LEFT JOIN {{ ref('item_productcode_v') }} c ON a.JobItem = c.Item
LEFT JOIN {{ source('mp_infor', 'Output_standart_new') }} b ON c.ProductcodeDescription = b.Description