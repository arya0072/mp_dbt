{{
  config(
    materialized= 'table'
  )
}}

SELECT
  DISTINCT
  TRIM(a.EmpNum) as EmpNum,
  a.EmpName as EmployeeName,
  a.EmpStatus as Status,
  user.Section as Resource,
  a.RlcResource as ResourceTrans,
  user.SectionGroup as ResourceGroup,
  a.Job,
  a.IncentiveDate,
  a.ProductCode,
  prod_code.Description as ProdCodeDesc,
  TRIM(a.ConeType) AS ConeType,
  a.JobItem as Item,
  a.JobItemDesc as Description,
  a.JobOper as Operation,
  a.JobStat as JobStatus,
  a.Loc as Location,
  a.wc as WorkCenter,
  a.Gross,
  a.BA,
  a.BLT,
  a.BS,
  a.CS,
  a.DLL,
  a.EXT,
  a.FL,
  a.FM,
  a.FT,
  a.FTZ,
  a.KR,
  a.OT,
  a.OV,
  a.NAL,
  a.Reject,
  a.NettoExcl,
  a.Netto,
  a.TargetQty,
  a.TotalHours,
  a.Posted,
  a.Closed,
  a.LastTransactionDate,
  CASE
    WHEN SUBSTR(a.Job, 1, 4) IN ('JFCJ', 'JFGJ', 'JPPJ', 'JSFJ', 'JSFG', 'JSFS', 'JSFM') THEN 'Permanent'
    WHEN SUBSTR(a.Job, 1, 3) IN ('JHR', 'JOS', 'JOT', 'JPU', 'JCP', 'JFP', 'JFC', 'JFG', 'JSM') THEN 'Permanent'
    WHEN SUBSTR(a.Job, 1, 4) IN ('JHRT', 'JOTT', 'JSTJ', 'JSTT', 'JSFT') THEN 'Magang'
    WHEN SUBSTR(a.Job, 1, 5) IN ('JSFTS', 'JSTMJ') THEN 'Magang'
    WHEN SUBSTR(a.Job, 1, 4) IN ('JFGO', 'JFOS', 'JFGO') THEN 'Outsource'
    WHEN SUBSTR(a.Job, 1, 5) IN ('JFGOJ') THEN 'Outsource'
    WHEN SUBSTR(a.Job, 1, 4) IN ('JSFO', 'JPUO', 'JFPO') THEN 'Overtime'
    WHEN SUBSTR(a.Job, 1, 4) IN ('JSFJO', 'JFGOT') THEN 'Overtime'
    ELSE 'Not Identify'
END AS CategoryJO
FROM {{ source('mp_infor', 'mp80_incentives') }} a
  LEFT JOIN {{ source('mp_infor', 'hris_user') }} user ON TRIM(a.EmpNum) = user.nik
  LEFT JOIN {{ source('mp_infor', 'product_codes_BQ') }} prod_code ON a.ProductCode = prod_code.ProductCode
WHERE a.Gross > 0 
  -- AND a.Job IN ('JSFO-12020','JSFG-76265','JSFO-12060','JSFO-12021')
  -- AND TRIM (a.EmpNum) IN ('190885','180485','200897','181096')
GROUP BY  a.Job,
  a.JobOper,
  a.JobStat,
  a.IncentiveDate,
  TRIM(a.EmpNum),
  a.EmpName,
  a.EmpStatus,
  a.Loc,
  a.JobItem,
  a.JobItemDesc,
  a.ProductCode,
  TRIM(a.ConeType),
  user.Section,
  user.SectionGroup,
  a.Gross,
  a.BA,
  a.BLT,
  a.BS,
  a.CS,
  a.DLL,
  a.EXT,
  a.FL,
  a.FM,
  a.FT,
  a.FTZ,
  a.KR,
  a.OT,
  a.OV,
  a.NAL,
  a.Reject,
  a.NettoExcl,
  a.Netto,
  a.TargetQty,
  a.TotalHours,
  a.Posted,
  a.Closed,
  a.LastTransactionDate,
  a.wc,
  a.RlcResource,
  prod_code.Description

--1081325
