{{
  config(
    materialized= 'table'
  )
}}

SELECT
  DISTINCT
  'INFOR' as source,
  TRIM(a.EmpNum) as EmpNum,
  a.EmpName as EmployeeName,
  a.EmpStatus as Status,
  user.Section as Resource,
  a.RlcResource as ResourceTrans,
  user.SectionGroup as ResourceGroup,
  a.Job,
  DATE(a.IncentiveDate) AS IncentiveDate,
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
  DATE(a.LastTransactionDate) AS LastTransactionDate,
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
  AND a.IncentiveDate <=  '2025-06-30' -- CUT OFF dari infor
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
UNION ALL
SELECT
  DISTINCT
  'PORTAL' as source,
  CAST(a.EmpNum AS STRING) AS EmpNum,
  a.EmployeeName,
  a.Status2 as Status,
  user.Section as Resource,
  a.Resource as ResourceTrans,
  user.SectionGroup as ResourceGroup,
  a.Job,
  DATE(TIMESTAMP_SECONDS(a.date) + INTERVAL 8 HOUR) AS IncentiveDate,
  a.ProductCode,
  prod_code.Description as ProdCodeDesc,
  TRIM(a.ConeType) AS ConeType,
  a.Item as Item,
  a.Description,
  a.Operation,
  'C' as JobStatus,
  m_floor.name as Location,
  JobRoute.wc AS WorkCenter,
  CAST(a.Gross AS FLOAT64) AS Gross,
  CAST(a.BA AS FLOAT64) AS BA,
  CAST(a.BLT AS FLOAT64) AS BLT,
  CAST(a.BS AS FLOAT64) AS BS,
  CAST(a.CS AS FLOAT64) AS CS,
  CAST(a.DLL AS FLOAT64) AS DLL,
  CAST(a.EXT AS FLOAT64) AS EXT,
  CAST(a.FL AS FLOAT64) AS FL,
  CAST(a.FM AS FLOAT64) AS FM,
  CAST(a.FT AS FLOAT64) AS FT,
  CAST(a.FTZ AS FLOAT64) AS FTZ,
  CAST(a.KR AS FLOAT64) AS KR,
  CAST(a.OT AS FLOAT64) AS OT,
  CAST(a.OV AS FLOAT64) AS OV,
  CAST(a.NAL AS FLOAT64) AS NAL,
  CAST(a.Reject AS FLOAT64) AS Reject,
  CAST(a.NettoExcl AS FLOAT64) AS NettoExcl,
  CAST(a.Netto AS FLOAT64) AS Netto,
  CAST(a.TargetQty AS FLOAT64) AS TargetQty,
  CAST(a.TotalHours AS FLOAT64) AS TotalHours,
  a.Posted,
  a.Closed,
  DATE(TIMESTAMP_SECONDS(a.date) + INTERVAL 8 HOUR) AS LastTransactionDate,
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
FROM {{ source('mp_infor', 'hrpayroll_p_daily_production_summary') }} a
  LEFT JOIN {{ source('mp_infor', 'hris_user') }}  user ON CAST(a.empnum AS STRING) = user.NIK
  LEFT JOIN {{ source('mp_infor', 'product_codes_BQ') }} prod_code ON a.ProductCode = prod_code.ProductCode
  LEFT JOIN {{ source('mp_infor', 'hrpayroll_m_section') }} section ON a.id_m_section = section.id
	LEFT JOIN {{ source('mp_infor', 'hrpayroll_m_floor') }} m_floor ON section.id_m_floor = m_floor.id
  LEFT JOIN (SELECT 
              DISTINCT 
              Job,
              JobItem,
              Wc
              FROM {{ source('mp_infor', 'jobRoutes') }}
              WHERE OperNum='10') JobRoute ON a.Job = JobRoute.Job AND a.Item = JobRoute.JobItem
WHERE a.Gross > 0 
  AND DATE(TIMESTAMP_SECONDS(a.date) + INTERVAL 8 HOUR) >=  '2025-07-01' -- Start dari Production Portal
