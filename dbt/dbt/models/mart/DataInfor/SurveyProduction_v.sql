{{
  config(
    materialized= 'table'
  )
}}

SELECT  
  DISTINCT
  TRIM(a.EmpNum) as NIK,
  user.FullName as EmployeeName,
  user.MaritalStatusActual,
  user.Age,
  CASE
    WHEN user.age BETWEEN 18 AND 23 THEN '18 - 23 Years'
    WHEN user.age BETWEEN 24 AND 29 THEN '24 - 29 Years'
    WHEN user.age BETWEEN 30 AND 35 THEN '30 - 35 Years'
    WHEN user.age > 35 THEN '> 35 Year'
    ELSE NULL
  END AS GroupAge,
  user.ContractStatus,
  a.Resource,
  a.RlcResource,
  section.Shift AS ShiftName,
  CASE 
    WHEN section.Shift LIKE '%Shift 1%' THEN 'Shift 1'
    WHEN section.Shift LIKE '%Shift 2%' THEN 'Shift 2'
    ELSE NULL
  END AS Shift,
  a.EmpStatus as Status,
  CASE
    WHEN user.Province LIKE '%BALI%' THEN 'BALI'
    WHEN user.Province LIKE '%JAWA%' THEN 'JAWA'
    WHEN user.Province LIKE '%NUSA TENGGARA TIMUR%' THEN 'NUSA TENGGARA TIMUR'
    WHEN user.Province LIKE '%NUSA TENGGARA BARAT%' THEN 'NUSA TENGGARA BARAT'
    ELSE 'OTHER'    
  END AS Province,
  -- user.Province,
  user.Regency,
  SUBSTR(a.Job, 1, 5) AS JobPrefix,
  a.Job,
  DATE(a.IncentiveDate) AS IncentiveDate,
  SUBSTR(a.ProductCode, 1, 3) AS ProductCode,
  prod_code.Description as ProdCodeDesc,
  TRIM(a.ConeType) AS ConeType,
  a.JobItem as Item,
  a.JobItemDesc as Description,
  a.JobStat as JobStatus,
  a.Loc as Location,
  a.wc as WorkCenter,
  SUM(a.Netto) as Netto,
  SUM(a.TotalHours) AS TotalHours,
  ROUND((SUM(a.Netto)/SUM(a.TotalHours)),2) AS NettHours,
  user.MaritalStatusTax
FROM {{ source('mp_infor', 'mp80_incentives') }} a
  JOIN {{ source('mp_infor', 'hris_user') }} user ON TRIM(a.EmpNum) = user.nik
  LEFT JOIN {{ source('mp_infor', 'product_codes_BQ') }} prod_code ON a.ProductCode = prod_code.ProductCode
  LEFT JOIN {{ source('mp_infor', 'HRIS_Section') }} section ON a.RlcResource = section.Section
WHERE (a.TargetQty > 0 AND a.TotalHours > 0) 
  AND a.IncentiveDate BETWEEN '2024-01-01' AND '2024-12-31'
  AND SUBSTR(a.Job, 1, 5) IN ('JSFG-','JSFJ-')  -- JO Gianyar
  AND SUBSTR(a.ProductCode, 1, 3) IN ('SCN') -- Type Cones
  AND (section.Shift LIKE '%Shift 1%' OR section.Shift LIKE '%Shift 2%')
  AND user.ContractStatus NOT IN ('Probation')
  -- AND TRIM(a.EmpNum) = '202561'
  -- AND a.Job= 'JSFG-61073'
  AND user.ContractStatus IS NOT NULL
GROUP BY
  NIK,
  EmployeeName,
  MaritalStatusActual,
  Age,
  ContractStatus,
  Resource,
  RlcResource,
  Shift,
  ShiftName,
  Status,
  JobPrefix,
  a.Job,
  IncentiveDate,
  ProductCode,
  ProdCodeDesc,
  ConeType,
  Item,
  Description,
  JobStatus,
  Location,
  WorkCenter,
  Province,
  Regency,
  user.MaritalStatusTax
