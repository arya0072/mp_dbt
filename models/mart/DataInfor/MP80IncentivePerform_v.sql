{{
  config(
    materialized= 'table'
  )
}}

SELECT
 a.EmpNum,
 a.EmployeeName,
 UPPER(SUBSTR(TRIM(a.job), 1, 4)) AS JobPrefix,
 a.Location,
 a.Resource,
 FORMAT_DATE('%Y-%m', a.IncentiveDate) AS IncentiveDate,
 a.ProductCode,
 ROUND(SUM(a.AdjusmentProd), 2) AS AdjusmentProd,
 SUM(a.Reject) AS Reject, 
 SUM(a.TargetQty) AS TargetQty,
 ROUND(AVG(a.ActPercenReject), 2) AS ActPercenReject, 
 SUM(a.TargetReject) AS TargetReject,
 SUM(a.Netto) AS Netto,
 ROUND(SUM(a.TotalHours), 2) AS TotalHours,
 ROUND(SUM(a.TargetByMatrix_WHActual), 2) AS TargetByMatrix_WHActual,
 ROUND(AVG(ProductivityRateByMatrix), 2) AS ProductivityRateByMatrix,
 ROUND(AVG(ActualProductivity), 2) AS ActualProductivity,
 CASE 
  WHEN AVG(a.ProductivityRateByMatrix) >= 100 THEN 'Perform'
  WHEN AVG(a.ProductivityRateByMatrix) < 100 THEN 'Under Perform'
  ELSE NULL
 END AS StatInMatrixExReject,
 CASE 
  WHEN AVG(a.ProductivityRateByMatrix) < 100 OR AVG(a.ActPercenReject) > 1 THEN 'Under Perform' 
  ELSE 'Perform'
 END AS StatInMatrixInReject,
 CASE 
  WHEN AVG(a.ActualProductivity) >= 100 THEN 'Perform'
  WHEN AVG(a.ActualProductivity) < 100 THEN 'Under Perform'
  ELSE NULL
 END AS StatExMatrixExReject,
 CASE 
  WHEN AVG(a.ActualProductivity) < 100 OR AVG(a.ActPercenReject) > 1 THEN 'Under Perform'
  ELSE 'Perform'
 END AS StatExMatrixInReject
FROM {{ ref('MP80IncentiveDetail_v') }} a
group by  
 a.EmpNum,
 a.EmployeeName,
 a.Location,
 a.ProductCode,
 a.Resource,
 IncentiveDate,
 JobPrefix
