{{
  config(
    materialized= 'table'
  )
}}

SELECT
 a.EmpNum,
 a.EmployeeName,
 a.Location,
 a.Resource,
 a.Item,
 a.Job,
 a.Description,
 a.IncentiveDate,
 a.ProductCode,
 SUM(a.AdjusmentProd) AS AdjusmentProd,
 SUM(a.Reject) AS Reject, 
 SUM(a.TargetQty) AS TargetQty,
 AVG(a.ActPercenReject) AS ActPercenReject, 
 SUM(a.TargetReject) AS TargetReject,
 SUM(a.Netto) AS Netto,
 SUM(a.TotalHours) AS TotalHours,
 SUM(a.TargetByMatrix_WHActual) AS TargetByMatrix_WHActual,
 AVG(ProductivityRateByMatrix) AS ProductivityRateByMatrix, 
 CASE 
  WHEN AVG(a.ProductivityRateByMatrix) > 100 THEN 'Perform'
  WHEN AVG(a.ProductivityRateByMatrix) < 100 THEN 'Under Perform'
  ELSE NULL
END AS stat
FROM {{ ref('MP80IncentiveDetail_v') }} a
GROUP BY 
a.EmpNum,
 a.EmployeeName,
 a.Location,
 a.Resource,
 a.Item,
 a.Job,
 a.Description,
 a.IncentiveDate,
 a.ProductCode
