{{
  config(
    materialized= 'table'
  )
}}

SELECT
  a.Periode,
  a.PeriodeDate,
  a.EmpNum AS NIK,
  a.EmployeeName,
  a.Resource,
  SUBSTR(a.Job, 1, 4) AS Job,
  SUBSTR(a.ProductCode, 1, 3) AS ProductCode,
  SUM(a.Gross) AS Gross,
  SUM(a.Reject) AS Reject,
  SUM(a.TargetReject) AS TargetReject,
  ROUND((SUM(a.Reject) / NULLIF(SUM(a.TargetReject), 0)) * 100, 0) AS ActPercenReject,
  SUM(a.Netto) AS Netto,
  ROUND((SUM(a.Netto)/SUM(a.WHMP80)), 2) AS ActualCPH,
  ROUND((SUM(a.TargetNett) / SUM(a.WHJT)), 2) AS TargetNettPerHours,
  SUM(a.TargetNett) AS TargetNett,
  SUM(a.WHMP80) AS WHMP80,
  SUM(a.WHJT) as WHJT,
  SUM(a.AdjusmentProd) AS AdjusmentProd,
  COALESCE(SUM(a.MaxPointPerformance),0) AS MaxPointPerformance,
  COALESCE(SUM(a.TotalJob),0) AS TotalJob,
  COALESCE(SUM(a.AbsencePoint),0) AS AbsencePoint,
  COALESCE(SUM(a.MaxPointPerformance),0) + COALESCE(SUM(a.TotalJob),0) + COALESCE(SUM(a.AbsencePoint),0) AS TargetPoint,
  SUM(a.PointActualPerformance) AS Point
FROM {{ ref('META_PointDetail_v') }} a
WHERE a.TargetReject > 0
  AND a.CategoryJO = 'Permanent'
  -- AND a.EmpNum = '170019'

GROUP BY
  a.Periode,
  a.PeriodeDate,
  a.EmpNum,
  a.EmployeeName,
  a.Location,
  a.Resource,
  a.ProductCode,
  SUBSTR(a.Job, 1, 4)