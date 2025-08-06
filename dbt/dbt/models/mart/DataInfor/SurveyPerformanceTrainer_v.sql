{{
  config(
    materialized= 'table'
  )
}}

SELECT
DISTINCT
  FORMAT_DATE('%Y-%m-%d', a.IncentiveDate) AS IncentiveDate,
  a.Job,
  SUBSTR(a.Job, 1, 4) AS JobPrefix,
  TRIM(a.EmpNum) as NIK,
  user.FullName as EmployeeName,
  user.JobTitle,
  user.JoinDate,
  user.LeaveDate,
  user.EmployeeStatus,
  user.ContractStatus,
  user.Age,
  user.ResidenceProvince,
  user.ResidenceVillage,
  user.ResidenceDistrict,
  user.ResidenceRegency,
  user.Location,
  user.Shift,
  user.FirstApprove,
  user.SecondApprove,
  TRIM(a.ConeType) AS ConeType,
  a.JobItem as Item,
  a.JobItemDesc as Description,
  a.Reject,
  a.Netto,
  hours.TotalHours,
  a.TargetQty,
  (a.TargetQty * hours.TotalHours) AS Target,
  ROUND((prod_code.prodcodeUf_MP80_RejectScore2 * a.gross),0) AS TargetReject,
  ROUND((a.Netto / hours.TotalHours),2) AS ActualCPH,
  ROUND((((a.Netto) / (a.TargetQty * hours.TotalHours))*100),2) AS ActualProductivity
FROM {{ source('mp_infor', 'mp80_incentives') }} a
  LEFT JOIN {{ source('mp_infor', 'hris_user') }} user ON TRIM(a.EmpNum) = user.nik
  LEFT JOIN {{ source('mp_infor', 'product_codes_BQ') }} prod_code ON a.ProductCode = prod_code.ProductCode
  LEFT JOIN {{ ref('META_TotalHours_v') }} hours ON a.Job = hours.job AND TRIM(a.EmpNum) = hours.EmpNum
WHERE (a.Gross > 0 AND a.TargetQty > 0 AND a.TotalHours > 0 AND prod_code.prodcodeUf_MP80_RejectScore2 > 0 AND hours.TotalHours>0) 
  AND a.IncentiveDate >= '2025-01-01'
  AND SUBSTR(a.Job, 1, 5) IN ('JSFG-','JSFJ-','JSMJ-')  -- JO Gianyar & Jembrana
  AND a.Job NOT IN (SELECT ue_Job FROM `mp_infor.JobExclude`) -- Exclude Job Special Case
  AND a.Loc <> 'GP3 Filter' -- Exclude GP3 Filter