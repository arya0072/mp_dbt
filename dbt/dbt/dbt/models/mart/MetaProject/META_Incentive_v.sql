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
  a.ResourceTrans AS Resource,
  SUBSTR(a.Job, 1, 4) AS Job,
  -- SUBSTR(a.ProductCode, 1, 3) AS ProductCode,
  SUM(a.Gross) AS Gross,
  SUM(a.Reject) AS Reject,
  SUM(a.TargetReject) AS TargetReject,
  ROUND((SUM(a.Reject) / NULLIF(SUM(a.TargetReject), 0)) * 100, 0) AS ActPercenReject,
  CASE 
    WHEN (SUM(a.Reject) / NULLIF(SUM(a.TargetReject), 0)) * 100 < 100 
    THEN 'Reject Qualified' 
    ELSE 'Reject Not Qualified' 
  END AS CategoryReject,
  SUM(a.Netto) AS Netto,
  ROUND((SUM(a.ActualCPH)),1) AS NettHour,
  ROUND(SUM(a.Netto) / SUM(a.TotalHours), 1) AS ActualCPH,
  SUM(a.Target) AS Target,
  ROUND(SUM(a.TargetByMatrix), 1) AS TargetByMatrix,
  ROUND(SUM(a.TotalHours), 1) AS TotalHours,
  CASE 
    WHEN /*/*SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND*/ ROUND((SUM(a.Netto) / NULLIF(SUM(a.Target), 0)) * 100, 2) BETWEEN 102.50 AND 103.49 THEN 'GROUP 1'
    WHEN /*SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND*/ ROUND((SUM(a.Netto) / NULLIF(SUM(a.Target), 0)) * 100, 2) BETWEEN 103.50 AND 104.49 THEN 'GROUP 2'
    WHEN /*SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND*/ ROUND((SUM(a.Netto) / NULLIF(SUM(a.Target), 0)) * 100, 2) BETWEEN 104.50 AND 107.49 THEN 'GROUP 3'
    WHEN /*SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND*/ ROUND((SUM(a.Netto) / NULLIF(SUM(a.Target), 0)) * 100, 2) BETWEEN 107.50 AND 110.49 THEN 'GROUP 4'
    WHEN /*SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND*/ ROUND((SUM(a.Netto) / NULLIF(SUM(a.Target), 0)) * 100, 2) BETWEEN 110.50 AND 117.49 THEN 'GROUP 5'
    WHEN /*SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND*/ ROUND((SUM(a.Netto) / NULLIF(SUM(a.Target), 0)) * 100, 2) BETWEEN 117.50 AND 122.49 THEN 'GROUP 6'
    WHEN /*SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND*/ ROUND((SUM(a.Netto) / NULLIF(SUM(a.Target), 0)) * 100, 2) >= 122.50 THEN 'GROUP 7'
    WHEN /*SUBSTR(a.ProductCode, 1, 3) IN ('SFT') AND*/ (SUM(a.Netto) / ROUND(SUM(a.TotalHours), 1))  > 598 THEN 'GROUP HRF'
    ELSE '-'
  END AS GroupProductivity,
  CASE
    WHEN /*SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') 
         AND*/ SUM(a.Target) <> SUM(a.TargetByMatrix) 
         AND ROUND((SUM(a.Netto) / NULLIF(SUM(a.TargetByMatrix), 0)) * 100, 2) > 110 
    THEN 'GROUP 1'
    ELSE '-'
  END AS GroupProductivityAdjust,
  COALESCE(MAX(absence.CountAbsence), 0) AS CountAbsence,
  COALESCE(MAX(leave.Countleave), 0) AS Countleave,
  COALESCE(MAX(wd.TotalDays), 0) AS TotalDays,
  COALESCE(MAX(sp.CountCase), 0) AS CountCase,
  COALESCE(MAX(resign.CountResign), 0) AS CountResign,
  ROUND((COALESCE(MAX(absence.CountAbsence), 0) + COALESCE(MAX(leave.Countleave), 0)) 
          / NULLIF(COALESCE(MAX(wd.TotalDays), 0), 0) * 100, 2) AS PercentAbsence,
  CASE 
    WHEN ROUND((COALESCE(MAX(absence.CountAbsence), 0) + COALESCE(MAX(leave.Countleave), 0)) 
          / NULLIF(COALESCE(MAX(wd.TotalDays), 0), 0) * 100, 2) >= 90
      AND COALESCE(MAX(sp.CountCase), 0) = 0
      AND COALESCE(MAX(resign.CountResign), 0) = 0 
      AND ROUND((SUM(a.Reject) / NULLIF(SUM(a.TargetReject), 0)) * 100, 0) <= 100 THEN 'YES'
    ELSE 'NO'
  END AS is_incentive,
  a.Location,
  ROUND((SUM(a.Netto) / NULLIF(SUM(a.Target),0)*100),2) AS ProdExcludeMatrix,
  ROUND((SUM(a.Netto)/ NULLIF(SUM(a.TargetByMatrix),0)*100),2) AS ProdIncludeMatrix
FROM {{ ref('META_IncentiveProd_v') }} a
LEFT JOIN {{ ref('META_AbsenceMP80_v') }} absence ON a.EmpNum = absence.NIK 
                                                     AND a.Periode = absence.Periode
LEFT JOIN {{ ref('META_LeaveIncentive_v') }} leave ON a.EmpNum = leave.NIK 
                                                 AND a.Periode = leave.Periode
LEFT JOIN {{ ref('META_WorkingDays_v') }} wd ON a.Periode = wd.Periode
LEFT JOIN {{ ref('META_CaseIncentive_v') }} sp ON a.EmpNum = sp.NIK 
                                             AND a.Periode = sp.Periode
LEFT JOIN {{ ref('META_TerminationProcess_v') }} resign ON a.EmpNum = resign.NIK 
                                                     AND a.Periode = resign.Periode                                            
WHERE  a.TargetReject > 0
AND a.CategoryJO = 'Permanent'
-- AND a.EmpNum = '250311'
GROUP BY 
  a.Periode,
  a.PeriodeDate,
  a.EmpNum,
  a.EmployeeName,
  a.Location,
  a.ResourceTrans,
  -- a.ProductCode,
  SUBSTR(a.Job, 1, 4)