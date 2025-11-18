{{
  config(
    materialized= 'table'
  )
}}

SELECT
  DISTINCT
  TRIM(a.EmpNum) AS EmpNum,
  user.FullName AS EmployeeName,
  a.Status AS Status,
  user.Section AS Resource,
  a.ResourceTrans,
  user.SectionGroup AS ResourceGroup,
  user.JoinDate,
  a.Job,
  DATE(a.IncentiveDate) AS IncentiveDate,
  DATE(
    CASE 
      WHEN EXTRACT(DAY FROM a.IncentiveDate) >= 21 THEN 
        DATE_ADD(DATE_TRUNC(DATE(a.IncentiveDate), MONTH), INTERVAL 1 MONTH)
      ELSE 
        DATE_TRUNC(DATE(a.IncentiveDate), MONTH)
    END
  ) AS PeriodeDate, 
  FORMAT_DATE(
    '%Y-%m', 
      CASE 
        WHEN EXTRACT(DAY FROM a.IncentiveDate) >= 21 THEN 
          DATE_ADD(DATE_TRUNC(DATE(a.IncentiveDate), MONTH), INTERVAL 1 MONTH)
        ELSE 
          DATE_TRUNC(DATE(a.IncentiveDate), MONTH)
      END
  ) AS Periode,
  SUBSTR(a.ProductCode, 1, 3) AS ProductCode,
  prod_code.Description AS ProdCodeDesc,
  TRIM(a.ConeType) AS ConeType,
  a.Item,
  a.Description,
  a.Operation,
  a.JobStatus,
  a.Location,
  a.WorkCenter,
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
  ROUND((prod_code.prodcodeUf_MP80_RejectScore2 * a.gross),0) AS TargetReject,
  ROUND((a.Reject / (prod_code.prodcodeUf_MP80_RejectScore2 * a.gross)),2) AS ActRejectRate,
  ROUND((a.Reject / (prod_code.prodcodeUf_MP80_RejectScore2 * a.gross)),2) AS ActPercenReject,
  CASE
    WHEN ROUND((a.Reject / a.Gross),4) < prod_code.prodcodeUf_MP80_RejectScore2 THEN 'Reject Qualified'
    ELSE 'Reject Not Qualified'
  END AS CategoryReject,
  a.NettoExcl,
  a.Netto,
  a.TargetQty,
  CASE 
    WHEN SUBSTR(a.Job, 1, 4) IN ('JSFT') THEN ROUND(((job_route.Efficiency/100) * (a.TargetQty * a.TotalHours)),2)
    ELSE (a.TargetQty * a.TotalHours)
  END AS Target,
  ROUND((a.Netto / a.TotalHours),2) AS ActualCPH,
  ROUND((((a.Netto) / (a.TargetQty * a.TotalHours))*100),2) AS ActualProductivity,
   CASE 
    WHEN SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND ROUND((((a.Netto) / (a.TargetQty * a.TotalHours))*100),2) BETWEEN 102.50 AND 103.49 THEN 'GROUP 1'
    WHEN SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND ROUND((((a.Netto) / (a.TargetQty * a.TotalHours))*100),2) BETWEEN 103.50 AND 104.49 THEN 'GROUP 2'
    WHEN SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND ROUND((((a.Netto) / (a.TargetQty * a.TotalHours))*100),2) BETWEEN 104.50 AND 107.49 THEN 'GROUP 3'
    WHEN SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND ROUND((((a.Netto) / (a.TargetQty * a.TotalHours))*100),2) BETWEEN 107.50 AND 110.49 THEN 'GROUP 4'
    WHEN SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND ROUND((((a.Netto) / (a.TargetQty * a.TotalHours))*100),2) BETWEEN 110.50 AND 117.49 THEN 'GROUP 5'
    WHEN SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND ROUND((((a.Netto) / (a.TargetQty * a.TotalHours))*100),2) BETWEEN 117.50 AND 122.49 THEN 'GROUP 6'
    WHEN SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND ROUND((((a.Netto) / (a.TargetQty * a.TotalHours))*100),2) >= 122.50  THEN 'GROUP 7'
    WHEN SUBSTR(a.ProductCode, 1, 3) IN ('SFT') AND ROUND((a.Netto / a.TotalHours),2) > 598  THEN 'GROUP HRF'
    ELSE '-'
  END AS GroupProductivity,
  CASE
    WHEN SUBSTR(a.Job, 1, 4) IN ('JSFT') THEN  (a.TargetQty * JT.AHrs * (job_route.Efficiency/100))
    WHEN ROUND((job_route.Efficiency / 90),2) > 1 THEN (a.TargetQty * JT.AHrs)
    ELSE ROUND(((job_route.Efficiency / 90) *  (a.TargetQty * JT.AHrs)),2)
  END AS TargetByMatrix_WHPPIC,
  CASE
    WHEN SUBSTR(a.Job, 1, 4) IN ('JSFT') THEN  (a.TargetQty * a.TotalHours * (job_route.Efficiency/100))
    WHEN ROUND((job_route.Efficiency / 90),2) > 1 THEN (a.TargetQty * a.TotalHours)
    ELSE ROUND(((job_route.Efficiency / 90) *  (a.TargetQty * a.TotalHours)),2)
  END AS TargetByMatrix_WHActual,
  CASE
    WHEN ROUND((job_route.Efficiency / 90), 2) > 1 THEN 
        ROUND( (SUM(a.Netto) / SUM(a.TargetQty * a.TotalHours)) * 100 , 2)
    ELSE 
        ROUND( 
            (SUM(a.Netto) / SUM((job_route.Efficiency / 90) * a.TargetQty * a.TotalHours)) * 100
        , 2)
  END AS ProductivityRateByMatrix,
  CASE
    WHEN ROUND((job_route.Efficiency / 90),2) >= 1 THEN '-'
    WHEN  SUBSTR(a.ProductCode, 1, 3) NOT IN ('SFT') AND ROUND((((a.Netto) / ((job_route.Efficiency / 90) *  a.TargetQty * a.TotalHours)) * 100),2) > 110 THEN 'GROUP 1'
  ELSE '-'
  END AS GroupProductivityAdjust,
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
  END AS CategoryJO,
  a.TotalHours,
  CASE
    WHEN SUBSTR(a.Job, 1, 4) IN ('JSFT') THEN ROUND((job_route.Efficiency / 100), 2)
    ELSE ROUND((job_route.Efficiency / 90),2)
  END AS AdjusmentProd,
  a.Posted,
  a.Closed,
  a.LastTransactionDate,
  JT.AHrs AS WH_PPIC,
  JR.EMP_PPIC AS EMP_PPIC,
  absence.absence_date,
  CASE 
    WHEN (
      (SUM(a.Netto) / SUM(a.TotalHours)) /
      (
        SUM(
          CASE
            WHEN SUBSTR(a.Job, 1, 4) IN ('JSFT') THEN (a.TargetQty * a.TotalHours * (job_route.Efficiency / 100))
            WHEN ROUND((job_route.Efficiency / 90), 2) > 1 THEN (a.TargetQty * a.TotalHours)
            ELSE ROUND(((job_route.Efficiency / 90) * (a.TargetQty * a.TotalHours)), 2)
          END
        ) / SUM(a.TotalHours)
      )
    ) < 1
    THEN 'Underperform'
    ELSE 'Perform'
  END AS StatusInMatrixExReject,
  CASE 
    WHEN (
      (SUM(a.Netto) / SUM(a.TotalHours)) /
      (
        SUM(
          CASE
            WHEN SUBSTR(a.Job, 1, 4) IN ('JSFT') THEN (a.TargetQty * a.TotalHours * (job_route.Efficiency / 100))
            WHEN ROUND((job_route.Efficiency / 90), 2) > 1 THEN (a.TargetQty * a.TotalHours)
            ELSE ROUND(((job_route.Efficiency / 90) * (a.TargetQty * a.TotalHours)), 2)
          END
        ) / SUM(a.TotalHours)
      )
    ) < 1
    OR AVG(ROUND((a.Reject / (prod_code.prodcodeUf_MP80_RejectScore2 * a.gross)),2)) > 1
    THEN 'Underperform'
    ELSE 'Perform'
  END AS StatusInMatrixInReject,
  CASE 
    WHEN (SUM(a.Netto) / SUM(a.TargetQty * a.TotalHours)) < 1
    THEN 'Underperform'
    ELSE 'Perform'
  END AS StatusExMatrixExReject,
  CASE 
    WHEN (SUM(a.Netto) / SUM(a.TargetQty * a.TotalHours)) < 1
        OR AVG(ROUND((a.Reject / (prod_code.prodcodeUf_MP80_RejectScore2 * a.gross)),2)) > 1
    THEN 'Underperform'
    ELSE 'Perform'
  END AS StatusExMatrixInReject
FROM {{ ref('MP80_IncentiveMP_v') }} a
LEFT JOIN {{ source('mp_infor', 'hris_user') }} user ON TRIM(a.EmpNum) = user.nik
LEFT JOIN {{ source('mp_infor', 'product_codes_BQ') }} prod_code ON a.ProductCode = prod_code.ProductCode
LEFT JOIN (
    SELECT 
        Job,
        OperNum,
        CASE
            WHEN Efficiency <= 1.0 AND SUBSTR(Job, 1, 4) IN ('JSFT') THEN 90
            ELSE Efficiency
        END AS Efficiency  
    FROM {{ source('mp_infor', 'jobRoutes') }}
) job_route ON a.Job = job_route.Job AND job_route.OperNum = '10'
LEFT JOIN (
    SELECT 
        a.nik,
        a.employee_name,
        FORMAT_DATE('%Y-%m-%d', a.absence_date) AS absence_date,
        user_absence.mins_late AS minutes_late
    FROM {{ source('mp_infor', 'employee_absence') }} a
    LEFT JOIN {{ source('mp_infor', 'user_absence') }} user_absence 
        ON a.id_user_absence = user_absence.id_user_absence
) absence ON TRIM(a.EmpNum) = absence.nik
AND FORMAT_DATE('%Y-%m-%d', a.IncentiveDate) = absence.absence_date
LEFT JOIN {{ ref('job_trans_v') }} JT ON a.Job = JT.Job
LEFT JOIN (
    SELECT 
        Job, 
       AVG(jbrUf_MP80_EmployeePlan) AS EMP_PPIC 
    FROM {{ source('mp_infor', 'jobRoutes') }}
    WHERE OperNum = '10'
    GROUP BY Job
) JR ON a.Job = JR.Job
WHERE (a.Gross > 0 AND a.TargetQty > 0 AND a.TotalHours > 0 AND prod_code.prodcodeUf_MP80_RejectScore2 > 0) 
  AND a.IncentiveDate >= '2025-01-01'
  -- AND a.Job = 'JSFG-87500'
GROUP BY 
  a.Job,
  a.Operation,
  a.JobStatus,
  a.IncentiveDate,
  TRIM(a.EmpNum),
  user.FullName,
  a.Status,
  a.Location,
  a.Item,
  a.Description,
  a.ProductCode,
  TRIM(a.ConeType),
  user.Section,
  user.SectionGroup,
  user.JoinDate,
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
  a.WorkCenter,
  a.ResourceTrans,
  prod_code.Description,
  prod_code.prodcodeUf_MP80_RejectScore2,
  job_route.Efficiency,
  absence.absence_date,
  JT.AHrs,
  JR.EMP_PPIC

