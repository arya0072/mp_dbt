{{
  config(
    materialized= 'table'
  )
}}

SELECT
  TRIM(a.EmpNum) as EmpNum,
  user.FullName as EmployeeName,
  a.EmpStatus as Status,
  user.Section as Resource,
  a.RlcResource as ResourceTrans,
  user.SectionGroup as ResourceGroup,
  a.Job,
  SUBSTR(a.Job, 1, 4) AS JobPrefix,
  DATE(a.IncentiveDate) AS IncentiveDate,
  DATE(
    CASE 
      WHEN EXTRACT(DAY FROM a.IncentiveDate) >= 26 THEN 
        DATE_ADD(DATE_TRUNC(DATE(a.IncentiveDate), MONTH), INTERVAL 1 MONTH)
      ELSE 
        DATE_TRUNC(DATE(a.IncentiveDate), MONTH)
    END
  ) AS PeriodeDate, 
  FORMAT_DATE(
    '%Y-%m', 
      CASE 
        WHEN EXTRACT(DAY FROM a.IncentiveDate) >= 26 THEN 
          DATE_ADD(DATE_TRUNC(DATE(a.IncentiveDate), MONTH), INTERVAL 1 MONTH)
        ELSE 
          DATE_TRUNC(DATE(a.IncentiveDate), MONTH)
      END
  ) AS Periode,
  SUBSTR(a.ProductCode, 1, 3) AS ProductCode,
  prod_code.Description as ProdCodeDesc,
  TRIM(a.ConeType) AS ConeType,
  a.JobItem as Item,
  a.JobItemDesc as Description,
  a.JobOper as Operation,
  a.JobStat as JobStatus,
  a.Loc as Location,
  a.wc as WorkCenter,
  a.Gross,
  a.Reject,
  ROUND((prod_code.prodcodeUf_MP80_RejectScore2 * a.gross),0) AS TargetReject,
  ROUND(((a.Reject / (prod_code.prodcodeUf_MP80_RejectScore2 * a.gross))),2) AS ActRejectRate,
  ROUND((a.Reject / (prod_code.prodcodeUf_MP80_RejectScore2 * a.gross)),2) AS ActPercenReject,
  a.NettoExcl,
  a.Netto,
  a.TargetQty AS TargetNettPerHours,
  (a.TargetQty * jt.AHrs) AS TargetNett,
  ROUND((a.Netto / a.TotalHours),2) AS ActualCPH,
  a.TotalHours AS WHMP80,
  jt.AHrs AS WHJT,
  ROUND((job_route.Efficiency / 90),2) AS AdjusmentProd,
  b.TotalJob as TotalJob,
  CASE 
    WHEN  ROUND((job_route.Efficiency / 90),2) >= 1 THEN 'Tidak Ganti Lintingan'
    ELSE 'Ganti Lintingan'
    END AS Matrix,
  CASE 
    WHEN ROUND((job_route.Efficiency / 90),2) >= 1 THEN 3 
    ELSE 0 
  END AS MaxPointPerformance,
  CASE 
    WHEN ROUND((job_route.Efficiency / 90),2) >= 1 THEN COALESCE(1/b.totalJob,0)
    ELSE 0
  END AS AbsencePoint,
  CASE 
    WHEN ROUND((job_route.Efficiency / 90),2) < 1 THEN 0
    WHEN ROUND((a.Netto / a.TotalHours),2) > a.TargetQty AND a.Reject <= ROUND((prod_code.prodcodeUf_MP80_RejectScore2 * a.gross),0) THEN 3
    WHEN ROUND((a.Netto / a.TotalHours),2) > a.TargetQty AND a.Reject > ROUND((prod_code.prodcodeUf_MP80_RejectScore2 * a.gross),0) THEN 2
    WHEN ROUND((a.Netto / a.TotalHours),2) < a.TargetQty AND a.Reject > ROUND((prod_code.prodcodeUf_MP80_RejectScore2 * a.gross),0) THEN 0
    WHEN ROUND((a.Netto / a.TotalHours),2) < a.TargetQty AND a.Reject <= ROUND((prod_code.prodcodeUf_MP80_RejectScore2 * a.gross),0) THEN 0
  ELSE NULL
  END AS PointActualPerformance,
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
  LEFT JOIN {{ source('mp_infor', 'jobRoutes') }} job_route ON a.Job = job_route.Job AND job_route.OperNum = '10'
  LEFT JOIN (SELECT  
              TRIM(a.empnum) AS NIK,  
              DATE(a.IncentiveDate) AS IncentiveDate,  
              COUNT(DISTINCT a.job) AS TotalJob  
            FROM {{ source('mp_infor', 'mp80_incentives') }} a  
            WHERE SUBSTR(a.Job, 1, 4) IN ('JSFG','JSFO')   
            GROUP BY NIK, IncentiveDate
            ) b ON b.NIK = TRIM(a.EmpNum) 
                AND b.IncentiveDate = DATE(a.IncentiveDate)
   LEFT JOIN {{ ref('job_trans_v') }} jt ON a.Job = jt.Job             
WHERE (a.Gross > 0 AND a.TargetQty > 0 AND a.TotalHours > 0 AND prod_code.prodcodeUf_MP80_RejectScore2 > 0) 
  AND DATE(a.IncentiveDate) >= '2025-01-25'
  -- AND DATE(a.IncentiveDate) BETWEEN	'2025-01-26' AND '2025-02-25'
  -- AND TRIM(a.EmpNum) = '170019'
  -- AND a.Job= 'JSFG-79324'
  AND SUBSTR(a.Job, 1, 4) IN ('JSFG','JSFO','JSFJ','JSMJ')  -- JO Gianyar & Jembrana
  AND a.Job NOT IN (SELECT ue_Job FROM `mp_infor.JobExclude`) -- Exclude Job Special Case
GROUP BY  
  a.Job,
  a.JobOper,
  a.JobStat,
  a.IncentiveDate,
  TRIM(a.EmpNum),
  user.FullName,
  a.EmpStatus,
  a.Loc,
  a.JobItem,
  a.JobItemDesc,
  a.ProductCode,
  TRIM(a.ConeType),
  user.Section,
  user.SectionGroup,
  a.Gross,
  a.Reject,
  a.NettoExcl,
  a.Netto,
  a.TargetQty,
  a.TotalHours,
  a.wc,
  a.RlcResource,
  prod_code.Description,
  prod_code.prodcodeUf_MP80_RejectScore2,
  job_route.Efficiency,
  b.TotalJob,
  jt.AHrs

  