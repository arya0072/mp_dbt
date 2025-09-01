{{
  config(
    materialized= 'table'
  )
}}

SELECT
    TRIM(a.EmployeeName) AS EmpName,
    a.Job,
    a.WorkCenter AS wc,
    a.Location AS Loc,
    a.Item AS JobItem,
    a.Description AS JobItemDesc,
    a.ProductCode,
    SUM(a.TotalHours) AS TotalHours,
    SUM(a.Gross) AS Gross,
    SUM(a.Netto) AS Netto,
    SUM(a.BA) AS BA,
    SUM(a.BLT) AS BLT,
    SUM(a.BS) AS BS,
    SUM(a.CS) AS CS,
    SUM(a.DLL) AS DLL,
    SUM(a.EXT) AS EXT,
    SUM(a.FL) AS FL,
    SUM(a.FM) AS FM,
    SUM(a.FT) AS FT,
    SUM(a.FTZ) AS FTZ,
    SUM(a.KR) AS KR,
    SUM(a.OT) AS OT,
    SUM(a.OV) AS OV,
    SUM(a.NAL) AS NAL,
    DATE(a.IncentiveDate) AS Date,
    CASE
        WHEN SUBSTR(a.Job, 1, 4) IN ('JFCJ', 'JFGJ', 'JPPJ', 'JSFJ', 'JSFG', 'JSFS', 'JSFM') THEN 'Permanent'
        WHEN SUBSTR(a.Job, 1, 3) IN ('JHR', 'JOS', 'JOT', 'JPU', 'JCP', 'JFP', 'JFC', 'JFG', 'JSM') THEN 'Permanent'
        WHEN SUBSTR(a.Job, 1, 4) IN ('JHRT', 'JOTT', 'JSTJ', 'JSTT', 'JSFT') THEN 'Magang'
        WHEN SUBSTR(a.Job, 1, 5) IN ('JSFTS', 'JSTMJ') THEN 'Magang'
        WHEN SUBSTR(a.Job, 1, 4) IN ('JFGO', 'JFOS') THEN 'Outsource'
        WHEN SUBSTR(a.Job, 1, 5) IN ('JFGOJ') THEN 'Outsource'
        WHEN SUBSTR(a.Job, 1, 4) IN ('JSFO', 'JPUO', 'JFPO') THEN 'Overtime'
        WHEN SUBSTR(a.Job, 1, 5) IN ('JSFJO', 'JFGOT') THEN 'Overtime'
        WHEN SUBSTR(a.Job, 1, 4) IN ('JSFB') THEN 'Borongan'
        ELSE 'Not Identify'
    END AS CategoryJO,
    SUM(a.TargetQty) AS TargetQty,
    a.Resource,
    a.ResourceTrans,
    CASE 
        WHEN AVG(a.TargetQty) < (SUM(a.Netto)/SUM(a.TotalHours)) THEN 'Capai Target'
        ELSE 'Tidak Tercapai'
    END AS Achivement
FROM {{ ref('MP80_IncentiveMP_v') }} a
WHERE a.TotalHours > 0
GROUP BY
    EmpName, a.Job, wc, Loc, JobItem, JobItemDesc, a.ProductCode,
    Date, CategoryJO, a.Resource, a.ResourceTrans
