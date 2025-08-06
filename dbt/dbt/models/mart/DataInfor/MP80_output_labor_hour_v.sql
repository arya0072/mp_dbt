{{
  config(
    materialized= 'table'
  )
}}

SELECT DISTINCT
    TRIM(a.EmpName) AS EmpName,
    a.Job,
    a.wc,
    a.Loc,
    a.JobItem,
    a.JobItemDesc,
    a.ProductCode,
    a.TotalHours,
    a.Gross,
    a.Netto,
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
    DATE(a.IncentiveDate) AS Date,
CASE
    WHEN SUBSTR(a.Job, 1, 4) IN ('JFCJ', 'JFGJ', 'JPPJ', 'JSFJ', 'JSFG', 'JSFS', 'JSFM') THEN 'Permanent'
    WHEN SUBSTR(a.Job, 1, 3) IN ('JHR', 'JOS', 'JOT', 'JPU', 'JCP', 'JFP', 'JFC', 'JFG', 'JSM') THEN 'Permanent'
    WHEN SUBSTR(a.Job, 1, 4) IN ('JHRT', 'JOTT', 'JSTJ', 'JSTT', 'JSFT') THEN 'Magang'
    WHEN SUBSTR(a.Job, 1, 5) IN ('JSFTS', 'JSTMJ') THEN 'Magang'
    WHEN SUBSTR(a.Job, 1, 4) IN ('JFGO', 'JFOS', 'JFGO') THEN 'Outsource'
    WHEN SUBSTR(a.Job, 1, 5) IN ('JFGOJ') THEN 'Outsource'
    WHEN SUBSTR(a.Job, 1, 4) IN ('JSFO', 'JPUO', 'JFPO') THEN 'Overtime'
    WHEN SUBSTR(a.Job, 1, 5) IN ('JSFJO', 'JFGOT') THEN 'Overtime'
    WHEN SUBSTR(a.Job, 1, 4) IN ('JSFB') THEN 'Borongan'
    ELSE 'Not Identify'
END AS CategoryJO,
    a.TargetQty
    -- b.productcodedescription
FROM {{ source('mp_infor', 'mp80_incentives') }} a
--  LEFT JOIN `mp_infor.item_productcode_v` b ON a.ProductCode = b.ProductCode
--                                             AND a.JobItem = b.Item

