{{
  config(
    materialized= 'table'
  )
}}

SELECT
    a.LstTrxDate AS Date,
    a.Job,
    a.Item,
    a.ItDesc,
    a.Location,
    a.ProdCode,
    a.Resource,
    a.WC,
    a.EmpCount AS EmpJT,
    COUNT(b.Empnum) AS Emp80,
    a.EmpCount - COUNT(b.EmpNum) AS EmpSupp, -- Perbaikan EmpSupp
    CASE
        WHEN a.ProdCode IN ('SCN-SPC', 'SCN-ONQ', 'SCN-KIN', 'SCN-MED') THEN 'KMQ'
        WHEN a.ProdCode = 'SCN-FAT' THEN 'FATBOY'
        WHEN a.ProdCode = 'SCN-SLI' THEN 'SLIM'
        WHEN a.ProdCode IN ('SCN-MZ26', 'SCN-SFI', 'SCN-TIN') THEN 'MTS'
        WHEN a.ProdCode IN ('SCN-BL26', 'SCN-OBD', 'SCN-RGD') THEN 'NEWPRODUCT'
        WHEN a.ProdCode = 'SCN-PAR26' THEN 'Party'
        WHEN a.ProdCode = 'SCN-PAR30' THEN 'PEACEMAKER'
        WHEN a.ProdCode IN ('SCN-REZ', 'SCN-RZ30') THEN 'ReeferZigZag'
        WHEN a.ProdCode IN ('SCN-SIN', 'SCN-SIN24', 'SCN-SIN30') THEN 'SINGLE'
        WHEN a.ProdCode IN ('SCN-DEG', 'SCN-LEA', 'SCN-MIN', 'SCN-REE') THEN 'SPIRAL'
        ELSE 'Others'
    END AS Category,
    ANY_VALUE(b.ConeType) AS ConeType, -- Menggunakan ANY_VALUE agar tidak perlu masuk GROUP BY
    AVG(b.AdjusmentProd) AS Matrix,
    SUM(b.Gross) AS Actual_Gross,
    SUM(b.Netto) AS Actual_Nett,
    SUM(b.Target) AS Target_Nett,
    SUM(b.TotalHours)/ COUNT(b.Empnum) AS Total_Jam,
    SUM(b.Target) / NULLIF(SUM(b.TotalHours), 0) AS Target_CPH, -- Menghindari division by zero
    SUM(b.Reject) AS Total_Reject,
    SUM(b.TargetReject) AS Total_TargetReject,
    (SUM(b.Reject) / NULLIF(SUM(b.Gross), 0)) * 100 AS Reject_Percentage, -- Menghindari division by zero
    CASE
        WHEN AVG(b.AdjusmentProd) < 1 THEN 'Ganti_Lintingan'
        ELSE 'Tidak_Ganti_Lintingan'
    END AS Ganti_Lintingan
FROM {{ ref('mp83_joborder_progress_v') }}a
    LEFT JOIN {{ ref('MP80IncentiveDetail_v') }} b 
    ON a.Job = b.Job
WHERE
    a.WC <> 'RESTCT'
    -- AND a.Job = 'JSFG-80749'
GROUP BY
    a.LstTrxDate,
    a.Job,
    a.Item,
    a.ItDesc,
    a.Location,
    a.ProdCode,
    a.Resource,
    a.WC,
    a.EmpCount