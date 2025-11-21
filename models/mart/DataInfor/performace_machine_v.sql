{{
  config(
    materialized= 'table'
  )
}}

SELECT DISTINCT
  a.Job,
  a.JobItem,
  a.JobDescription,
  a.JobrWc,
  a.ItemUM,
  a.TransDate,
  -- f.JobDate,
  -- CASE 
  -- WHEN EXTRACT(MONTH FROM a.TransDate) = EXTRACT(MONTH FROM f.JobDate)
  --      AND EXTRACT(YEAR FROM a.TransDate) = EXTRACT(YEAR FROM f.JobDate) THEN '0'
  -- WHEN a.TransDate > f.JobDate 
  --      THEN CAST(DATE_DIFF(DATE(a.TransDate), DATE(f.JobDate), MONTH) AS STRING)
  -- ELSE 'NotIdentify'
  -- END AS Var_Month,
  a.TransType,
  EXTRACT(MONTH FROM a.TransDate) AS Month,
  EXTRACT(YEAR FROM a.TransDate) AS Year,
  a.qty_complete_new AS QtyComplete,
  a.QtyScrapped,
  b.MachineAHrs,
  c.ProductCode,
  c.ProductCodeDescription,
  d.ue_TotalMaterialCost,
  d.ue_TotalLaborCost,
  d.ue_TotalFovhdCost,
  d.ue_TotalVovhdCost,
  e.DerRunMchHrs,
  a.jobtUf_MP55_EmployeeCount,
  IFNULL(ct.AttributeValue, 'NotIdentify') AS Cone_Type,
  IFNULL(ct.QtypcsperSheet, 0) AS Qtypcs_Sheet,
  a.qty_complete_new * ct.QtypcsperSheet AS Qty_Pcs,
  SAFE_DIVIDE(a.qty_complete_new, b.MachineAHrs) AS Output_perhour
FROM {{ ref('job_trans_jmbr_v') }} a
JOIN (
  SELECT 
    Job,
    CASE 
      WHEN SUM(CASE WHEN TransType IN ('C','T') THEN AHrs ELSE 0 END) = 0 
        THEN MAX(AHrs)
      ELSE SUM(CASE WHEN TransType IN ('C','T') THEN AHrs ELSE 0 END)
    END AS MachineAHrs
  FROM {{ ref('job_trans_jmbr_v') }}
  GROUP BY Job
) b ON a.Job = b.Job
JOIN {{ ref('item_information_v') }} c 
  ON a.JobItem = c.Item
LEFT JOIN (
  SELECT 
    Item, 
    AttributeValue,
    CASE	
      WHEN AttributeValue = 'King' THEN 45
      WHEN AttributeValue = 'Mini' THEN 45
      WHEN AttributeValue = 'Single' THEN 45
      WHEN AttributeValue = 'Bomb' THEN 35
      WHEN AttributeValue = 'Tube' THEN 30
      WHEN AttributeValue = 'Reefer' THEN 40
      WHEN AttributeValue = 'Bijou' THEN 40
      WHEN AttributeValue = 'One-Degree' THEN 30
      WHEN AttributeValue = 'Fatboy' THEN 14
      WHEN AttributeValue = 'KMQ' THEN 45
      WHEN AttributeValue = 'KMQ (Bead)' THEN 32
      WHEN AttributeValue = 'Reefer (Z Style)' THEN 54
      WHEN AttributeValue = 'Reefer (Z Type)' THEN 54
      ELSE 0
    END AS QtypcsperSheet
  FROM {{ ref('item_information_v') }} 
  WHERE AttributeLabel = 'Cone Type'
) ct ON a.JobItem = ct.Item
LEFT JOIN (
  SELECT
    ue_Job,
    ue_TotalMaterialCost,
    ue_TotalLaborCost,
    ue_TotalFovhdCost,
    ue_TotalVovhdCost
  FROM {{ ref('cop_all_jmbr_new_v') }} 
  WHERE ue_ItemType = 'Item'
) d ON a.Job = d.ue_Job
JOIN {{ source('mp_infor', 'current_operation') }} e ON a.JobItem = e.DerJobItem
-- JOIN `mitraprodin-data-warehouse.mp_infor.Jobs` f ON a.Job = f.Job
WHERE a.TransType = 'C' AND SUBSTR(a.JobrWc, 1, 4) = 'M-PR' AND a.Whse = 'PRJM'
-- WHERE a.Job = 'JFCJ-00001' AND a.TransType = 'C'
GROUP BY 
  a.Job,
  a.JobItem,
  a.JobDescription,
  a.JobrWc,
  a.ItemUM,
  a.TransDate,
  -- f.JobDate,
  a.TransType,
  EXTRACT(MONTH FROM a.TransDate),
  EXTRACT(YEAR FROM a.TransDate),
  a.qty_complete_new,
  a.QtyScrapped,
  b.MachineAHrs,
  c.ProductCode,
  c.ProductCodeDescription,
  d.ue_TotalMaterialCost,
  d.ue_TotalLaborCost,
  d.ue_TotalFovhdCost,
  d.ue_TotalVovhdCost,
  e.DerRunMchHrs,
  a.jobtUf_MP55_EmployeeCount,
  ct.AttributeValue,
  ct.QtypcsperSheet


