{{
  config(
    materialized= 'table'
  )
}}

select
  summary.*,
  summary.MachineAhrs + summary.LaborAhrs + summary.DowntimeAhrs as Ahrs
from (
SELECT distinct
  coalesce(QtyMovedC_10.TransDate,a.TransDate) as TransDate,
  COALESCE(QtyMovedC_10.RESID,a.RESID) AS RESID,
  a.Job,
  CASE 
    WHEN a.TransType IN ('C') AND a.OperNum='20' THEN 0
    WHEN a.TransType  NOT IN ('C') THEN 0
    ELSE  COALESCE(QtyMovedC_10.QtyMovedC, QtyMovedR.QtyMovedR)
    END AS Qty,
  -- a.Opernum,
  a.DowntimeCode,
  b.DowntimeCodeDescription,
  a.TransType,
  a.Whse,
  CASE
    WHEN substr(a.RESID, 1, 7) IN ('JMO-CP1') THEN 'Jembrana Machine Overtime - Cut Paper 1'
    WHEN substr(a.RESID, 1, 7) IN ('M-CSJ01') THEN 'Machine - Cutting Slice Jembrana 01'
    WHEN substr(a.RESID, 1, 7) IN ('M-UNW-1') THEN 'Machine Unwind 01'
    WHEN substr(a.RESID, 1, 7) IN ('M-UNW-2') THEN 'Machine Unwind 02'
    WHEN substr(a.RESID, 1, 5) IN ('M-CTS') THEN 'Machine_Cutting_Slice'
    WHEN substr(a.RESID, 1, 5) IN ('M-PLG') THEN 'Machine_Plong'
    WHEN substr(a.RESID, 1, 5) IN ('M-CTP') THEN 'Machine_Cutting_Paper'
    WHEN substr(a.RESID, 1, 5) IN ('M-PLM') THEN 'Machine_Plong_Manual'
    WHEN substr(a.RESID, 1, 5) IN ('M-UNW') THEN 'Machine_Unwind'
    WHEN substr(a.RESID,1,2) IN ('M1') Then 'Machine_1'
    When substr(a.RESID,1,2) IN ('M2') Then 'Machine_2'
    When substr(a.RESID,1,5) IN ('M-PRT') Then 'Machine_Printing'
    WHEN substr(a.RESID,1,3) IN ('CUT') Then 'Machine_Cutting'
    WHEN substr(a.RESID,1,5) IN ('M-CUT') Then 'Machine_Cutting'
    WHEN substr(a.RESID,1,2) IN ('MS','MB','MR') Then 'Machine_Surabaya'
    WHEN substr(a.RESID,9,14) IN ('CNC-01') Then 'Machine-CNC_1'
    When substr(a.RESID,9,14) IN ('CNC-02') Then 'Machine-CNC_2'
    When substr(a.RESID,9,14) IN ('LSR-01') Then 'Machine-LASER_1'
    When substr(a.RESID,9,14) IN ('LSR-02') Then 'Machine-LASER_2'
    WHEN substr(a.RESID, 1, 7) IN ('M-CSJ01') THEN 'Machine - Cutting Slice Jembrana 01'
    WHEN substr(a.RESID, 1, 7) IN ('M-UNW-1') THEN 'Machine Unwind 01'
    WHEN substr(a.RESID, 1, 7) IN ('M-UNW-2') THEN 'Machine Unwind 02'
    ELSE 'NotIdentify'
  END AS machine_name,
  CASE 
    WHEN a.Shift = 'P1' THEN 'Shift_1'
    WHEN a.Shift = 'P2' THEN 'Shift_2'
    WHEN a.Shift = 'P3' THEN 'Shift_3'
    ELSE shift.shift
  END AS Shift,
  CASE
    WHEN a.TransType = 'C' THEN 'Machine'
    WHEN a.TransType = 'R' THEN 'Labor'
    WHEN a.TransType = 'T' THEN 'Downtime'
  END AS GroupType,
  CASE 
    WHEN a.TransType = 'C' AND a.OperNum='20' THEN a.Ahrs 
    WHEN a.TransType = 'C' AND a.Ahrs=0 THEN shift.Ahrs
    WHEN a.TransType IN ('R','T') THEN 0 
    ELSE a.Ahrs 
  END AS MachineAhrs,
  SUM(CASE WHEN a.TransType = 'R' THEN a.Ahrs ELSE 0 END) AS LaborAhrs,
  CASE 
    WHEN a.TransType = 'T' THEN a.Ahrs 
    ELSE 0 
  END AS DowntimeAhrs,
FROM {{ source('mp_infor', 'job_transaction_mp') }} a
LEFT JOIN {{ source('mp_infor', 'DownTimeList') }} b ON a.DowntimeCode = b.DowntimeCode
LEFT JOIN (SELECT DISTINCT
              a1.Job,
              CASE 
                WHEN a1.Shift = 'P1' THEN 'Shift_1'
                WHEN a1.Shift = 'P2' THEN 'Shift_2'
                WHEN a1.Shift = 'P3' THEN 'Shift_3'
              END AS Shift,
              a1.Ahrs
           FROM {{ source('mp_infor', 'job_transaction_mp') }} a1
           WHERE a1.Shift IS NOT NULL 
             AND a1.TransType = 'R'
           GROUP BY a1.Job, a1.Shift,a1.Ahrs) shift ON a.Job = shift.Job

LEFT JOIN (SELECT DISTINCT
              b1.Job,
              SUM(b1.QtyMoved) AS QtyMovedR
            FROM {{ source('mp_infor', 'job_transaction_mp') }} b1
            WHERE b1.TransType = 'R'
            GROUP BY b1.Job
          ) QtyMovedR ON a.Job = QtyMovedR.Job 

LEFT JOIN (SELECT DISTINCT
              c1.TransDate,
              c1.Job,
              c1.RESID,
              c1.TransType,
              SUM(coalesce(c1.QtyMoved,opernum20.QtyMoved)) AS QtyMovedC
            FROM {{ source('mp_infor', 'job_transaction_mp') }} c1
              LEFT JOIN (SELECT DISTINCT
                          c2.Job,
                          c2.OperNum,
                          c2.QtyMoved
                        FROM {{ source('mp_infor', 'job_transaction_mp') }} c2
                        WHERE c2.OperNum='20') opernum20 ON c1.Job = opernum20.Job
            WHERE c1.TransType = 'C' AND c1.OperNum='10'
            GROUP BY c1.TransDate,
              c1.Job,
              c1.RESID,
              c1.TransType
          ) QtyMovedC_10 ON a.Job = QtyMovedC_10.Job 
                         AND a.TransType = QtyMovedC_10.TransType

-- WHERE a.Job = 'JPU-000580'
GROUP BY 
    QtyMovedC_10.RESID,
    a.RESID,
    CASE 
    WHEN a.TransType IN ('C') AND a.OperNum='20' THEN 0
    WHEN a.TransType  NOT IN ('C') THEN 0
    ELSE  COALESCE(QtyMovedC_10.QtyMovedC, QtyMovedR.QtyMovedR)
    END,
    a.TransDate,
    QtyMovedC_10.TransDate, 
    a.Job, a.DowntimeCode, 
    b.DowntimeCodeDescription, 
    a.TransType, 
    a.Whse,
    CASE 
      WHEN a.Shift = 'P1' THEN 'Shift_1'
      WHEN a.Shift = 'P2' THEN 'Shift_2'
      WHEN a.Shift = 'P3' THEN 'Shift_3'
      ELSE shift.Shift
    END,
    a.Ahrs,
    QtyMovedC_10.QtyMovedC,
    QtyMovedR.QtyMovedR,
    CASE 
      WHEN a.TransType = 'C' AND a.OperNum='20' THEN a.Ahrs 
      WHEN a.TransType = 'C' AND a.Ahrs=0 THEN shift.Ahrs 
      WHEN a.TransType IN ('R','T') THEN 0 
      ELSE a.Ahrs 
    END ) summary