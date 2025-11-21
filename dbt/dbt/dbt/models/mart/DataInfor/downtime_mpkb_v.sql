{{
  config(
    materialized= 'table'
  )
}}

SELECT distinct
  a.TransDate,
  a.RESID,
  a.Job,
  sum(a.QtyMoved) AS Qty,
  a.DowntimeCode,
  b.DowntimeCodeDescription,
  a.TransType,
  a.Whse,
  CASE
    WHEN substr(a.RESID,9,14) IN ('CNC-01') Then 'Machine-CNC_1'
    When substr(a.RESID,9,14) IN ('CNC-02') Then 'Machine-CNC_2'
    When substr(a.RESID,9,14) IN ('LSR-01') Then 'Machine-LASER_1'
    When substr(a.RESID,9,14) IN ('LSR-02') Then 'Machine-LASER_2'
    ELSE 'NotIdentify'
  END AS machine_name,
  CASE 
    When a.Shift = 'P1' Then 'Shift_1'
    When a.Shift = 'P2' Then 'Shift_2'
    When a.Shift = 'P3' Then 'Shift_3'
    Else shift.shift
  END AS Shift,
  CASE
    WHEN TransType = 'C' THEN 'Machine'
    WHEN TransType = 'R' THEN 'Labor'
    WHEN TransType = 'T' THEN 'Downtime'
  END AS GroupType,
  SUM(CASE WHEN TransType = 'C' THEN Ahrs ELSE 0 END) AS MachineAhrs,
  SUM(CASE WHEN TransType = 'R' THEN Ahrs ELSE 0 END) AS LaborAhrs,
  SUM(CASE WHEN TransType = 'T' THEN Ahrs ELSE 0 END) AS DowntimeAhrs,
  sum(Ahrs) AS Ahrs
FROM {{ source('mp_infor', 'job_transaction_mpkb') }} a
  LEFT JOIN {{ source('mp_infor', 'DownTimeList') }} b ON a.DowntimeCode = b.DowntimeCode
  LEFT JOIN (SELECT
                a1.Job,
                 CASE 
                    When a1.Shift = 'P1' Then 'Shift_1'
                    When a1.Shift = 'P2' Then 'Shift_2'
                    When a1.Shift = 'P3' Then 'Shift_3'
                  end as shift
              FROM {{ source('mp_infor', 'job_transaction_mpkb') }} a1
              WHERE a1.Shift IS NOT NULL AND a1.TransType='T'
              GROUP BY a1.Job,
                a1.Shift) shift ON a.Job = shift.Job

-- where a.Job = 'JOS-000610'
GROUP BY 
  a.RESID, a.TransDate, a.Job, a.DowntimeCode, b.DowntimeCodeDescription, a.TransType,a.Whse, CASE 
    When a.Shift = 'P1' Then 'Shift_1'
    When a.Shift = 'P2' Then 'Shift_2'
    When a.Shift = 'P3' Then 'Shift_3'
    Else shift.Shift
  END 