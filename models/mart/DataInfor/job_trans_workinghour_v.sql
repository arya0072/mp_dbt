{{
  config(
    materialized= 'table'
  )
}}

SELECT 
    DISTINCT
    CASE
      WHEN SUBSTR(a.JobrWc, 1,2) IN ('C-') AND SUBSTR(a.RESID, 1, 2) NOT IN ('M-')AND SUBSTR(a.JobrWc,1,4) NOT IN ('C-OQ','C-SP') AND SUBSTR(a.JobrWc,1,6) NOT IN ('C-SLOT')THEN 'Cutting'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('CPPROT', 'CPSTOT','C-SLOT') THEN 'Overtime - Cutting'
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('CUT') AND SUBSTR(a.RESID, 1, 2) NOT IN ('M-') AND SUBSTR(a.JobrWc,1,6) NOT IN ('CUTSMP') THEN 'Cutting'
      WHEN SUBSTR(a.JobrWc,1,6) IN ('CUTSMP') THEN 'Sample'
      WHEN SUBSTR(a.JobrWc, 1,2) IN ('F-') THEN 'Rolling Permanent'
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('HFR') THEN 'Rolling Permanent'
      WHEN SUBSTR(a.JobrWc, 1,2) IN ('H-') THEN 'Rolling Permanent'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('MTRPRP') THEN 'Material Preparation'
      WHEN SUBSTR(a.JobrWc, 1,2) IN ('P-') AND SUBSTR(a.RESID, 1, 2) NOT IN ('M-') AND SUBSTR(a.JobrWc,1,6) NOT IN ('P-FTOT')THEN 'Cutting'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('P-FTOT') THEN 'Overtime - Cutting'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('PBKSMP') THEN 'Sample'
      WHEN SUBSTR(a.JobrWc, 1,2) IN ('PC') AND SUBSTR(a.JobrWc, 1, 4) NOT IN ('PCOS') AND SUBSTR(a.JobrWc, 1, 6) NOT IN ('PCKOVT') THEN 'Packing'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('PCKOVT') THEN 'Overtime - Packing'
      WHEN SUBSTR(a.JobrWc, 1,4) IN ('PCOS') THEN 'Packing Outsource'
      WHEN SUBSTR(a.JobrWc, 1,2) IN ('PL') AND SUBSTR(a.RESID, 1, 2) NOT IN ('M-') AND SUBSTR(a.JobrWc, 1, 6) NOT IN ('PLMNOT') THEN 'Cutting'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('PLMNOT') THEN 'Overtime - Cutting'
      WHEN SUBSTR(a.JobrWc, 1,2) IN ('R-') AND SUBSTR(a.JobrWc, 1, 4) NOT IN ('R-OT') THEN 'Rolling Permanent'
      WHEN SUBSTR(a.JobrWc, 1,4) IN ('R-OT') THEN 'Overtime - Rolling Permanent'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('RESTCT') THEN 'Rolling Permanent'
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('RMV') AND SUBSTR(a.JobrWc, 1, 6) NOT IN ('RMV-OS','RMV-OT') THEN 'Cutting'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('RMV-OS') THEN 'Cutting - Outsource'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('RMV-OT') THEN 'Overtime - Cutting'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('ROLSMP') THEN 'Sample'
      WHEN SUBSTR(a.JobrWc, 1,2) IN ('TF') THEN 'Rolling Magang'
      WHEN SUBSTR(a.JobrWc, 1,2) IN ('T-') and SUBSTR(a.JobrWc, 1,4) NOT IN ('T-KI') THEN 'Rolling Magang'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('UNWBBN') AND SUBSTR(a.RESID, 1, 2) NOT IN ('M-') THEN 'Cutting'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('RESTTM') AND SUBSTR(a.RESID, 1, 2) NOT IN ('M-') THEN 'Cutting'
      WHEN SUBSTR(a.JobrWc, 1,2) IN ('RL') AND SUBSTR(a.RESID, 1, 2) NOT IN ('M-') AND SUBSTR(a.JobrWc, 1, 6) NOT IN ('RLY-OT') THEN 'Cutting'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('RLY-OT') THEN 'Overtime - Cutting'
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('ACR') AND SUBSTR(a.JobrWc,1,6) not in ('ACR-AW')  THEN 'CNC'
      WHEN SUBSTR(a.JobrWc, 1,6) IN ('ACR-AW') THEN 'Workbench'
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('ASY')  THEN 'Assembly'
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('FIN')  THEN 'Packing'
      WHEN SUBSTR(a.JobrWc, 1,3) IN ('PCK')  THEN 'Packing'
      WHEN a.JobrWc = 'T-KI26' THEN 'Rolling Magang'
      WHEN SUBSTR(a.JobrWc, 1,4) = 'T-KI' THEN 'Rolling Permanent'
      ELSE 'NotIdentify'
    END AS WC,
    EXTRACT(MONTH FROM a.TransDate) AS Month,
    EXTRACT(YEAR FROM a.TransDate) AS Year,
    sum(a.AHrs * a.jobtUf_MP55_EmployeeCount) as Working_Hours,
    a.whse
FROM {{ source('mp_infor', 'job_transaction_mp') }} a
group by WC, Month, Year, a.Whse

