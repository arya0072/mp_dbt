{{
  config(
    materialized= 'table'
  )
}}

SELECT
  fix.*,
  coalesce(Convertion.DecimalValue,1) AS Convertion,
  Convertion.ProductcodeDescription,
  COALESCE(Convertion.DecimalValue,1)*fix.qty_complete_new AS QtyPcs 
FROM  (
SELECT 
    a.Job,
    min_wc.JobrWc,
    a.OperNum,
    SUM(a.QtyMoved) as qty_complete_new,
    CASE 
      WHEN SUM(a.jobtUf_MP55_EmployeeCount) = 0 THEN SUM(c.jobtUf_MP55_EmployeeCount)
      WHEN SUM(a.jobtUf_MP55_EmployeeCount) IS NULL THEN SUM(c.jobtUf_MP55_EmployeeCount)
      ELSE SUM(a.jobtUf_MP55_EmployeeCount)
    END AS jobtUf_MP55_EmployeeCount,
    CASE 
      WHEN SUM(a.AHrs) = 0 AND a.TransType='T' THEN SUM(a.AHrs)
      WHEN SUM(a.AHrs) = 0 THEN SUM(c.AHrs)
      WHEN SUM(a.AHrs) IS NULL THEN SUM(c.AHrs)
      ELSE SUM(a.AHrs)
    END as AHrs,
    CASE
          WHEN SUBSTR(min_wc.JobrWc, 1,3) IN ('C-S','CUT') AND SUBSTR(min_wc.JobrWc, 1,6) NOT IN ('C-SLOT') THEN 'Cutting'
          WHEN SUBSTR(min_wc.JobrWc, 1,2) IN ('P-','PL','RE','RL','RM','RS','UN') AND SUBSTR(min_wc.JobrWc, 1,6) NOT IN ('RLY-OT','RMV-OS','RESTCT') THEN 'Cutting'
          WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('C-SLOT','RLY-OT','RMV-OT','CPPROT') THEN 'Cutting - Overtime'
          WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('RMV-OS') THEN 'Cutting - Outsource'
          ELSE 'NotIdentify'
    END AS JobType,
    a.TransDate,
    a.DowntimeCode,
    a.Shift,
    a.Whse,
    a.JobItem,
    a.JobDescription,
    a.ItemUM,
    a.RESID AS RESID,
    a.RESDescription AS RESDescription,
    AVG(a.JobRate) as JobRate,
    a.TransType AS TransType,
    DownTimeList.DowntimeCodeDescription,
    a.ue_ExptTotalHour,
    a.ue_JobQtyReleased
FROM {{ source('mp_infor', 'job_transaction_mp') }} a 
    LEFT JOIN {{ source('mp_infor', 'DownTimeList') }} DownTimeList ON a.DowntimeCode = DownTimeList.DowntimeCode
    JOIN (SELECT
            UPPER(Job) as Job,
            MAX(OperNum) as OperNum
          FROM {{ source('mp_infor', 'job_transaction_mp') }}
          GROUP BY UPPER(Job)
          ) b ON UPPER(a.Job) = b.Job 
            AND a.OperNum = b.OperNum
    LEFT JOIN (SELECT
                  UPPER(Job) as Job,
                  SUM(jobtUf_MP55_EmployeeCount) as jobtUf_MP55_EmployeeCount,
                  SUM(AHrs) as AHrs,
                  AVG(JobRate) as JobRate
                FROM {{ source('mp_infor', 'job_transaction_mp') }} 
                WHERE jobtUf_MP55_EmployeeCount > 0 
                  -- AND job='JPU-000518' 
                GROUP BY 
                UPPER(Job)) c ON UPPER(a.Job) = c.Job
    JOIN (select
            a.Job,
            a.JobrWc,
            a.OperNum
          from {{ source('mp_infor', 'job_transaction_mp') }} a
            JOIN (select 
                    UPPER(Job) as Job,
                    MIN(OperNum) as opernum        
                  from {{ source('mp_infor', 'job_transaction_mp') }} 
                  group by job
                  ) min_opernum ON UPPER(a.Job) = min_opernum.Job
                                AND a.OperNum = min_opernum.opernum
            -- where a.Job='JCP-019129'
            group by  a.Job, a.JobrWc, a.OperNum
            ) min_wc ON UPPER(a.Job) = min_wc.Job

WHERE SUBSTR(min_wc.JobrWc, 1,3) IN ('C-S','CUT') AND SUBSTR(min_wc.JobrWc, 1,6) NOT IN ('C-SLOT')
  OR SUBSTR(min_wc.JobrWc, 1,2) IN ('P-','PL','RE','RL','RM','RS','UN') AND SUBSTR(min_wc.JobrWc, 1,6) NOT IN ('RLY-OT','RMV-OS','RESTCT')
  OR SUBSTR(min_wc.JobrWc, 1,6) IN ('C-SLOT','RLY-OT','RMV-OT','CPPROT')
  OR SUBSTR(min_wc.JobrWc, 1,6) IN ('RMV-OS')
GROUP BY
  a.Job,
  min_wc.JobrWc,
  a.OperNum,
  JobType,
  a.TransDate,
  a.JobItem,
  a.JobDescription,
  a.ItemUM,
  RESID,
  RESDescription,
  a.TransType,
  a.DowntimeCode,
  a.Shift,
  a.Whse,
  DownTimeList.DowntimeCodeDescription,
  a.ue_ExptTotalHour,
  a.ue_JobQtyReleased
) fix
LEFT JOIN (SELECT
              c.Item,
              c.ProductcodeDescription,
              c.DecimalValue
             FROM {{ ref('item_attributeall_v') }} c
            ) Convertion ON fix.JobItem = Convertion.Item
-- where fix.job IN ('JPU-000518')            