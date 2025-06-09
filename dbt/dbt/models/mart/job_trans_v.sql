{{
  config(
    materialized= 'table'
  )
}}

SELECT
  job_trans.*,
  coalesce(Convertion.DecimalValue,1) AS Convertion,
  Convertion.ProductcodeDescription,
  COALESCE(Convertion.DecimalValue,1)*job_trans.qty_complete_new AS QtyPcs 
FROM (
SELECT 
  UPPER(a.Job) as Job,
  min_wc.JobrWc,
  max_opernum.opernum,
  CASE
    WHEN max_opernum.opernum = '20' THEN a.QtyMoved
    ELSE b.QtyMoved 
  END as qty_complete_new,
  b.jobtUf_MP55_EmployeeCount,
  b.AHrs,
  CASE
        WHEN SUBSTR(min_wc.JobrWc, 1,2) IN ('F-','HF') AND SUBSTR(min_wc.JobrWc, 1,6) NOT IN ('F-TIPS') THEN 'HRF - Rolling Filter'
        WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('F-TIPS') THEN 'Tip - Rolling Filter Tip'
        WHEN SUBSTR(min_wc.JobrWc, 1,2) IN ('H-','R-') AND SUBSTR(min_wc.JobrWc, 1,4) NOT IN ('R-OT','R-AS','R-CH','R-MG') THEN 'Cones - Rolling'
        WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('RESTCT') AND SUBSTR(a.JobItem, 1,4) NOT IN ('5301','5302') THEN 'Cones - Rolling'
        WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('RESTCT') AND SUBSTR(a.JobItem, 1,4) = '5301'THEN 'HRF - Rolling Filter'
        WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('RESTCT') AND SUBSTR(a.JobItem, 1,4) = '5302'THEN 'Tip - Rolling Filter Tip'
        WHEN SUBSTR(min_wc.JobrWc, 1,4) = 'R-OT' AND SUBSTR(a.JobItem, 1,2) = '51' THEN 'Cones - Overtime Rolling'
        WHEN SUBSTR(min_wc.JobrWc, 1,4) = 'R-OT' AND SUBSTR(a.JobItem, 1,4) = '5301' THEN 'HRF - Overtime Rolling Filter'
        WHEN SUBSTR(min_wc.JobrWc, 1,4) = 'R-OT' AND SUBSTR(a.JobItem, 1,4) = '5302' THEN 'Tip - Overtime Rolling Filter Tip'
        WHEN SUBSTR(min_wc.JobrWc, 1,2) IN ('T-') THEN 'Cones - Magang Rolling'
        WHEN SUBSTR(min_wc.JobrWc, 1,2) IN ('TF')AND SUBSTR(min_wc.JobrWc, 1,6) NOT IN ('TF-TIP') THEN 'HRF - Magang Rolling Filter'
        WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('TF-TIP') THEN 'Tip - Magang Rolling Filter Tip'
        WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('PCKBLK') THEN 'Packing Bulk'
        WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('PCKCSP') THEN 'Packing Consumer Pack'
        WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('PCKFTT') THEN 'Packing Filter Tip'
        WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('PCKOVT') THEN 'Packing Bulk - Overtime'
        WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('PCOSBL') THEN 'Packing Bulk - Outsource'
        WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('PCOSCS') THEN 'Packing Consumer Pack - Outsource'
        WHEN SUBSTR(min_wc.JobrWc, 4,6) IN ('SMP') THEN 'Sample'
        WHEN SUBSTR(min_wc.JobrWc, 1,4) IN ('R-AS','R-CH','R-MG') THEN 'Sample'
        WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('MTRPRP') THEN 'Material Preparation'
        WHEN SUBSTR(min_wc.JobrWc, 1,4) IN ('M-HC') THEN 'HRC - Rolling Machine'
        WHEN SUBSTR(min_wc.JobrWc, 1,4) IN ('M-PR') THEN 'Filter - Printing'
        WHEN SUBSTR(min_wc.JobrWc, 1,4) IN ('M-KI') THEN 'Mesin - Rolling'
        WHEN SUBSTR(min_wc.JobrWc, 1,4) IN ('TMKI') THEN 'Mesin Rolling - Magang'
        WHEN SUBSTR(min_wc.JobrWc, 1,3) IN ('ACR')  THEN 'Acrylic'
        WHEN SUBSTR(min_wc.JobrWc, 1,3) IN ('ASY')  THEN 'Assembly'
        WHEN SUBSTR(min_wc.JobrWc, 1,3) IN ('FIN')  THEN 'Finishing'
        WHEN SUBSTR(min_wc.JobrWc, 1,3) IN ('PCK')  THEN 'Packing'
        WHEN SUBSTR(min_wc.JobrWc, 1,3) IN ('QC')  THEN 'QC'
        WHEN SUBSTR(min_wc.JobrWc, 1,4) IN ('T-KI') AND SUBSTR(min_wc.JobrWc, 1,6) NOT IN ('T-KI26') THEN 'Tipper - Rolling'
        WHEN SUBSTR(min_wc.JobrWc, 1,6) IN ('T-KI26') THEN 'Cones - Magang Rolling' 
        WHEN SUBSTR(min_wc.JobrWc, 1,4) IN ('T-OQ') THEN 'Tipper - Rolling' 
        WHEN SUBSTR(min_wc.JobrWc, 1,3) IN ('T-MD') THEN 'Tipper - Rolling'
        WHEN SUBSTR(min_wc.JobrWc, 1,3) IN ('TT-') THEN 'Tipper - Magang Rolling'
        WHEN SUBSTR(min_wc.JobrWc, 1,4) IN ('TP-M') THEN 'Tipper - Rolling'
        WHEN SUBSTR(min_wc.JobrWc, 1,4) IN ('TP-O') THEN 'Tipper - Rolling'
        ELSE 'NotIdentify'
    END as JobType,
    a.TransDate,
    a.JobItem,
    a.JobDescription,
    a.ItemUM,
    b.RESID,
    b.RESDescription,
    b.JobRate,
    b.TransType,
    a.DowntimeCode,
    a.Shift,
    a.Whse,
    a.QtyScrapped,
    CASE
      WHEN b.TransType NOT IN ('R','C') THEN 0
      else b.jobtUf_MP55_EmployeeCount
    end as emp_count
FROM {{ source('mp_infor', 'job_transaction_mp') }} a
  JOIN (select 
          UPPER(Job) as Job,
          MAX(OperNum) as opernum        
          from {{ source('mp_infor', 'job_transaction_mp') }}
          -- where Job='JFG-016328' 
          group by job) max_opernum ON UPPER(a.Job) = UPPER(max_opernum.Job)
                                    AND a.OperNum = max_opernum.opernum
  JOIN (
        SELECT 
          UPPER(xx.Job) as Job,
          CASE 
            WHEN xx.jobtUf_MP55_EmployeeCount = 0 THEN zz.jobtUf_MP55_EmployeeCount
            WHEN xx.jobtUf_MP55_EmployeeCount IS NULL THEN zz.jobtUf_MP55_EmployeeCount
            ELSE xx.jobtUf_MP55_EmployeeCount
          END AS jobtUf_MP55_EmployeeCount,
          CASE 
            WHEN SUM(xx.AHrs) = 0 THEN zz.AHrs
            ELSE SUM(xx.AHrs)  
          END as AHrs,
          CASE 
            WHEN SUM(xx.JobRate) = 0 THEN zz.JobRate
            ELSE SUM(xx.JobRate)  
          END as JobRate,
          xx.RESID,
          xx.RESDescription,
          xx.TransType,
          xx.JobrWc,
          SUM(xx.QtyMoved) as QtyMoved,
          MAX(ue_CreateDate) as ue_CreateDate
        FROM {{ source('mp_infor', 'job_transaction_mp') }} xx
          JOIN (SELECT
                  UPPER(Job) as Job,
                  MAX(OperNum) as OperNum
                FROM {{ source('mp_infor', 'job_transaction_mp') }}
                -- WHERE Job IN ('JSFT-06724','JSFG-61358','JFG-016328','JSFT-06814') 
                GROUP BY UPPER(Job)
                ) yy ON UPPER(xx.Job) = UPPER(yy.Job) 
                    AND xx.OperNum = yy.OperNum
          LEFT JOIN (SELECT
                        UPPER(Job) as Job,
                        jobtUf_MP55_EmployeeCount,
                        SUM(AHrs) as AHrs,
                        JobRate
                      FROM {{ source('mp_infor', 'job_transaction_mp') }}
                      WHERE jobtUf_MP55_EmployeeCount > 0 
                      GROUP BY 
                      UPPER(Job),
                      jobtUf_MP55_EmployeeCount,
                      JobRate) zz ON UPPER(xx.Job) = UPPER(zz.Job) 
        -- WHERE xx.Job IN ('JSFT-06724','JSFG-61358','JFG-016328','JSFT-06814')
        GROUP BY
          UPPER(xx.Job),
          zz.jobtUf_MP55_EmployeeCount,
          xx.jobtUf_MP55_EmployeeCount,
          xx.JobRate,
          xx.RESID,
          xx.RESDescription,
          xx.TransType,
          xx.JobrWc,
          zz.AHrs,
          zz.JobRate
        ) b ON UPPER(a.Job) = UPPER(b.Job)
            AND a.ue_CreateDate = b.ue_CreateDate

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
                        ) min_opernum ON UPPER(a.Job) = UPPER(min_opernum.Job)
                                        AND a.OperNum = min_opernum.opernum
                  ) min_wc ON UPPER(a.Job) = UPPER(min_wc.Job)


  -- WHERE a.Job IN ('JFG-016328','JSFO-03735','JSFG-61110','JSM-000657','JSFT-06724','JSFG-61761','JSFG-61138','JFG-016250','JSFT-06814','JCP-018564','JSFT-06968')  
  GROUP BY 
    UPPER(a.Job),
    max_opernum.opernum,
    b.jobtUf_MP55_EmployeeCount,
    b.AHrs,
    min_wc.JobrWc,
    JobType,
    a.TransDate,
    a.JobItem,
    a.JobDescription,
    a.ItemUM,
    b.RESID,
    b.RESDescription,
    b.JobRate,
    b.TransType,
    a.DowntimeCode,
    a.Shift,
    a.Whse,
    a.QtyScrapped,
    CASE
    WHEN max_opernum.opernum = '20' THEN a.QtyMoved
    ELSE b.QtyMoved 
    END
    ) job_trans

    LEFT JOIN (SELECT
              c.Item,
              c.ProductcodeDescription,
              c.DecimalValue
             FROM {{ ref('item_attributeall_v') }} c
            ) Convertion ON job_trans.JobItem = Convertion.Item
    WHERE  job_trans.AHrs <> 0
    -- AND (
    --       SUBSTR(job_trans.JobrWc, 1,3) NOT IN ('C-S', 'CUT') AND SUBSTR(job_trans.JobrWc, 1,6) NOT IN ('C-SLOT')
    --       OR SUBSTR(job_trans.JobrWc, 1,2) NOT IN ('P-', 'PL', 'RE', 'RL', 'RM', 'RS', 'UN') AND SUBSTR(job_trans.JobrWc, 1,6) NOT IN ('RLY-OT', 'RMV-OS', 'RESTCT')
    --       OR SUBSTR(job_trans.JobrWc, 1,6) NOT IN ('C-SLOT', 'RLY-OT', 'RMV-OT')
    --       OR SUBSTR(job_trans.JobrWc, 1,6) NOT IN ('RMV-OS')
    --     )
    order by job_trans.Job asc