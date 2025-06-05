{{
  config(
    materialized: 'table'
  )
}}

SELECT
  a.NIK,
  a.full_name AS EmployeeName,
  DATE(
    CASE 
      WHEN EXTRACT(DAY FROM a.start_date) >= 21 THEN 
        DATE_ADD(DATE_TRUNC(DATE(a.start_date), MONTH), INTERVAL 1 MONTH)
      ELSE 
        DATE_TRUNC(DATE(a.start_date), MONTH)
    END
  ) AS PeriodeDate, 
  FORMAT_DATE(
    '%Y-%m', 
    CASE 
      WHEN EXTRACT(DAY FROM a.start_date) >= 21 THEN 
        DATE_ADD(DATE_TRUNC(DATE(a.start_date), MONTH), INTERVAL 1 MONTH)
      ELSE 
        DATE_TRUNC(DATE(a.start_date), MONTH)
    END
  ) AS Periode,
  COUNT(a.NIK) AS CountCase
FROM {{ source('mp_infor', 'user_case') }} a
WHERE a.start_date IS NOT NULL
  AND a.GroupCaseType = 'Behavior'
  AND a.sp_type NOT IN ('Discussion Form','Surat Teguran Pertama (ST-I)','Surat Teguran Kedua (ST-II)','Surat Teguran Ketiga (ST-III)')
GROUP BY PeriodeDate, Periode,
a.nik,a.full_name
ORDER BY PeriodeDate;
