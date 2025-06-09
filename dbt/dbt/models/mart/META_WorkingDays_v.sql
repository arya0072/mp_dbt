{{
  config(
    materialized= 'table'
  )
}}

WITH WorkingDays AS (
  SELECT 
    FORMAT_DATE('%Y-%m-%d', DATE(a.date)) AS Date,
    FORMAT_DATE(
      '%Y-%m', 
      CASE 
        WHEN EXTRACT(DAY FROM a.date) >= 21 THEN 
          DATE_ADD(DATE_TRUNC(DATE(a.date), MONTH), INTERVAL 1 MONTH)
        ELSE 
          DATE_TRUNC(DATE(a.date), MONTH)
      END
    ) AS PeriodeDate,
    CURRENT_DATE AS CurrentDate,
    FORMAT_DATE(
      '%Y-%m', 
      CASE 
        WHEN EXTRACT(DAY FROM CURRENT_DATE) >= 21 THEN 
          DATE_ADD(DATE_TRUNC(DATE(CURRENT_DATE), MONTH), INTERVAL 1 MONTH)
        ELSE 
          DATE_TRUNC(DATE(CURRENT_DATE), MONTH)
      END
    ) AS PeriodeCurrentDate
  FROM `mp_infor.WorkingDays` a
  WHERE a.DayTypeId IN (1,4)
)

SELECT 
  PeriodeDate AS Periode,
  CASE 
    WHEN PeriodeDate < FORMAT_DATE('%Y-%m', CURRENT_DATE)  THEN COUNT(*)  -- Periode lalu: Hitung semua hari dalam periode itu
    WHEN PeriodeDate = PeriodeCurrentDate  THEN COUNTIF(Date <= FORMAT_DATE('%Y-%m-%d', CURRENT_DATE)) -- Periode sekarang: Hitung sampai hari ini
    ELSE COUNT(*)  -- Periode mendatang: Hitung semua hari dalam periode itu
  END AS TotalDays,
  COUNT(*) AS TotalDaysMonth
FROM WorkingDays
GROUP BY PeriodeDate,PeriodeCurrentDate
ORDER BY PeriodeDate ASC;