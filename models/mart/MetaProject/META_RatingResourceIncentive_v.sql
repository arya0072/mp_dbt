{{
  config(
    materialized= 'table'
  )
}}

WITH Summary AS (
  SELECT
    a.Location,
    a.EmployeeName,
    a.Resource,
    a.is_incentive,
    SUM(
      CASE
        WHEN a.GroupProductivity = 'GROUP 1' THEN 86000
        WHEN a.GroupProductivity = 'GROUP 2' THEN 117000
        WHEN a.GroupProductivity = 'GROUP 3' THEN 159000
        WHEN a.GroupProductivity = 'GROUP 4' THEN 256000
        WHEN a.GroupProductivity = 'GROUP 5' THEN 392000
        WHEN a.GroupProductivity = 'GROUP 6' THEN 640000
        WHEN a.GroupProductivity = 'GROUP 7' THEN 921000
        WHEN GroupProductivity = 'GROUP HRF' THEN 450000
        ELSE 0
      END
    ) AS GroupValueProductivity,
    SUM(
      CASE
        WHEN a.GroupProductivityAdjust = 'GROUP 1' THEN 86000
        ELSE 0
      END
    ) AS GroupValueByMatrix,
    SUM(
      CASE
        WHEN a.GroupProductivity = 'GROUP 1' THEN 86000
        WHEN a.GroupProductivity = 'GROUP 2' THEN 117000
        WHEN a.GroupProductivity = 'GROUP 3' THEN 159000
        WHEN a.GroupProductivity = 'GROUP 4' THEN 256000
        WHEN a.GroupProductivity = 'GROUP 5' THEN 392000
        WHEN a.GroupProductivity = 'GROUP 6' THEN 640000
        WHEN a.GroupProductivity = 'GROUP 7' THEN 921000
        WHEN GroupProductivity = 'GROUP HRF' THEN 450000
        ELSE 0
      END
    ) +
    SUM(
      CASE
        WHEN a.GroupProductivityAdjust = 'GROUP 1' THEN 86000
        ELSE 0
      END
    ) AS TotalProductivity
  FROM {{ ref('META_Incentive_v') }} a
  GROUP BY a.Resource, a.EmployeeName, a.Location,a.is_incentive
)

SELECT 
  Location,
  EmployeeName,
  Resource, 
  GroupValueProductivity, 
  GroupValueByMatrix, 
  TotalProductivity,
  is_incentive,
  ROW_NUMBER() OVER (PARTITION BY Location ORDER BY TotalProductivity DESC) AS Rating
FROM Summary
WHERE Location = 'BB-Lt2' AND is_incentive = 'YES'
ORDER BY Location,Rating