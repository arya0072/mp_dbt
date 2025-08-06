{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  a.Year,
  'January' as month,
  a.January as actual,
  a.JanuaryBudget as budget,
  a.JanuaryVariance as variance,
  1 AS month_num
FROM {{ source('mp_infor', 'budget_actual') }}  a where Account like '6%'
UNION ALL
SELECT 
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  a.Year,
  'February' as month,
  a.February,
  a.FebruaryBudget,
  a.FebruaryVariance,
  2 AS month_num
FROM {{ source('mp_infor', 'budget_actual') }}  a where Account like '6%'
UNION ALL
SELECT 
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  a.Year,
  'March' as month,
  a.March,
  a.MarchBudget,
  a.MarchVariance,
  3 AS month_num
FROM {{ source('mp_infor', 'budget_actual') }}  a where Account like '6%'
UNION ALL
SELECT 
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  a.Year,
  'April' as month,
  a.April,
  a.AprilBudget,
  a.AprilVariance,
  4 AS month_num
FROM {{ source('mp_infor', 'budget_actual') }}  a where Account like '6%'
UNION ALL
SELECT 
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  a.Year,
  'May' as month,
  a.May,
  a.MayBudget,
  a.MayVariance,
  5 AS month_num
FROM {{ source('mp_infor', 'budget_actual') }}  a where Account like '6%'
UNION ALL
SELECT 
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  a.Year,
  'June' as month,
  a.June,
  a.JuneBudget,
  a.JuneVariance,
  6 AS month_num
FROM {{ source('mp_infor', 'budget_actual') }}  a where Account like '6%'
UNION ALL
SELECT 
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  a.Year,
  'July' as month,
  a.July,
  a.JulyBudget,
  a.JulyVariance,
  7 AS month_num
FROM {{ source('mp_infor', 'budget_actual') }}  a where Account like '6%'
UNION ALL
SELECT 
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  a.Year,
  'August' as month,
  a.August,
  a.AugustBudget,
  a.AugustVariance,
  8 AS month_num
FROM {{ source('mp_infor', 'budget_actual') }}  a where Account like '6%'
UNION ALL
SELECT 
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  a.Year,
  'September' as month,
  a.September,
  a.SeptemberBudget,
  a.SeptemberVariance,
  9 AS month_num
FROM {{ source('mp_infor', 'budget_actual') }}  a where Account like '6%'
UNION ALL
SELECT 
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  a.Year,
  'October' as month,
  a.October,
  a.OctoberBudget,
  a.OctoberVariance,
  10 AS month_num
FROM {{ source('mp_infor', 'budget_actual') }}  a where Account like '6%'
UNION ALL
SELECT 
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  a.Year,
  'November' as month,
  a.November,
  a.NovemberBudget,
  a.NovemberVariance,
  11 AS month_num
FROM {{ source('mp_infor', 'budget_actual') }}  a where Account like '6%'
UNION ALL
SELECT 
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  a.Year,
  'December' as month,
  a.December,
  a.DecemberBudget,
  a.DecemberVariance,
  12 AS month_num
FROM {{ source('mp_infor', 'budget_actual') }}  a where Account like '6%'