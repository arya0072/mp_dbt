{{
  config(
    materialized= 'table'
  )
}}

select * from
(SELECT 
  a.Year,
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  '01-January' as periode,
  a.January as actual,
  a.JanuaryBudget as budget,
  a.JanuaryVariance as variance
FROM {{ source('mp_infor', 'budget_actual') }}  a
UNION ALL
SELECT 
  a.Year,
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  '02-February' as periode,
  a.February as actual,
  a.FebruaryBudget as budget,
  a.FebruaryVariance as variance
FROM {{ source('mp_infor', 'budget_actual') }}  a
UNION ALL
SELECT 
  a.Year,
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  '03-March' as periode,
  a.March as actual,
  a.MarchBudget as budget,
  a.MarchVariance as variance
FROM {{ source('mp_infor', 'budget_actual') }}  a
UNION ALL
SELECT 
  a.Year,
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  '04-April' as periode,
  a.April as actual,
  a.AprilBudget as budget,
  a.AprilVariance as variance
FROM {{ source('mp_infor', 'budget_actual') }}  a
UNION ALL
SELECT 
  a.Year,
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  '05-May' as periode,
  a.May as actual,
  a.MayBudget as budget,
  a.MayVariance as variance
FROM {{ source('mp_infor', 'budget_actual') }}  a
UNION ALL
SELECT 
  a.Year,
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  '06-June' as periode,
  a.June as actual,
  a.JuneBudget as budget,
  a.JuneVariance as variance
FROM {{ source('mp_infor', 'budget_actual') }}  a
UNION ALL
SELECT 
  a.Year,
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  '07-July' as periode,
  a.July as actual,
  a.JulyBudget as budget,
  a.JulyVariance as variance
FROM {{ source('mp_infor', 'budget_actual') }}  a
UNION ALL
SELECT 
  a.Year,
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  '08-August' as periode,
  a.August as actual,
  a.AugustBudget as budget,
  a.AugustVariance as variance
FROM {{ source('mp_infor', 'budget_actual') }}  a
UNION ALL
SELECT 
  a.Year,
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  '09-September' as periode,
  a.September as actual,
  a.SeptemberBudget as budget,
  a.SeptemberVariance as variance
FROM {{ source('mp_infor', 'budget_actual') }}  a
UNION ALL
SELECT 
  a.Year,
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  '10-October' as periode,
  a.October as actual,
  a.OctoberBudget as budget,
  a.OctoberVariance as variance
FROM {{ source('mp_infor', 'budget_actual') }}  a
UNION ALL
SELECT 
  a.Year,
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  '11-November' as periode,
  a.November as actual,
  a.NovemberBudget as budget,
  a.NovemberVariance as variance
FROM {{ source('mp_infor', 'budget_actual') }}  a
UNION ALL
SELECT 
  a.Year,
  a.Account,
  a.Description,
  a.UnitCode1,
  a.UnitCode2,
  '12-December' as periode,
  a.December as actual,
  a.DecemberBudget as budget,
  a.DecemberVariance as variance
FROM {{ source('mp_infor', 'budget_actual') }}  a)
coa6 where coa6.Account like '6%'