{{
  config(
    materialized= 'table'
  )
}} 

SELECT 
  mval.Site,
  mval.ue_Acct,
  mval.ue_AcctDesc,
  mval.ue_CustName,
  mval.ue_Customer,
  mval.ue_Item,
  mval.ue_ItemDesc,
  mval.Year,
  mval.Month,
  CONCAT(CAST(mval.year AS STRING), '-',FORMAT_DATE('%m', PARSE_DATE('%b', mval.month))) AS year_month,
  mloc.ue_Loc,
  mloc.ue_LocDesc,
  mloc.ue_QtyEnding,
  mloc.ue_QtyEnding_Pcs,
  (mloc.ue_QtyEnding*CostMonth.MatCost) AS MatCost,
  (mloc.ue_QtyEnding*CostMonth.LaborCost) AS LaborCost,
  (mloc.ue_QtyEnding*CostMonth.VarCost) AS VarCost,
  (mloc.ue_QtyEnding*CostMonth.FixCost) AS FixCost,
  (mloc.ue_QtyEnding*CostMonth.TotalCost) AS TotalCost
FROM {{ source('mp_infor', 'movement_value_mp_new') }} mval
  LEFT JOIN {{ source('mp_infor', 'movement_location_mp') }} mloc ON mval.ue_Item = mloc.ue_Item
                                                 AND CONCAT(CAST(mval.year AS STRING), '-',FORMAT_DATE('%m', PARSE_DATE('%b', mval.month))) = mloc.month
  LEFT JOIN {{ ref('CostItemMonth_v') }} CostMonth ON mval.ue_Item = CostMonth.ue_Item
                                                 AND mval.Site = CostMonth.Site
                                                 AND CONCAT(CAST(mval.year AS STRING), '-',FORMAT_DATE('%m', PARSE_DATE('%b', mval.month))) = CostMonth.YearMonth
UNION ALL
SELECT 
  mval.Site,
  mval.ue_Acct,
  mval.ue_AcctDesc,
  mval.ue_CustName,
  mval.ue_Customer,
  mval.ue_Item,
  mval.ue_ItemDesc,
  mval.Year,
  mval.Month,
  CONCAT(CAST(mval.year AS STRING), '-',FORMAT_DATE('%m', PARSE_DATE('%b', mval.month))) AS year_month,
  mloc.ue_Loc,
  mloc.ue_LocDesc,
  mloc.ue_QtyEnding,
  mloc.ue_QtyEnding_Pcs,
  (mloc.ue_QtyEnding*CostMonth.MatCost) AS MatCost,
  (mloc.ue_QtyEnding*CostMonth.LaborCost) AS LaborCost,
  (mloc.ue_QtyEnding*CostMonth.VarCost) AS VarCost,
  (mloc.ue_QtyEnding*CostMonth.FixCost) AS FixCost,
  (mloc.ue_QtyEnding*CostMonth.TotalCost) AS TotalCost
FROM {{ source('mp_infor', 'movement_value_mpkb_new') }} mval
  LEFT JOIN {{ source('mp_infor', 'movement_location_mpkb') }} mloc ON mval.ue_Item = mloc.ue_Item
                                                 AND CONCAT(CAST(mval.year AS STRING), '-',FORMAT_DATE('%m', PARSE_DATE('%b', mval.month))) = mloc.month
  LEFT JOIN {{ ref('CostItemMonth_mpkb_v') }} CostMonth ON mval.ue_Item = CostMonth.ue_Item
                                                 AND mval.Site = CostMonth.Site
                                                 AND CONCAT(CAST(mval.year AS STRING), '-',FORMAT_DATE('%m', PARSE_DATE('%b', mval.month))) = CostMonth.YearMonth
UNION ALL
SELECT 
  mval.Site,
  mval.ue_Acct,
  mval.ue_AcctDesc,
  mval.ue_CustName,
  mval.ue_Customer,
  mval.ue_Item,
  mval.ue_ItemDesc,
  mval.Year,
  mval.Month,
  CONCAT(CAST(mval.year AS STRING), '-',FORMAT_DATE('%m', PARSE_DATE('%b', mval.month))) AS year_month,
  mloc.ue_Loc,
  mloc.ue_LocDesc,
  mloc.ue_QtyEnding,
  mloc.ue_QtyEnding_Pcs,
  (mloc.ue_QtyEnding*CostMonth.MatCost) AS MatCost,
  (mloc.ue_QtyEnding*CostMonth.LaborCost) AS LaborCost,
  (mloc.ue_QtyEnding*CostMonth.VarCost) AS VarCost,
  (mloc.ue_QtyEnding*CostMonth.FixCost) AS FixCost,
  (mloc.ue_QtyEnding*CostMonth.TotalCost) AS TotalCost
FROM {{ source('mp_infor', 'movement_value_jmbr_new') }} mval
  LEFT JOIN {{ source('mp_infor', 'movement_location_jmbr') }} mloc ON mval.ue_Item = mloc.ue_Item
                                                 AND CONCAT(CAST(mval.year AS STRING), '-',FORMAT_DATE('%m', PARSE_DATE('%b', mval.month))) = mloc.month
  LEFT JOIN {{ ref('CostItemMonth_jmbr_v') }} CostMonth ON mval.ue_Item = CostMonth.ue_Item
                                                 AND mval.Site = CostMonth.Site
                                                 AND CONCAT(CAST(mval.year AS STRING), '-',FORMAT_DATE('%m', PARSE_DATE('%b', mval.month))) = CostMonth.YearMonth