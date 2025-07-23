{{
  config(
    materialized= 'table'
  )
}}

SELECT
  CASE 
  When a.Month = 'JAN' Then '01.JAN'
  When a.Month = 'FEB' Then '02.FEB'
  When a.Month = 'MAR' Then '03.MAR'
  When a.Month = 'APR' Then '04.APR'
  When a.Month = 'MAY' Then '05.MAY'
  When a.Month = 'JUN' Then '06.JUN'
  When a.Month = 'JUL' Then '07.JUL'
  When a.Month = 'AUG' Then '08.AUG'
  When a.Month = 'SEP' Then '09.SEP'
  When a.Month = 'OCT' Then '10.OCT'
  When a.Month = 'NOV' Then '11.NOV'
  When a.Month = 'DEC' Then '12.DEC'
  Else 'Notidentify'
  END AS Month,
  a.Year,
  a.ue_AcctDesc,
  a.ue_Item,
  a.ue_ItemDesc,
  SUM(a.ue_matl_QtyEnding + a.ue_Lbr_QtyEnding + a.ue_Matl_Beginning + a.ue_Lbr_Beginning + a.ue_Ovh_QtyEnding + a.ue_Ovh_Beginning + a.ue_Vovhd_QtyEnding + a.ue_Vovhd_Beginning) / 2 AS Average_inventory,
  SUM(a.ue_Lbr_QtyEnding + a.ue_Lbr_Beginning) / 2 AS Average_inv_labor,
  SUM(a.ue_matl_QtyEnding + a.ue_Matl_Beginning) / 2 AS Average_inv_material,
  sum(a.ue_Ovh_QtyEnding + a.ue_Ovh_Beginning) / 2 AS Average_inv_FOvh,
  Sum (a.ue_Vovhd_QtyEnding + a.ue_Vovhd_Beginning) /2 AS Average_inv_VOvh,
  SUM(COALESCE(-a.ue_Matl_ShipCO,0) + COALESCE(-a.ue_Matl_RMA,0)+COALESCE(-a.ue_Lbr_ShipCO,0) + COALESCE(-a.ue_Lbr_RMA,0)+COALESCE(-a.ue_Ovh_ShipCO,0) + COALESCE (-a.ue_Ovh_RMA,0)+COALESCE(-a.ue_Vovhd_ShipCO,0) + COALESCE (-a.ue_Vovhd_RMA,0)) AS Total_COGS,
  SUM(COALESCE(-a.ue_Matl_ShipCO,0) + COALESCE(-a.ue_Matl_RMA,0)) AS COGS_Material,
  SUM(COALESCE(-a.ue_Lbr_ShipCO,0) + COALESCE(-a.ue_Lbr_RMA,0)) As COGS_Labor,
  Sum(COALESCE(-a.ue_Ovh_ShipCO,0) + COALESCE (-a.ue_Ovh_RMA,0)) AS COGS_FOvh,
  Sum(COALESCE(-a.ue_Vovhd_ShipCO,0) + COALESCE (-a.ue_Vovhd_RMA,0)) AS COGS_VOvh
FROM {{ ref('movement_value_new_v') }} a
-- Where a.ue_Item = '900501167'
GROUP BY
  a.Month,
  a.Year,
  a.ue_AcctDesc,
  a.ue_Item,
  a.ue_ItemDesc