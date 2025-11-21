{{
  config(
    materialized= 'table'
  )
}} 

SELECT 
    a.ue_Item,
    a.Month,
    a.Year,
    a.Site,
    b.Month AS YearMonth,
    a.ue_matl_QtyEnding,
    a.ue_Lbr_QtyEnding,
    a.ue_Vovhd_QtyEnding,
    a.ue_Ovh_QtyEnding, 
    SUM(a.ue_matl_QtyEnding + a.ue_Lbr_QtyEnding + a.ue_Vovhd_QtyEnding + a.ue_Ovh_QtyEnding) as TotalValueEnding,
    b.QtyEnding,
    (a.ue_matl_QtyEnding / NULLIF(b.QtyEnding, 0)) as MatCost,
    (a.ue_Lbr_QtyEnding / NULLIF(b.QtyEnding, 0)) AS LaborCost,
    (a.ue_Vovhd_QtyEnding / NULLIF(b.QtyEnding, 0)) AS VarCost,
    (a.ue_Ovh_QtyEnding / NULLIF(b.QtyEnding, 0)) AS FixCost,
    SUM(a.ue_matl_QtyEnding + a.ue_Lbr_QtyEnding + a.ue_Vovhd_QtyEnding + a.ue_Ovh_QtyEnding) / NULLIF(b.QtyEnding, 0) AS TotalCost
FROM {{ source('mp_infor', 'movement_value_mp_new') }} a
    LEFT JOIN (SELECT 
                ue_Item,
                Month,
                SUM(ue_QtyEnding) as QtyEnding
               FROM {{ source('mp_infor', 'movement_location_mp') }} 
               GROUP BY 
                ue_Item,
                Month) b ON a.ue_Item = b.ue_Item
                         AND CONCAT(CAST(a.year AS STRING), '-',FORMAT_DATE('%m', PARSE_DATE('%b', a.month))) = b.Month
-- where a.ue_Item='500700827'
GROUP BY a.ue_Item,
         a.Month,
         a.Year,
         a.Site,
         a.ue_matl_QtyEnding,
         a.ue_Lbr_QtyEnding,
         a.ue_Vovhd_QtyEnding,
         a.ue_Ovh_QtyEnding,
         b.QtyEnding,
         YearMonth
