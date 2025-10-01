{{
  config(
    materialized= 'table'
  )
}}

SELECT
  a.*,
  uom.ConvFactor AS convertion,
  b.DeptDescription
FROM {{ ref('profitability_v') }} a 
  LEFT JOIN (SELECT 
              DISTINCT
              Item,
              FromUM,
              ToUM,
              ConvFactor
             FROM {{ source('mp_infor', 'uom_conversion') }}
             WHERE ToUM='PCS') uom ON a.Item = uom.item
                                  AND a.Um = uom.FromUM
  LEFT JOIN {{ source('mp_infor', 'WorkCenter') }} b ON a.WorkCenter = b.Wc
