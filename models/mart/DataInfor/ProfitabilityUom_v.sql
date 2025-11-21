{{
  config(
    materialized= 'table'
  )
}}

SELECT
  a.*,
  coalesce(uom.ConvFactor,uom.ConvFactor) AS convertion,
  b.DeptDescription,
   SumFinish.SumQty
FROM {{ ref('profitability_v') }} a 
  LEFT JOIN (select
                Distinct
                FORMAT_DATE('%Y-%m', date) as date,
                CopType,
                Item,
                SUM(ProdQty) AS SumQty,
              from {{ ref('profitability_v') }} 
              group by  
                FORMAT_DATE('%Y-%m', Date),
                CopType,
                Item
            ) SumFinish ON a.Item = SumFinish.Item
                      AND FORMAT_DATE('%Y-%m', a.date) = SumFinish.date
                      AND a.CopType = SumFinish.CopType  
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