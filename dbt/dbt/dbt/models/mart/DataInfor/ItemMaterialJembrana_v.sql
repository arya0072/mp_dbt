{{
  config(
    materialized= 'table'
  )
}}

SELECT
  item.Item AS ItemFG,
  item.Description AS ItemDescFG,
  item.UM AS umFG,
  mat.Item as ItemMatSFG,
  mat.DerItemDescription as ItemDescMatSFG,
  mat.UM AS umMatSFG,
  mat2.Item as ItemMat,
  mat2.DerItemDescription as ItemMatDesc,
  mat2.UM AS umItem
FROM {{ source('mp_infor', 'items_jembrana') }} item
  LEFT JOIN {{ source('mp_infor', 'current_material_jembrana') }} mat ON item.Job = mat.Job
                                                    AND item.Suffix = mat.Suffix
                                                    AND mat.Item LIKE '5%'
  LEFT JOIN {{ source('mp_infor', 'items_jembrana') }} item2 ON mat.Item = item2.Item
  LEFT JOIN {{ source('mp_infor', 'current_material_jembrana') }} mat2 ON item2.Job = mat2.Job
                                                    AND item2.Suffix = mat2.Suffix
                                                    AND mat2.Item LIKE '5%'
WHERE (item.Item LIKE '9003%' 
   OR item.Item LIKE '9005%' 
   OR item.Item LIKE '9006%'
   OR item.Item LIKE '9007%')
-- AND  item.Job IS NOT NULL
-- AND item.Item='900700002'
