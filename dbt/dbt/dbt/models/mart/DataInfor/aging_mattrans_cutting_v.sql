{{
  config(
    materialized= 'table'
  )
}}

select 
  DISTINCT
  a.Item, 
  a.ItemDescription, 
  a.TransDate,
  a.RefType,
  a.Whse,
  a.Loc
from {{ source('mp_infor', 'material_transaction') }} a 
  JOIN (select 
          aging.Item, 
          aging.RefType,
          aging.Whse,
          aging.Loc,
          max(aging.TransDate) as max_transdate
        from {{ source('mp_infor', 'material_transaction') }} aging
        where aging.RefType  IN ('P','J','G') 
          AND aging.Loc IN ('BB-Cut')
        group by
          aging.Item, 
          aging.RefType,
          aging.Whse,
          aging.Loc
         ) max_ag ON a.Item = max_ag.Item 
                 AND a.RefType = max_ag.RefType 
                 AND a.Whse = max_ag.Whse
                 AND a.TransDate = max_ag.max_transdate
                 AND a.Loc = max_ag.Loc
where a.Whse  IN ('MP') 