{{
  config(
    materialized= 'table'
  )
}}

select  
    'pivot' as source,
    a.Item ue_Item,
    a.Product_codes,
    a.description,
    a.watermark,
    a.paper_genus,
    a.paper_color,
    a.paper_width_cm,
    a.filter_paper_genus,
    a.filter_paper_width_cm,
    a.filter_paper_lenght_cm,
    a.filter_paper_color,
    a.logo,
    a.cone_type,
    a.filter_paper_length_mm,
    a.grammage_gsm,
    a.paper_length_mm,
    a.paper_size_bottom_mm,
    a.paper_size_top_mm,
    a.paper_size_length_mm,
    a.filter_shape,
    a.cones_per_sc,
    a.unit_bulk_per_sc,
    a.cones_per_pack_bulk,
    a.pack_per_unit
from {{ ref('item_attribute_pivot_v') }} a 
UNION ALL 
SELECT
   'item_attribut' as source,
    b.ue_Item,
    NULL as Product_codes,
    b.ue_ItemDesc,
    b.ue_Watermark,
    b.ue_PaperGenus,
    b.ue_PaperColor,
    NULL as paper_width_cm,
    NULL as filter_paper_genus,
    NULL as filter_paper_width_cm,
    b.ue_FilterPaperLength,
    b.ue_FilterPaperColor,
    b.ue_Logo,
    b.ue_ConeType,
    b.ue_FilterPaperLength,
    NULL as grammage_gsm,
    b.ue_PaperLength,
    NULL as paper_size_bottom_mm,
    NULL as paper_size_top_mm,
    NULL as paper_size_length_mm,
    b.ue_FilterShape,
    CAST(b.ue_ConesPerSC AS STRING)  as ue_ConesPerSC,
    CAST(b.ue_UnitOrBulkPerSC AS STRING) as ue_UnitOrBulkPerSC,
    CAST(b.ue_ConesPerPackOrBulk AS STRING) as ue_ConesPerPackOrBulk,
    CAST(b.ue_PackPerUnit AS STRING) as ue_PackPerUnit
from {{ source('mp_infor', 'item_atrribute') }} b