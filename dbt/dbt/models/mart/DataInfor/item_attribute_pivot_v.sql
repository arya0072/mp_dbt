{{
  config(
    materialized= 'table'
  )
}}

SELECT
    b.Item,
    b.Product_codes,
    b.description,
    max(b.Watermark) as watermark,
    max(b.paper_genus) as paper_genus,
    max(b.paper_color) as paper_color,
    max(b.paper_width_cm) as paper_width_cm,
    max(b.filter_paper_genus) as filter_paper_genus,
    max(b.filter_paper_width_cm) as filter_paper_width_cm,
    max(b.filter_paper_lenght_cm) as filter_paper_lenght_cm,
    max(b.filter_paper_color) as filter_paper_color,
    max(b.logo) as logo,
    max(b.cone_type) as cone_type,
    max(b.filter_paper_length_mm) as filter_paper_length_mm,
    max(b.grammage_gsm) as grammage_gsm,
    max(b.paper_length_mm) as paper_length_mm,
    max(b.paper_size_bottom_mm) as paper_size_bottom_mm,
    max(b.paper_size_top_mm) as paper_size_top_mm,
    max(b.paper_size_length_mm) as paper_size_length_mm,
    max(b.filter_shape) as filter_shape,
    max(b.cones_per_sc) as cones_per_sc,
    max(b.unit_bulk_per_sc) as unit_bulk_per_sc,
    max(b.cones_per_pack_bulk) as cones_per_pack_bulk,
    max(b.pack_per_unit) as pack_per_unit
FROM
(select
    item.Item,
    item.ProductCode as Product_codes,
    item.description,
    CASE
        WHEN item_att.AttributeLabel = 'Watermark' THEN watermark.AttributeValue
        ELSE NULL
    END AS Watermark,
    CASE
        WHEN item_att.AttributeLabel = 'Paper Genus' THEN paper_genus.AttributeValue
        ELSE NULL
    END AS paper_genus,
    CASE
        WHEN item_att.AttributeLabel = 'Paper Color' THEN paper_color.AttributeValue
        ELSE NULL
    END AS paper_color,
    CASE
        WHEN item_att.AttributeLabel = 'Paper Width (cm)' THEN paper_widthcm.AttributeValue
        ELSE NULL
    END AS paper_width_cm,
    CASE
        WHEN item_att.AttributeLabel = 'Filter Paper Genus' THEN filer_papgenus.AttributeValue
        ELSE NULL
    END AS filter_paper_genus,
    CASE
        WHEN item_att.AttributeLabel = 'Filter Paper Width (cm)' THEN filer_papwidthcm.AttributeValue
        ELSE NULL
    END AS filter_paper_width_cm,
    CASE
        WHEN item_att.AttributeLabel = 'Filter Paper Length (cm)' THEN filer_paplengcm.AttributeValue
        ELSE NULL
    END AS filter_paper_lenght_cm,
    CASE
        WHEN item_att.AttributeLabel = 'Filter Paper Color' THEN filter_paper_color.AttributeValue
        ELSE NULL
    END AS filter_paper_color,
    CASE
        WHEN item_att.AttributeLabel = 'Logo' THEN logo.AttributeValue
        ELSE NULL
    END AS logo,
    CASE
        WHEN item_att.AttributeLabel = 'Cone Type' THEN cone_type.AttributeValue
        ELSE NULL
    END AS cone_type,
     CASE
        WHEN item_att.AttributeLabel = 'Filter Paper Length (mm)' THEN filter_paper_length_mm.AttributeValue
        ELSE NULL
    END AS filter_paper_length_mm,
    CASE
        WHEN item_att.AttributeLabel = 'Grammage (gsm)' THEN grammage_gsm.AttributeValue
        ELSE NULL
    END AS grammage_gsm,
    CASE
        WHEN item_att.AttributeLabel = 'Paper Length (mm)' THEN paper_length_mm.AttributeValue
        ELSE NULL
    END AS paper_length_mm,
    CASE
        WHEN item_att.AttributeLabel = 'Paper Size Bottom (mm)' THEN paper_size_bottom_mm.AttributeValue
        ELSE NULL
    END AS paper_size_bottom_mm,
     CASE
        WHEN item_att.AttributeLabel = 'Paper Size Top (mm)' THEN paper_size_top_mm.AttributeValue
        ELSE NULL
    END AS paper_size_top_mm,
     CASE
        WHEN item_att.AttributeLabel = 'Paper Size Length (mm)' THEN paper_size_length_mm.AttributeValue
        ELSE NULL
    END AS paper_size_length_mm,
     CASE
        WHEN item_att.AttributeLabel = 'Filter Shape' THEN filter_shape.AttributeValue
        ELSE NULL
    END AS filter_shape,
     CASE
        WHEN item_att.AttributeLabel = 'Cones per SC' THEN cones_per_sc.AttributeValue
        ELSE NULL
    END AS cones_per_sc,
     CASE
        WHEN item_att.AttributeLabel = 'Unit / Bulk per SC' THEN unit_bulk_per_sc.AttributeValue
        ELSE NULL
    END AS unit_bulk_per_sc,
     CASE
        WHEN item_att.AttributeLabel = 'Cones per Pack / Bulk' THEN cones_per_pack_bulk.AttributeValue
        ELSE NULL
    END AS cones_per_pack_bulk,
    CASE
        WHEN item_att.AttributeLabel = 'Pack per Unit' THEN pack_per_unit.AttributeValue
        ELSE NULL
    END AS pack_per_unit
FROM {{ source('mp_infor', 'items') }} item
  JOIN {{ source('mp_infor', 'item_attribute_all') }} item_att ON item_att.RefRowPointer = item.RowPointer
  LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Watermark') watermark ON watermark.RefRowPointer = item.RowPointer
  LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Paper Genus') paper_genus ON paper_genus.RefRowPointer = item.RowPointer
  LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Paper Color') paper_color ON paper_color.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Paper Width (cm)') paper_widthcm ON paper_widthcm.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Filter Paper Genus') filer_papgenus ON filer_papgenus.RefRowPointer = item.RowPointer
     LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Filter Paper Width (cm)') filer_papwidthcm ON filer_papwidthcm.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Filter Paper Length (cm)') filer_paplengcm ON filer_paplengcm.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Filter Paper Color') filter_paper_color ON filter_paper_color.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Logo') logo ON logo.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Cone Type') cone_type ON cone_type.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Filter Paper Length (mm)') filter_paper_length_mm ON filter_paper_length_mm.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Grammage (gsm)') grammage_gsm ON grammage_gsm.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Paper Length (mm)') paper_length_mm ON paper_length_mm.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Paper Size Bottom (mm)') paper_size_bottom_mm ON paper_size_bottom_mm.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Paper Size Top (mm)') paper_size_top_mm ON paper_size_top_mm.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Paper Size Length (mm)') paper_size_length_mm ON paper_size_length_mm.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Filter Shape') filter_shape ON filter_shape.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Cones per SC') cones_per_sc ON cones_per_sc.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Unit / Bulk per SC') unit_bulk_per_sc ON unit_bulk_per_sc.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Cones per Pack / Bulk') cones_per_pack_bulk ON cones_per_pack_bulk.RefRowPointer = item.RowPointer
    LEFT JOIN (select
                a.AttributeValue,
                a.RefRowPointer
            from {{ source('mp_infor', 'item_attribute_all') }} a
            where a.AttributeLabel='Pack per Unit') pack_per_unit ON pack_per_unit.RefRowPointer = item.RowPointer
WHERE item.ProductCode LIKE '%SCN%' OR item.ProductCode LIKE '%SFT-TIP%'
) b
group by b.description, b.Product_codes, b.Item