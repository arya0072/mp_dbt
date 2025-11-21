{{
  config(
    materialized= 'table'
  )
}}

SELECT 
    a.reqnum AS pr_num,
    a.reqdate AS pr_date,
    a.Stat AS stat_approve_header,
    a.preqUf_MP74_ApproveDate2 AS approve_date_prheader,
    pr_detail.ponum AS po_num,
    po_detail.poorderdate AS po_date,
    po_detail.item,
    po_detail.description,
    po_detail.venadrname,
    CASE
      WHEN po_detail.Stat = 'C' THEN 'Complete'
      WHEN po_detail.Stat = 'P' THEN 'Planned'
      WHEN po_detail.Stat = 'O' THEN 'Ordered'
      WHEN po_detail.Stat = 'F' THEN 'Filled'
      ELSE NULL
    END AS po_status,
    DATE_DIFF(po_header.pouf_mp74_approvedate2, a.preqUf_MP74_ApproveDate2, DAY) AS days_completoin_pr_po,
     po_header.pouf_mp74_approvedate2 AS approve_date_poheader,
     NULL AS jumlah_hari_libur
FROM {{ source('mp_infor', 'pr_header') }} a
     JOIN ( SELECT 
              DISTINCT b.reqnum,
              b.ponum
           FROM {{ source('mp_infor', 'pr_detail') }} b) pr_detail ON pr_detail.reqnum = a.reqnum
     LEFT JOIN {{ source('mp_infor', 'po_header') }} po_header ON po_header.ponum = pr_detail.ponum
     LEFT JOIN {{ source('mp_infor', 'po_detail') }} po_detail ON po_detail.ponum = po_header.ponum
