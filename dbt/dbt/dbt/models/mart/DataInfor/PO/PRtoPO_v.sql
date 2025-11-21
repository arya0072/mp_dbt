{{
  config(
    materialized= 'table'
  )
}}

SELECT  
    pr_header.ReqNum AS pr_num,
    pr_header.ReqDate AS pr_date,
    pr_header.preqUf_MP74_StatusApproval AS stat_approve_header,
    pr_header.preqUf_MP74_ApproveDate2 AS approve_date_prheader,
    pr_detail.PoNum AS po_num,
    po_detail.PoOrderDate AS po_date,
    po_detail.Item,
    po_detail.Description,
    po_detail.VenadrName,
    CASE
        WHEN po_detail.Stat = 'C' THEN 'Complete'
        WHEN po_detail.Stat = 'P' THEN 'Planned'
        WHEN po_detail.Stat = 'O' THEN 'Ordered'
        WHEN po_detail.Stat = 'F' THEN 'Filled'
        ELSE NULL
    END AS po_status,
    DATE_DIFF(CAST(po_header.poUf_MP74_ApproveDate2 AS DATE), CAST(pr_header.preqUf_MP74_ApproveDate2 AS DATE), DAY) AS days_completion_pr_po,
    po_header.poUf_MP74_ApproveDate2 AS approve_date_poheader,
    (
      SELECT COUNT(*)
      FROM UNNEST(GENERATE_DATE_ARRAY(CAST(pr_header.preqUf_MP74_ApproveDate2 AS DATE), CAST(po_header.poUf_MP74_ApproveDate2 AS DATE), INTERVAL 1 DAY)) AS dates
      WHERE EXTRACT(DAYOFWEEK FROM dates) IN (1, 7) -- 1 = Minggu, 7 = Sabtu
    ) AS jumlah_hari_libur
FROM {{ source('mp_infor', 'pr_header') }} pr_header
  JOIN (SELECT DISTINCT ReqNum, PoNum FROM {{ source('mp_infor', 'pr_detail') }}) pr_detail ON pr_detail.ReqNum = pr_header.ReqNum
  LEFT JOIN {{ source('mp_infor', 'po_header') }} po_header ON po_header.PoNum = pr_detail.PoNum
  LEFT JOIN {{ source('mp_infor', 'po_detail') }} po_detail ON po_header.PoNum = po_detail.PoNum

