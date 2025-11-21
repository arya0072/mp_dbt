{{
  config(
    materialized= 'table'
  )
}}

 SELECT 
    holiday.ponum,
    holiday.poorderdate,
    holiday.venadrname,
    holiday.itmproductcode,
    holiday.item,
    holiday.description,
    holiday.qtyorderedconv,
    holiday.derqtyreceivedconv,
    holiday.derqtyvoucherconv,
    holiday.po_line_status,
    holiday.rcvddate,
    holiday.recorddate,
    holiday.days_completion,
    holiday.jumlah_hari_libur,
    holiday.whse,
    count(holiday.holiday_date) AS total_tgl_merah
   FROM ( SELECT 
            a.ponum,
            a.poorderdate,
            a.venadrname,
            a.itmproductcode,
            a.item,
            a.description,
            a.qtyorderedconv,
            a.derqtyreceivedconv,
            a.derqtyvoucherconv,
            CASE
                WHEN a.stat = 'C' THEN 'Complete'
                WHEN a.stat = 'P' THEN 'Planned'
                WHEN a.stat = 'O' THEN 'Ordered'
                WHEN a.stat = 'F' THEN 'Filled'
                ELSE NULL
            END AS po_line_status,
            a.rcvddate,
            a.recorddate,
            DATE_DIFF(a.recorddate, a.rcvddate, DAY) AS days_completion,
            (
                SELECT COUNT(*) AS count
                FROM UNNEST(GENERATE_ARRAY(
                    DATE_DIFF(a.rcvddate, a.recorddate, DAY), 
                    0
                )) AS days
                WHERE EXTRACT(DAYOFWEEK FROM DATE_ADD(a.rcvddate, INTERVAL days DAY)) IN (1, 7)
            ) AS jumlah_hari_libur,
            NULL AS holiday_date,
            NULL AS holiday_name,
            a.whse
        FROM {{ source('mp_infor', 'po_detail') }} a
             ) holiday
  GROUP BY holiday.ponum, holiday.poorderdate, holiday.venadrname, holiday.itmproductcode, holiday.item, holiday.description, holiday.qtyorderedconv, holiday.derqtyreceivedconv, holiday.derqtyvoucherconv, holiday.po_line_status, holiday.rcvddate, holiday.recorddate, holiday.days_completion, holiday.jumlah_hari_libur,holiday.whse