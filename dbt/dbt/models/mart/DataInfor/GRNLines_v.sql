{{
  config(
    materialized= 'table'
  )
}}

 SELECT 
    a.container,
    a.deritemdescription,
    a.deritemexists,
    a.derpoexists,
    a.derqtyrec,
    a.derqtyrej,
    a.derqtyreturned,
    a.derqtyvouchered,
    a.deruomconversionfactor,
    a.grncontainer,
    a.grnediasn,
    a.grnhdrdate,
    a.grnline,
    a.grnnum,
    a.grnshippeddate,
    a.grnstat,
    a.inworkflow,
    a.itemdescription,
    a.itmitem,
    a.noteexistsflag,
    a.poidescription,
    a.poitem,
    a.poitemum,
    a.poline,
    a.ponum,
    a.porelease,
    a.potype,
    a.qtyrec,
    a.qtyrej,
    a.qtyreturned,
    a.qtyshippedconv,
    a.qtyvouchered,
    a.recorddate,
    a.rowpointer,
    a.showindropdownlist,
    a.um,
    a.vendname,
    a.vendnum,
    a.whse,
    a.whsename,
    a._itemid,
    CASE
        WHEN a.DerQtyRej > 0 THEN 'REJECT'
        ELSE 'PASS'
    END AS status_reject_pass
   FROM {{ source('mp_infor', 'GRNLines') }} a