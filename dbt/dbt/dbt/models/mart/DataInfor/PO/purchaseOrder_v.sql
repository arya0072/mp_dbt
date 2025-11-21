{{
  config(
    materialized= 'table'
  )
}}

SELECT 
    a.ponum,
    a.povendnum,
    a.venadrname,
    a.postat,
    a.poorderdate,
    a.poline,
    a.item,
    a.stat,
    a.description,
    a.venditem,
    a.itemcostconv,
    a.qtyreceived,
    a.unitmatcostconv,
    a.derduedate,
    a.derpromisedate,
    a.qtyorderedconv,
    a.um,
    a.itemcost,
    a.derextitemcostconv,
    a.whse,
    a.reftype,
    a.plancostconv,
    a.derqtyreceivedconv,
    a.derqtyrejectedconv,
    a.derqtyvoucherconv,
    a.dertotalrcvdcost,
    a.rcvddate,
    a.pocurrcode,
    a.currcodedesc,
    a.noninvacct,
    a.noninvacctunit1,
    a.noninvacctunit2,
    a.shipaddr,
    a.exportvalue,
    a.recorddate,
    a.promisedate,
    a.itemtype,
    a.qtyrejected,
    a.qtyreturned,
    a.qtyvoucher,
    a.duedate,
    a.itmproductcode,
    a._itemid,
    b.prodcodeUf_MP10_ItemPrefix AS product_prefix,
    b.Description AS product_code_desc,
    SPLIT(b.Description, '-')[SAFE_OFFSET(0)] AS prod_prefix,
    a.pobuyer,
    items.DerCurUCost AS last_price
FROM {{ source('mp_infor', 'po_detail') }} a
LEFT JOIN {{ source('mp_infor', 'items') }} items ON a.Item = items.Item
LEFT JOIN {{ source('mp_infor', 'product_codes_BQ') }} b ON a.itmproductcode = b.ProductCode
