{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  a.ue_ci_desc,
  a.ue_co_line,
  a.ue_co_num,
  a.ue_CoiOriPrice,
  a.ue_CoiQtyShipped,
  a.ue_CoiUM,
  a.ue_ConeGroup,
  a.ue_ConvUM,
  a.ue_curr_code,
  a.ue_cust_item,
  a.ue_cust_name,
  a.ue_cust_num,
  a.ue_cust_po,
  a.ue_delterm,
  a.ue_DiscAmount,
  a.ue_DueDate,
  a.ue_FIDueDate,
  a.ue_ihRate,
  a.ue_infordesc,
  a.ue_inv_date,
  a.ue_inv_num,
  a.ue_InvNetPrice,
  a.ue_item,
  a.ue_ItemUM,
  a.ue_order_date,
  a.ue_pickup_date,
  a.ue_ProductCode,
  a.ue_QtyInvoiced,
  a.ue_QtyShippedPcs,
  a.ue_SFGConesDesc,
  a.ue_ship_via,
  a.ue_shipment_id,
  a.ue_shipviadesc,
  a.ue_terms_code,
  a.ue_TotalValueQtyShipped,
  a.ue_ProductCodeDesc,
  item_att.ue_PaperLength,
  item_att.ue_FilterPaperLength,
  item_att.ue_ConeType,
  item_att.ue_FilterShape,
  item_att.ue_PaperGenus,
  item_att.ue_PaperColor,
  item_att.ue_Watermark,
  item_att.ue_FilterPaperColor, 
  item_att.ue_Logo,
  a.ue_SalesTeam,
  item_att.ue_PrintTipCat,
  item_att_all.AttributeLabel,
  item_att_all.Type,
  item_cust.itmcstUf_MP04_ConeCategory AS cone_category
FROM {{ source('mp_infor', 'shipment_or_fi_detail') }} a
  LEFT JOIN {{ source('mp_infor', 'item_atrribute') }} item_att ON a.ue_item = item_att.ue_Item
  LEFT JOIN {{ source('mp_infor', 'itemsCustomer') }} item_cust ON a.ue_item = item_cust.Item AND a.ue_cust_num = item_cust.CustNum
  LEFT JOIN (SELECT 
              item.Item, 
              a.*
            FROM {{ source('mp_infor', 'item_attribute_all') }} a
              LEFT JOIN {{ source('mp_infor', 'items') }} item ON item.RowPointer = a.RefRowPointer
            WHERE LEFT(item.Item, 4) IN ('9001','9004')
              AND a.AttributeLabel='Type') item_att_all ON a.ue_item = item_att_all.Item