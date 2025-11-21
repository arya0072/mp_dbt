{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  a.InvDate,
  a.InvNum,
  a.ShipDate,
  a.CustNum,
  a.CustName,
  a.Item,
  a.ItemDesc,
  a.Um,
  a.ConvFactor,
  a.QtyInvoiced,
  a.QtyPcs,
  a.CurrCode,
  a.CoiPrice,
  a.DiscAmt,
  a.ExtendedPrice,
  a.ExtendedNetPrice,
  a.ExchRate,
  a.DomesticExtendedPrice,
  a.CgsMatlTotal,
  a.CgsLbrTotal,
  a.CgsFovhdTotal,
  a.CgsVovhdTotal,
  a.CgsOutTotal,
  a.CgsTotal,
  a.DomesticExtendedCogs,
  a.cust_item,
  item_att.ue_PaperLength,
  item_att.ue_FilterPaperLength,
  item_att.ue_ConeType,
  item_att.ue_FilterShape,
  item_att.ue_PaperGenus,
  item_att.ue_PaperColor,
  item_att.ue_Watermark,
  item_att.ue_FilterPaperColor, 
  item_att.ue_Logo,
  item_att.ue_PrintTipCat,
  item_att_all.AttributeLabel,
  item_att_all.Type,
  item_cust.itmcstUf_MP04_ConeCategory AS cone_category,
  a.overview,
  a.ProductcodeDescription,
  a.ConvUn
FROM  {{ source('mp_infor', 'salestransaction') }} a
  LEFT JOIN {{ source('mp_infor', 'item_atrribute') }} item_att ON a.Item = item_att.ue_item
  LEFT JOIN {{ source('mp_infor', 'itemsCustomer') }} item_cust ON a.Item = item_cust.Item and a.CustNum = item_cust.CustNum
  LEFT JOIN (SELECT 
              item.Item, 
              a.*
            FROM {{ source('mp_infor', 'item_attribute_all') }} a
              LEFT JOIN {{ source('mp_infor', 'items') }} item ON item.RowPointer = a.RefRowPointer
            WHERE LEFT(item.Item, 4) IN ('9001','9004')
              AND a.AttributeLabel='Type') item_att_all ON a.Item = item_att_all.Item
