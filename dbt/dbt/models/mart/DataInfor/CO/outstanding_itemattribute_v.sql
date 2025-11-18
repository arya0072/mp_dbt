{{
  config(
    materialized= 'table'
  )
}}

SELECT
  a.ue_AttributeValue,
  a.ue_bill_term,
  a.ue_CIDesc,
  a.ue_co_line,
  a.ue_co_num,
  a.ue_CoiDescription,
  a.ue_CoiOriPrice,
  a.ue_CoiQtyOpen,
  a.ue_CoiQtyOrdered,
  a.ue_CoiQtyShipped,
  a.ue_curr_code,
  a.ue_cust_item,
  a.ue_cust_num,
  a.ue_cust_po,
  a.ue_custname,
  a.ue_delterm,
  a.ue_disc,
  a.ue_due_date,
  a.ue_item,
  a.ue_ItemUM,
  a.ue_LineStatus,
  a.ue_order_date,
  a.ue_Overdue,
  a.ue_PriceAfterDisc,
  a.ue_ProductCode,
  a.ue_projected_date,
  a.ue_PromiseDate,
  a.ue_QtyOpenInPcs,
  a.ue_QtyOpenPerItemUM,
  a.ue_QtyOrderedInPcs,
  a.ue_QtyOrderedPerUM,
  a.ue_QtyShippedInPcs,
  a.ue_QtyShippedPerItemUM,
  a.ue_SFCCONesCode,
  a.ue_SFCCONesDesc,
  a.ue_TotalValueQtyOpen,
  a.ue_u_m,ue_ProdCodeDesc,
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
  item_cust.Uf_MP04_CIPrintTipCategory AS ue_PrintTipCat, 
  a.ue_QtyReadyToShip,
  a.ue_QtyReadyToShipInCoUM,
  a.ue_QtyReadyToShipInPcs,
  a.ue_QtyAfterPlanShipment,
  a.ue_QtyAfterPlanShipmentInCoUM,
  a.ue_QtyAfterPlanShipmentInPcs,
  a.ue_TotalValueQtyOpenAfterReadyToShip,
  a.ue_Uf_MP49_EORCustomTips,
  a.ue_Uf_MP49_EORCones,
  a.ue_Uf_MP49_EORPackaging,
  a.ue_Uf_MP49_EORStickerEtc,
  a.ue_Uf_MP49_EORPacking,
  a.ue_InitialReqDate,
  a.ue_ConfirmDate,
  a.ue_EffectiveDate,
  a.ue_Note,
  item_cust.itmcstUf_MP04_CIDesc as CI_description,
  item_cust.itmcstUf_MP04_CustProdCode as CustProductCode,
  item_att_all.AttributeLabel,
  item_att_all.Type,
  item_cust.itmcstUf_MP04_ConeCategory AS cone_category,
  a.ue_RollingGroup AS RollingGroup,
  item_cust.effectdate AS FG_ReleaseDate,
  a.ue_Uf_MP144_PPIC_Note AS PPIC_Notes
  -- budget_sales.*
from {{ source('mp_infor', 'outstanding_co') }} a
  LEFT JOIN {{ source('mp_infor', 'item_atrribute') }} item_att ON a.ue_item = item_att.ue_Item
  LEFT JOIN {{ source('mp_infor', 'itemsCustomer') }} item_cust ON a.ue_item = item_cust.Item AND a.ue_cust_num = item_cust.CustNum
  -- LEFT JOIN `mp_infor.budget_sales` budget_sales ON a.ue_cust_num = budget_sales.CustNum
  --                                               AND a.ue_cust_item = budget_sales.NPD
  --                                               AND EXTRACT(YEAR FROM a.ue_PromiseDate) = CAST(budget_sales.Year AS INT64)
  --                                               AND EXTRACT(MONTH FROM a.ue_PromiseDate) = CAST(budget_sales.Period AS INT64)
  LEFT JOIN (SELECT 
              item.Item, 
              a.*
            FROM {{ source('mp_infor', 'item_attribute_all') }} a
              LEFT JOIN {{ source('mp_infor', 'items') }} item ON item.RowPointer = a.RefRowPointer
            WHERE LEFT(item.Item, 4) IN ('9001','9004')
              AND a.AttributeLabel='Type') item_att_all ON a.ue_item = item_att_all.Item
