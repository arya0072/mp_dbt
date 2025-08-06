{{
  config(
    materialized= 'table'
  )
}}

SELECT 
  'PR' AS data_source,
  CAST(pr_noninv.AgingOST AS INT64) AS AgingOST,
  pr_noninv.DueDatePO,
  pr_noninv.Item,
  pr_noninv.ItemDesc,
  pr_noninv.MatlCost,
  pr_noninv.OrderDate,
  pr_noninv.PaymentDate,
  pr_noninv.PendingPR,
  pr_noninv.PoApproveDate,
  pr_noninv.PoLine,
  pr_noninv.PoLineStatus,
  pr_noninv.PoNum,
  pr_noninv.PoToAcctDate,
  pr_noninv.PoToVendDate,
  pr_noninv.PrApproveDate,
  pr_noninv.PRLineStatus,
  pr_noninv.PromiseDate,
  pr_noninv.QtyOrdered,
  pr_noninv.QtyReceived,
  pr_noninv.Ranking,
  pr_noninv.RcvdDate,
  pr_noninv.Reason,
  pr_noninv.ReqDate,
  pr_noninv.ReqNum,
  pr_noninv.Requester,
  pr_noninv.StatusOST,
  pr_noninv.Total,
  pr_noninv.UM,
  pr_noninv.VendName,
  pr_noninv.VendNum,
  pr_noninv.LeadTime,
  pr_noninv.LeadTimeStatus,
  pr_noninv.CoaNum,
  pr_noninv.CoaDescription,
  pr_noninv.POBuyer,
  pr_noninv.DueDatePR,
  pr_noninv.TermsCode,
  pr_noninv.TermsCodeDesc,
  pr_noninv.AgingPO,
  pr_noninv.AgingPR,
  pr_header.preqUf_MP74_StatusApproval AS status_approve,
  pr_header.RowPointer,
  COALESCE(cek_agreement.DocumentName, 'Non - Agreement') AS documentname,
  pr_noninv.Username,
  pr_items.RowPointer as LineRowPointer,
  pr_items.item AS LineItems,
  pr_items.Description AS LineDescription,
  pr_items.DueDate AS LineDueDate,
  pr_items.UM AS LineUom,
  pr_items.Buyer AS LineBuyer,
  PRPO_NotesDetail.DerContent,
  pr_noninv.ReqLine
FROM {{ source('mp_infor', 'PR_NonInventory') }} pr_noninv
  LEFT JOIN {{ source('mp_infor', 'pr_header') }} pr_header ON pr_noninv.ReqNum = pr_header.ReqNum
  LEFT JOIN (SELECT
              TableRowPointer,
              DocumentName
            FROM {{ source('mp_infor', 'document_object') }} 
            WHERE DocumentName='Agreement') cek_agreement ON pr_header.RowPointer = cek_agreement.TableRowPointer
  LEFT JOIN {{ source('mp_infor', 'PR_Items') }} pr_items ON pr_noninv.ReqNum = pr_items.ReqNum AND pr_noninv.ReqLine = pr_items.ReqLine
  LEFT JOIN {{ source('mp_infor', 'PRPO_NotesDetail') }} PRPO_NotesDetail ON pr_items.RowPointer = PRPO_NotesDetail.RefRowPointer
  WHERE pr_noninv.PoNum IS NULL 
    AND pr_noninv.PRLineStatus='Requested'
UNION ALL
SELECT 
  'PO' AS data_source,
  CAST(po_noninv.AgingOST AS INT64) AS AgingOST,
  po_noninv.DueDatePO,
  po_noninv.Item,
  po_noninv.ItemDesc,
  po_noninv.MatlCost,
  po_noninv.OrderDate,
  po_noninv.PaymentDate,
  po_noninv.PendingPR,
  po_noninv.PoApproveDate,
  po_noninv.PoLine,
  po_noninv.PoLineStatus,
  po_noninv.PoNum,
  po_noninv.PoToAcctDate,
  po_noninv.PoToVendDate,
  po_noninv.PrApproveDate,
  po_noninv.PRLineStatus,
  po_noninv.PromiseDate,
  po_noninv.QtyOrdered,
  po_noninv.QtyReceived,
  po_noninv.Ranking,
  po_noninv.RcvdDate,
  po_noninv.Reason,
  po_noninv.ReqDate,
  po_noninv.ReqNum,
  po_noninv.Requester,
  po_noninv.StatusOST,
  po_noninv.Total,
  po_noninv.UM,
  po_noninv.VendName,
  po_noninv.VendNum,
  po_noninv.LeadTime,
  po_noninv.LeadTimeStatus,
  po_noninv.CoaNum,
  po_noninv.CoaDescription,
  po_noninv.POBuyer,
  po_noninv.DueDatePR,
  po_noninv.TermsCode,
  po_noninv.TermsCodeDesc,
  NULL AS AgingPO,
  NULL AS AgingPR,
  po_header.poUf_MP74_StatusApproval AS status_approve,
  po_header.RowPointer,
  COALESCE(cek_agreement.DocumentName, 'Non - Agreement') AS documentname,
  username.Username,
  po_items.RowPointer as ItemRowPointer,
  po_items.item AS LineItems,
  po_items.Description AS LineDescription,
  po_items.DueDate AS LineDueDate,
  po_items.UM AS LineUom,
  NULL AS LineBuyer,
  PRPO_NotesDetail.DerContent,
  NULL AS ReqLine
FROM {{ source('mp_infor', 'PO_NonInventory') }} po_noninv
  LEFT JOIN {{ source('mp_infor', 'po_header') }} po_header ON po_noninv.ponum = po_header.ponum
  LEFT JOIN (SELECT
              TableRowPointer,
              DocumentName
            FROM {{ source('mp_infor', 'document_object') }}
            WHERE DocumentName='Agreement') cek_agreement ON po_header.RowPointer = cek_agreement.TableRowPointer
  LEFT JOIN {{ source('mp_infor', 'PO_Items') }} po_items ON po_noninv.ponum = po_items.ponum AND po_noninv.PoLine = po_items.PoLine
  LEFT JOIN {{ source('mp_infor', 'PRPO_NotesDetail') }} PRPO_NotesDetail ON po_items.RowPointer = PRPO_NotesDetail.RefRowPointer
  LEFT JOIN (select PoNum,
                    Username 
             FROM {{ source('mp_infor', 'PR_NonInventory') }} 
             GROUP BY PoNum,Username) username ON po_noninv.ponum = username.ponum
  where po_noninv.PoNum IS NOT NULL
  -- AND po_noninv.PoLineStatus IN ('Planned','Ordered')
  -- AND po_noninv.PoNum='PN-0004353'
 