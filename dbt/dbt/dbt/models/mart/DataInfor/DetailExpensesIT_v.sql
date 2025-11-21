{{
  config(
    materialized= 'table'
  )
}}

SELECT DISTINCT
  ledger.Acct,
  ledger.ChaDescription,
  ledger.TransDate,
  ledger.VendNum,
  ledger.Voucher,
  ledger.MatlTransNum,
  ledger.TransNum,
  ledger.AcctUnit1,
  ledger.AcctUnit2,
  ledger.AcctUnit3,
  ledger.AcctUnit4,
  ledger.Dercontent,
  ledger.DocumentNum,
  ledger.CheckNum,
  ledger.Ref,
  ledger.DerDomAmountDebit,
  ledger.DerDomAmountCredit,
  mat_trans.Item,
  mat_trans.ItemDescription,
  mat_trans.Qty,
  mat_trans.RefNum,
  mat_trans.RefLineSuf,
  po_detail.PoNum, 
  po_detail.PoLine,
  po_detail.Description
FROM {{ ref('ledger_notes_v') }} ledger
  LEFT JOIN (SELECT 
              TransNum,
              Item,
              ItemDescription,
              Qty,
              RefNum,
              RefLineSuf
            FROM {{ source('mp_infor', 'material_transaction') }}
            ) mat_trans ON ledger.MatlTransNum = mat_trans.TransNum
  LEFT JOIN (SELECT 
              PoNum, 
              PoLine,
              Description 
             FROM {{ source('mp_infor', 'po_detail') }} ) po_detail ON mat_trans.RefNum = po_detail.PoNum
                                                  AND mat_trans.RefLineSuf = po_detail.PoLine
WHERE ledger.AcctUnit1 = '21'

