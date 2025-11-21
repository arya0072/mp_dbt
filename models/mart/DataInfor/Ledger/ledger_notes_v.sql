{{
  config(
    materialized= 'table'
  )
}}

SELECT
  led.ChaDescription,
  led.Acct,
  led.AcctUnit1,
  led.AcctUnit2,
  led.AcctUnit3,
  led.AcctUnit4,
  led.ControlPrefix,
  led.Ref,
  led.Voucher,
  led.VendNum,
  led.TransDate,
  led.TransNum,
  led.DocumentNum,
  led.MatlTransNum,
  led.CheckNum,
  mat_trans.Whse,
  led.RowPointer,
  led.DerDomAmountDebit,
  led.DerDomAmountCredit,
  coalesce(led.DerDomAmountDebit,0) - coalesce(led.DerDomAmountCredit,0) as balance,
  led.Site,
  notes.Dercontent,
  notes.RecordDate
FROM {{ ref('ledger_v') }} AS led
  LEFT JOIN (SELECT distinct
    a.RefRowPointer,
    a.RecordDate,
    a.Dercontent,
    ledger.ref
  FROM {{ ref('led_notes_v') }} AS a
    JOIN (SELECT  
            DISTINCT MAX(RecordDate) as RecordDate,
            RefRowPointer
          FROM {{ ref('led_notes_v') }}
          GROUP BY RefRowPointer) min_notes ON a.RefRowPointer = min_notes.RefRowPointer  
                                            AND a.RecordDate = min_notes.RecordDate
    LEFT JOIN {{ source('mp_infor', 'ledger') }} ledger ON a.RefRowPointer = ledger.RowPointer
          ) notes ON led.RowPointer = notes.RefRowPointer 
  LEFT JOIN (SELECT 
              TransNum,
              Item,
              ItemDescription,
              Qty,
              RefNum,
              RefLineSuf,
              Whse
            FROM {{ source('mp_infor', 'material_transaction') }}
            ) mat_trans ON led.MatlTransNum = mat_trans.TransNum
-- where notes.Dercontent IS NOT NULL  
-- WHERE Acct = '6-2328'                               

