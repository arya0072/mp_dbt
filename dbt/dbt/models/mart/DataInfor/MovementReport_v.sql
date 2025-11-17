{{
  config(
    materialized= 'table'
  )
}} 

SELECT
  DISTINCT
  i.item AS Item,
  FORMAT_DATE('%Y-%m', mt.TransDate) AS Periode,
  i.Description,
  i.UM,
  ch.Acct,
  ch.Description AS AcctDescription,
  i.ProductCode,
  pc.Description AS ProductCodeDescription,
  coitem.CustNum AS Customer,
  cust.name AS CustomerName,
  mt.Whse AS Warehouse,
  mt.Loc AS Location,
  mt.TransType,
  -- mt.MatlTranViewTypeDesc AS TransactionDesc,
  CASE
    WHEN mt.TransType = 'A' THEN 'Adjustment'
    WHEN mt.TransType = 'B' THEN 'Cycle Count'
    WHEN mt.TransType = 'C' THEN 'Split/Merge/Create'
    WHEN mt.TransType = 'D' THEN 'Scrap'
    WHEN mt.TransType = 'F' THEN 'Finish'
    WHEN mt.TransType = 'G' THEN 'Misc.Issue'
    WHEN mt.TransType = 'H' THEN 'Misc.Receipt'
    WHEN mt.TransType = 'I' THEN 'Issue/WIPChange'
    WHEN mt.TransType = 'L' THEN 'Transfer Loss'
    WHEN mt.TransType = 'M' THEN 'Stock Move'
    WHEN mt.TransType = 'N' THEN 'Labor/NextOperation'
    WHEN mt.TransType = 'O' THEN 'Other Cost'
    WHEN mt.TransType = 'P' THEN 'Physical Inventory'
    WHEN mt.TransType = 'R' THEN 'Receipt'
    WHEN mt.TransType = 'S' THEN 'Ship'
    WHEN mt.TransType = 'T' THEN 'Transfer Order'
    WHEN mt.TransType = 'W' THEN 'Withdrawal/Return'
  ELSE NULL
  END AS TransactionType,
  mt.RefType,
  CASE
    WHEN mt.RefType = 'C' THEN 'Project'
    WHEN mt.RefType = 'F' THEN 'Service Order'
    WHEN mt.RefType = 'I' THEN 'Inventory'
    WHEN mt.RefType = 'J' THEN 'Job'
    WHEN mt.RefType = 'K' THEN 'JIT Production'
    WHEN mt.RefType = 'O' THEN 'Customer Order'
    WHEN mt.RefType = 'P' THEN 'Purchase Order'
    WHEN mt.RefType = 'R' THEN 'RMA'
    WHEN mt.RefType = 'S' THEN 'Producttion schedule'
    WHEN mt.RefType = 'T' THEN 'Transfer'
    WHEN mt.RefType = 'W' THEN 'Work Center'
  ELSE NULL
  END AS ReferenceType,
  CASE 
    WHEN mt.TransType = 'F' AND mt.RefType = 'J' THEN  FinishJob.Material
    WHEN mt.TransType = 'T' AND mt.RefType = 'T' THEN  TransferOrder.Material
    WHEN mt.TransType = 'P' AND mt.RefType = 'I' THEN  PhysicalInv.Material
    WHEN mt.TransType = 'S' AND mt.RefType = 'O' THEN  ShipCO.Material
    WHEN mt.TransType = 'H' AND mt.RefType = 'I' THEN  MissRI.Material
    WHEN mt.TransType = 'M' AND mt.RefType = 'I' THEN  StockMoveInv.Material
    WHEN mt.TransType = 'G' AND mt.RefType = 'I' THEN  MissIssueInv.Material
    WHEN mt.TransType = 'R' AND mt.RefType = 'P' THEN  ReceiptPO.Material  
    WHEN mt.TransType = 'W' AND mt.RefType = 'P' THEN  WithdrawPO.Material
    WHEN mt.TransType = 'W' AND mt.RefType = 'R' THEN  WithdrawRMA.Material
    WHEN mt.TransType = 'W' AND mt.RefType = 'J' THEN  WithdrawJOB.Material
    WHEN mt.TransType = 'I' AND mt.RefType = 'J' THEN  IssueJob.Material
    WHEN mt.TransType = 'L' AND mt.RefType = 'T' THEN  TransferLoss.Material
    WHEN mt.TransType = 'B' AND mt.RefType = 'I' THEN  CycleCountInv.Material
    WHEN mt.TransType = 'C' AND mt.RefType = 'J' THEN  SMCJob.Material
    WHEN mt.TransType = 'A' AND mt.RefType = 'I' THEN  AdjInv.Material
    ELSE 0
  END Material,
  CASE 
    WHEN mt.TransType = 'F' AND mt.RefType = 'J' THEN  FinishJob.Labour
    WHEN mt.TransType = 'T' AND mt.RefType = 'T' THEN  TransferOrder.Labour
    WHEN mt.TransType = 'P' AND mt.RefType = 'I' THEN  PhysicalInv.Labour
    WHEN mt.TransType = 'S' AND mt.RefType = 'O' THEN  ShipCO.Labour
    WHEN mt.TransType = 'H' AND mt.RefType = 'I' THEN  MissRI.Labour
    WHEN mt.TransType = 'M' AND mt.RefType = 'I' THEN  StockMoveInv.Labour
    WHEN mt.TransType = 'G' AND mt.RefType = 'I' THEN  MissIssueInv.Labour
    WHEN mt.TransType = 'R' AND mt.RefType = 'P' THEN  ReceiptPO.Labour
    WHEN mt.TransType = 'W' AND mt.RefType = 'P' THEN  WithdrawPO.Labour
    WHEN mt.TransType = 'W' AND mt.RefType = 'R' THEN  WithdrawRMA.Labour
    WHEN mt.TransType = 'W' AND mt.RefType = 'J' THEN  WithdrawJOB.Labour
    WHEN mt.TransType = 'I' AND mt.RefType = 'J' THEN  IssueJob.Labour
    WHEN mt.TransType = 'L' AND mt.RefType = 'T' THEN  TransferLoss.Labour
    WHEN mt.TransType = 'B' AND mt.RefType = 'I' THEN  CycleCountInv.Labour
    WHEN mt.TransType = 'C' AND mt.RefType = 'J' THEN  SMCJob.Labour
    WHEN mt.TransType = 'A' AND mt.RefType = 'I' THEN  AdjInv.Labour
    ELSE 0
  END Labour,
  CASE 
    WHEN mt.TransType = 'F' AND mt.RefType = 'J' THEN  FinishJob.FixOH
    WHEN mt.TransType = 'T' AND mt.RefType = 'T' THEN  TransferOrder.FixOH
    WHEN mt.TransType = 'P' AND mt.RefType = 'I' THEN  PhysicalInv.FixOH
    WHEN mt.TransType = 'S' AND mt.RefType = 'O' THEN  ShipCO.FixOH
    WHEN mt.TransType = 'H' AND mt.RefType = 'I' THEN  MissRI.FixOH
    WHEN mt.TransType = 'M' AND mt.RefType = 'I' THEN  StockMoveInv.FixOH
    WHEN mt.TransType = 'G' AND mt.RefType = 'I' THEN  MissIssueInv.FixOH
    WHEN mt.TransType = 'R' AND mt.RefType = 'P' THEN  ReceiptPO.FixOH
    WHEN mt.TransType = 'W' AND mt.RefType = 'P' THEN  WithdrawPO.FixOH
    WHEN mt.TransType = 'W' AND mt.RefType = 'R' THEN  WithdrawRMA.FixOH
    WHEN mt.TransType = 'W' AND mt.RefType = 'J' THEN  WithdrawJOB.FixOH
    WHEN mt.TransType = 'I' AND mt.RefType = 'J' THEN  IssueJob.FixOH
    WHEN mt.TransType = 'L' AND mt.RefType = 'T' THEN  TransferLoss.FixOH
    WHEN mt.TransType = 'B' AND mt.RefType = 'I' THEN  CycleCountInv.FixOH
    WHEN mt.TransType = 'C' AND mt.RefType = 'J' THEN  SMCJob.FixOH
    WHEN mt.TransType = 'A' AND mt.RefType = 'I' THEN  AdjInv.FixOH
    ELSE 0
  END FixOH,
  CASE 
    WHEN mt.TransType = 'F' AND mt.RefType = 'J' THEN  FinishJob.VariabelOH
    WHEN mt.TransType = 'T' AND mt.RefType = 'T' THEN  TransferOrder.VariabelOH
    WHEN mt.TransType = 'P' AND mt.RefType = 'I' THEN  PhysicalInv.VariabelOH
    WHEN mt.TransType = 'S' AND mt.RefType = 'O' THEN  ShipCO.VariabelOH
    WHEN mt.TransType = 'H' AND mt.RefType = 'I' THEN  MissRI.VariabelOH
    WHEN mt.TransType = 'M' AND mt.RefType = 'I' THEN  StockMoveInv.VariabelOH
    WHEN mt.TransType = 'G' AND mt.RefType = 'I' THEN  MissIssueInv.VariabelOH
    WHEN mt.TransType = 'R' AND mt.RefType = 'P' THEN  ReceiptPO.VariabelOH
    WHEN mt.TransType = 'W' AND mt.RefType = 'P' THEN  WithdrawPO.VariabelOH
    WHEN mt.TransType = 'W' AND mt.RefType = 'R' THEN  WithdrawRMA.VariabelOH
    WHEN mt.TransType = 'W' AND mt.RefType = 'J' THEN  WithdrawJOB.VariabelOH
    WHEN mt.TransType = 'I' AND mt.RefType = 'J' THEN  IssueJob.VariabelOH
    WHEN mt.TransType = 'L' AND mt.RefType = 'T' THEN  TransferLoss.VariabelOH
    WHEN mt.TransType = 'B' AND mt.RefType = 'I' THEN  CycleCountInv.VariabelOH
    WHEN mt.TransType = 'C' AND mt.RefType = 'J' THEN  SMCJob.VariabelOH
    WHEN mt.TransType = 'A' AND mt.RefType = 'I' THEN  AdjInv.VariabelOH
    ELSE 0
  END VariabelOH,
   CASE 
    WHEN mt.TransType = 'F' AND mt.RefType = 'J' THEN  FinishJob.Outside
    WHEN mt.TransType = 'T' AND mt.RefType = 'T' THEN  TransferOrder.Outside
    WHEN mt.TransType = 'P' AND mt.RefType = 'I' THEN  PhysicalInv.Outside
    WHEN mt.TransType = 'S' AND mt.RefType = 'O' THEN  ShipCO.Outside
    WHEN mt.TransType = 'H' AND mt.RefType = 'I' THEN  MissRI.Outside
    WHEN mt.TransType = 'M' AND mt.RefType = 'I' THEN  StockMoveInv.Outside
    WHEN mt.TransType = 'G' AND mt.RefType = 'I' THEN  MissIssueInv.Outside
    WHEN mt.TransType = 'R' AND mt.RefType = 'P' THEN  ReceiptPO.Outside
    WHEN mt.TransType = 'W' AND mt.RefType = 'P' THEN  WithdrawPO.Outside
    WHEN mt.TransType = 'W' AND mt.RefType = 'R' THEN  WithdrawRMA.Outside
    WHEN mt.TransType = 'W' AND mt.RefType = 'J' THEN  WithdrawJOB.Outside
    WHEN mt.TransType = 'I' AND mt.RefType = 'J' THEN  IssueJob.Outside
    WHEN mt.TransType = 'L' AND mt.RefType = 'T' THEN  TransferLoss.Outside
    WHEN mt.TransType = 'B' AND mt.RefType = 'I' THEN  CycleCountInv.Outside
    WHEN mt.TransType = 'C' AND mt.RefType = 'J' THEN  SMCJob.Outside
     WHEN mt.TransType = 'A' AND mt.RefType = 'I' THEN AdjInv.Outside
    ELSE 0
  END Outside
FROM `mp_infor.items_new` i
  LEFT JOIN `mp_infor.product_codes_BQ` pc ON CAST(i.ProductCode AS STRING) = CAST(pc.ProductCode AS STRING)
  LEFT JOIN `mp_infor.distribution_productcodes` da ON CAST(i.ProductCode AS STRING) = CAST(da.ProductCode AS STRING)
  LEFT JOIN `mp_infor.COA` ch ON CAST(da.InvAcct AS STRING) = CAST(ch.Acct AS STRING)
  LEFT JOIN `mp_infor.COItems` coitem ON CAST(i.item AS STRING) = CAST(coitem.Item AS STRING)
  LEFT JOIN `mp_infor.MP_Customer` cust ON coitem.CustNum = cust.CustNum
  LEFT JOIN `mp_infor.material_transaction` mt ON CAST(i.item AS STRING) = CAST(mt.Item AS STRING)
  LEFT JOIN `mp_infor.material_tran_acct` mta ON CAST(mt.TransNum AS STRING) = CAST(mta.TransNum AS STRING)
  LEFT JOIN (             ----------------------------------------------------------------- Finish Job -----------------------------------------------------------------
              SELECT 
                mt.Item,
                mt.Whse,
                mt.Loc,
                FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                SUM(mt.Qty) AS Qty,
                SUM(mt.MatlCost * mt.Qty) AS Material,
                SUM(mt.LbrCost * mt.Qty) AS Labour,
                SUM(mt.FovhdCost * mt.Qty) AS FixOH,
                SUM(mt.VovhdCost * mt.Qty) AS VariabelOH,
                SUM(mt.OutCost * mt.Qty) AS Outside
              FROM `mp_infor.material_transaction` mt
              WHERE mt.TransType='F'
                AND mt.RefType  ='J' 
              GROUP BY mt.item,
                mt.Whse,
                mt.Loc,
                Period
              ) FinishJob ON CAST(i.item AS STRING) = CAST(FinishJob.Item AS STRING)
                          AND  FORMAT_DATE('%Y-%m', mt.TransDate) = FinishJob.Period
                          AND mt.Whse = FinishJob.Whse
                          AND mt.Loc = FinishJob.Loc
                  
    LEFT JOIN (     ----------------------------------------------------------------- Transfer Order -----------------------------------------------------------------
               SELECT 
                  mt.Item,
                  -- mt.Whse,
                  -- mt.Loc,
                  FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                  SUM(mt.Qty) AS Qty,
                  SUM(mt.MatlCost * mt.Qty) AS Material,
                  SUM(mt.LbrCost * mt.Qty) AS Labour,
                  SUM(mt.FovhdCost * mt.Qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.Qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.Qty) AS Outside
                FROM `mp_infor.material_transaction` mt
                WHERE mt.TransType='T'
                  AND mt.RefType  ='T' 
                GROUP BY mt.item,
                    -- mt.Whse,
                    -- mt.Loc,
                    Period
              ) TransferOrder ON CAST(i.item AS STRING) = CAST(TransferOrder.Item AS STRING)
                          AND FORMAT_DATE('%Y-%m', mt.TransDate) = TransferOrder.Period
                          -- AND mt.Whse = TransferOrder.Whse
                          -- AND mt.Loc = TransferOrder.Loc
                         
    LEFT JOIN (     ----------------------------------------------------------------- Physical Inventory -----------------------------------------------------------------
               SELECT 
                  mt.Item,
                  mt.Whse,
                  mt.Loc,
                  FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                  SUM(mt.Qty) AS Qty,
                  SUM(mt.MatlCost * mt.Qty) AS Material,
                  SUM(mt.LbrCost * mt.Qty) AS Labour,
                  SUM(mt.FovhdCost * mt.Qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.Qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.Qty) AS Outside
                FROM `mp_infor.material_transaction` mt
                WHERE mt.TransType='P'
                  AND mt.RefType  ='I' 
                GROUP BY mt.item,
                    mt.Whse,
                    mt.Loc,
                    Period
              ) PhysicalInv ON CAST(i.item AS STRING) = CAST(PhysicalInv.Item AS STRING)
                            AND FORMAT_DATE('%Y-%m', mt.TransDate) = PhysicalInv.Period
                            AND mt.Whse = PhysicalInv.Whse
                            AND mt.Loc = PhysicalInv.Loc
                            
    LEFT JOIN (     ----------------------------------------------------------------- Miss Receipt Inv -----------------------------------------------------------------
               SELECT 
                  mt.Item,
                  mt.Whse,
                  mt.Loc,
                  FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                  SUM(mt.Qty) AS Qty,
                  SUM(mt.MatlCost * mt.Qty) AS Material,
                  SUM(mt.LbrCost * mt.Qty) AS Labour,
                  SUM(mt.FovhdCost * mt.Qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.Qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.Qty) AS Outside
                FROM `mp_infor.material_transaction` mt
                WHERE mt.TransType='H'
                  AND mt.RefType  ='I'
                GROUP BY mt.item,
                    mt.Whse,
                    mt.Loc,
                    Period
              ) MissRI ON CAST(i.item AS STRING) = CAST(MissRI.Item AS STRING)
                       AND FORMAT_DATE('%Y-%m', mt.TransDate) = MissRI.Period
                       AND mt.Whse = MissRI.Whse
                       AND mt.Loc = MissRI.Loc
    LEFT JOIN (     ----------------------------------------------------------------- Ship Customer Order -----------------------------------------------------------------
               SELECT 
                  mt.Item,
                  mt.Whse,
                  mt.Loc,
                  FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                  SUM(mt.Qty) AS Qty,
                  SUM(mt.MatlCost * mt.Qty) AS Material,
                  SUM(mt.LbrCost * mt.Qty) AS Labour,
                  SUM(mt.FovhdCost * mt.Qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.Qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.Qty) AS Outside
                FROM `mp_infor.material_transaction` mt
                WHERE mt.TransType='S'
                  AND mt.RefType  ='O'
                GROUP BY mt.item,
                    mt.Whse,
                    mt.Loc,
                    Period
              ) ShipCO ON CAST(i.item AS STRING) = CAST(ShipCO.Item AS STRING)
                       AND FORMAT_DATE('%Y-%m', mt.TransDate) = ShipCO.Period
                       AND mt.Whse = ShipCO.Whse
                       AND mt.Loc = ShipCO.Loc
    LEFT JOIN (     ----------------------------------------------------------------- Receipt PO -----------------------------------------------------------------
               SELECT 
                  mt.Item,
                  mt.Whse,
                  -- mt.Loc,
                  FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                  SUM(mt.Qty) AS Qty,
                  SUM(mt.MatlCost * mt.Qty) AS Material,
                  SUM(mt.LbrCost * mt.Qty) AS Labour,
                  SUM(mt.FovhdCost * mt.Qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.Qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.Qty) AS Outside
                FROM `mp_infor.material_transaction` mt
                WHERE mt.TransType='R'
                  AND mt.RefType  ='P' 
                GROUP BY mt.item,
                    mt.Whse,
                    -- mt.Loc,
                    Period
              ) ReceiptPO ON CAST(i.item AS STRING) = CAST(ReceiptPO.Item AS STRING)
                          AND FORMAT_DATE('%Y-%m', mt.TransDate) = ReceiptPO.Period
                          AND mt.Whse = ReceiptPO.Whse
                          -- AND mt.Loc = ReceiptPO.Loc
    LEFT JOIN (     ----------------------------------------------------------------- Withdrawal/Return PO -----------------------------------------------------------------
               SELECT 
                mt.Item,
                mt.Whse,
                mt.Loc,
                FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                SUM(mt.MatlCost * mt.Qty) AS Material,
                SUM(mt.LbrCost * mt.Qty) AS Labour,
                SUM(mt.FovhdCost * mt.Qty) AS FixOH,
                SUM(mt.VovhdCost * mt.Qty) AS VariabelOH,
                SUM(mt.OutCost * mt.Qty) AS Outside
              FROM `mp_infor.material_transaction` mt
              WHERE mt.TransType='W'
                AND mt.RefType  ='P'
              GROUP BY mt.item,
                  mt.Whse,
                  mt.Loc,
                  Period
              ) WithdrawPO ON CAST(i.item AS STRING) = CAST(WithdrawPO.Item AS STRING)
                           AND FORMAT_DATE('%Y-%m', mt.TransDate) = WithdrawPO.Period
                           AND mt.Whse = WithdrawPO.Whse
                           AND mt.Loc = WithdrawPO.Loc
    LEFT JOIN (     ----------------------------------------------------------------- Withdrawal/Return RMA -----------------------------------------------------------------
               SELECT 
                  mt.Item,
                  mt.Whse,
                  mt.Loc,
                  FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                  SUM(Qty) AS Qty,
                  SUM(mt.MatlCost * mt.Qty) AS Material,
                  SUM(mt.LbrCost * mt.Qty) AS Labour,
                  SUM(mt.FovhdCost * mt.Qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.Qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.Qty) AS Outside
                FROM `mp_infor.material_transaction` mt
                WHERE mt.TransType='W'
                  AND mt.RefType  ='R' 
                GROUP BY mt.item,
                    mt.Whse,
                    mt.Loc,
                    Period
              ) WithdrawRMA ON CAST(i.item AS STRING) = CAST(WithdrawRMA.Item AS STRING)
                            AND FORMAT_DATE('%Y-%m', mt.TransDate) = WithdrawRMA.Period
                            AND mt.Whse = WithdrawRMA.Whse
                            AND mt.Loc = WithdrawRMA.Loc
    LEFT JOIN (     ----------------------------------------------------------------- Withdrawal/Return JOB -----------------------------------------------------------------
               SELECT 
                  mt.Item,
                  mt.Whse,
                  mt.Loc,
                  FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                  SUM(Qty) AS Qty,
                  SUM(mt.MatlCost * mt.Qty) AS Material,
                  SUM(mt.LbrCost * mt.Qty) AS Labour,
                  SUM(mt.FovhdCost * mt.Qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.Qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.Qty) AS Outside
                FROM `mp_infor.material_transaction` mt
                WHERE mt.TransType='W'
                  AND mt.RefType  ='J' 
                GROUP BY mt.item,
                    mt.Whse,
                    mt.Loc,
                    Period
              ) WithdrawJOB ON CAST(i.item AS STRING) = CAST(WithdrawJOB.Item AS STRING)
                            AND FORMAT_DATE('%Y-%m', mt.TransDate) = WithdrawJOB.Period
                            AND mt.Whse = WithdrawJOB.Whse
                            AND mt.Loc = WithdrawJOB.Loc
    LEFT JOIN (     ----------------------------------------------------------------- Issue/WIPChange Job -----------------------------------------------------------------
               SELECT 
                  mt.Item,
                  mt.Whse,
                  mt.Loc,
                  FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                  SUM(Qty) AS Qty,
                  SUM(mt.MatlCost * mt.Qty) AS Material,
                  SUM(mt.LbrCost * mt.Qty) AS Labour,
                  SUM(mt.FovhdCost * mt.Qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.Qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.Qty) AS Outside
                FROM `mp_infor.material_transaction` mt
                WHERE mt.TransType='I'
                  AND mt.RefType  ='J' 
                GROUP BY mt.item,
                    mt.Whse,
                    mt.Loc,
                    Period
              ) IssueJob ON CAST(i.item AS STRING) = CAST(IssueJob.Item AS STRING)
                         AND FORMAT_DATE('%Y-%m', mt.TransDate) = IssueJob.Period
                         AND mt.Whse = IssueJob.Whse
                         AND mt.Loc = IssueJob.Loc
    LEFT JOIN (     ----------------------------------------------------------------- Stock Move Inventory -----------------------------------------------------------------
               SELECT 
                mt.Item,
                mt.Whse,
                -- mt.Loc,
                FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                SUM(mt.Qty) AS Qty,
                SUM(mt.MatlCost * mt.Qty) AS Material,
                SUM(mt.LbrCost * mt.Qty) AS Labour,
                SUM(mt.FovhdCost * mt.Qty) AS FixOH,
                SUM(mt.VovhdCost * mt.Qty) AS VariabelOH,
                SUM(mt.OutCost * mt.Qty) AS Outside
              FROM `mp_infor.material_transaction` mt
              WHERE mt.TransType='M'
                AND mt.RefType  ='I'
              GROUP BY mt.item,
                    mt.Whse,
                    -- mt.Loc,
                    Period
              ) StockMoveInv ON CAST(i.item AS STRING) = CAST(StockMoveInv.Item AS STRING)
                             AND FORMAT_DATE('%Y-%m', mt.TransDate) = StockMoveInv.Period
                             AND mt.Whse = StockMoveInv.Whse
                            --  AND mt.Loc = StockMoveInv.Loc
                
    LEFT JOIN (     ----------------------------------------------------------------- Transfer Loss Transfer -----------------------------------------------------------------
               SELECT 
                  mt.Item,
                  mt.Whse,
                  mt.Loc,
                  FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                  SUM(mt.qty) AS Qty,
                  SUM(mt.MatlCost * mt.qty) AS Material,
                  SUM(mt.LbrCost * mt.qty) AS Labour,
                  SUM(mt.FovhdCost * mt.qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.qty) AS Outside
                FROM `mp_infor.material_transaction` mt
                WHERE mt.TransType='L'
                  AND mt.RefType  ='T'
                GROUP BY mt.item,
                    mt.Whse,
                    mt.Loc,
                    Period
              ) TransferLoss ON CAST(i.item AS STRING) = CAST(TransferLoss.Item AS STRING)
                             AND FORMAT_DATE('%Y-%m', mt.TransDate) = TransferLoss.Period
                             AND mt.Whse = TransferLoss.Whse
                             AND mt.Loc = TransferLoss.Loc
    LEFT JOIN (     ----------------------------------------------------------------- Cycle Count Inventory -----------------------------------------------------------------
               SELECT 
                  mt.Item,
                  mt.Whse,
                  mt.Loc,
                  FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                  SUM(mt.qty) AS Qty,
                  SUM(mt.MatlCost * mt.qty) AS Material,
                  SUM(mt.LbrCost * mt.qty) AS Labour,
                  SUM(mt.FovhdCost * mt.qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.qty) AS Outside
                FROM `mp_infor.material_transaction` mt
                WHERE mt.TransType='B'
                  AND mt.RefType  ='I' 
                GROUP BY mt.item,
                    mt.Whse,
                    mt.Loc,
                    Period
              ) CycleCountInv ON CAST(i.item AS STRING) = CAST(CycleCountInv.Item AS STRING)
                             AND FORMAT_DATE('%Y-%m', mt.TransDate) = CycleCountInv.Period
                             AND mt.Whse = CycleCountInv.Whse
                             AND mt.Loc = CycleCountInv.Loc
    LEFT JOIN (     ----------------------------------------------------------------- Misc.Issue Inventory-----------------------------------------------------------------
               SELECT 
                  mt.Item,
                  mt.Whse,
                  mt.Loc,
                  FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                  SUM(mt.Qty) AS Qty,
                  SUM(mt.MatlCost * mt.Qty) AS Material,
                  SUM(mt.LbrCost * mt.Qty) AS Labour,
                  SUM(mt.FovhdCost * mt.Qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.Qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.Qty) AS Outside
                FROM `mp_infor.material_transaction` mt
                WHERE mt.TransType='G'
                  AND mt.RefType  ='I' 
                GROUP BY mt.item,
                    mt.Whse,
                    mt.Loc,
                    Period
              ) MissIssueInv ON CAST(i.item AS STRING) = CAST(MissIssueInv.Item AS STRING)
                             AND FORMAT_DATE('%Y-%m', mt.TransDate) = MissIssueInv.Period
                             AND mt.Whse = MissIssueInv.Whse
                             AND mt.Loc = MissIssueInv.Loc
                             
    LEFT JOIN (     ----------------------------------------------------------------- Split/Merge/Create Job-----------------------------------------------------------------
               SELECT 
                  mt.Item,
                  mt.Whse,
                  mt.Loc,
                  FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                  SUM(mt.qty) AS Qty,
                  SUM(mt.MatlCost * mt.qty) AS Material,
                  SUM(mt.LbrCost * mt.qty) AS Labour,
                  SUM(mt.FovhdCost * mt.qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.qty) AS Outside
                FROM `mp_infor.material_transaction` mt
                WHERE mt.TransType='C'
                  AND mt.RefType  ='J' 
                GROUP BY mt.item,
                    mt.Whse,
                    mt.Loc,
                    Period
              ) SMCJob ON CAST(i.item AS STRING) = CAST(SMCJob.Item AS STRING)
                             AND FORMAT_DATE('%Y-%m', mt.TransDate) = SMCJob.Period
                             AND mt.Whse = SMCJob.Whse
                             AND mt.Loc = SMCJob.Loc
    LEFT JOIN (     ----------------------------------------------------------------- Adjustment Inventory-----------------------------------------------------------------
               SELECT 
                mt.Item,
                mt.Whse,
                mt.Loc,
                FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                CASE 
                  WHEN SUM(mt.qty) < 0 THEN 0
                  ELSE SUM(mt.qty)
                END AS Qty,
                CASE 
                  WHEN SUM(mt.qty) < 0 THEN 0
                  ELSE SUM(mt.MatlCost * mt.qty)
                END AS Material,
                CASE 
                  WHEN SUM(mt.qty) < 0 THEN 0
                  ELSE SUM(mt.LbrCost * mt.qty)
                END AS Labour,
                CASE 
                  WHEN SUM(mt.qty) < 0 THEN 0
                  ELSE SUM(mt.FovhdCost * mt.qty)
                END AS FixOH,
                CASE 
                  WHEN SUM(mt.qty) < 0 THEN 0
                  ELSE SUM(mt.VovhdCost * mt.qty)
                END AS VariabelOH,
                CASE 
                  WHEN SUM(mt.qty) < 0 THEN 0
                  ELSE SUM(mt.OutCost * mt.qty)
                END AS Outside
              FROM `mp_infor.material_transaction` mt
              WHERE mt.TransType='A'
                AND mt.RefType  ='I' 
              GROUP BY mt.item,
                  mt.Whse,
                  mt.Loc,
                  Period
              ) AdjInv ON CAST(i.item AS STRING) = CAST(AdjInv.Item AS STRING)
                             AND FORMAT_DATE('%Y-%m', mt.TransDate) = AdjInv.Period
                             AND mt.Whse = AdjInv.Whse
                             AND mt.Loc = AdjInv.Loc
      LEFT JOIN (     ----------------------------------------------------------------- Ending Balance-----------------------------------------------------------------
               SELECT 
                  mt.Item,
                  FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                  SUM(mt.qty) AS Qty,
                  SUM(mt.MatlCost * mt.qty) AS Material,
                  SUM(mt.LbrCost * mt.qty) AS Labour,
                  SUM(mt.FovhdCost * mt.qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.qty) AS Outside
                FROM `mp_infor.material_transaction` mt
                WHERE mt.TransType IN ('A', 'B', 'C', 'D', 'F', 'G', 'H', 'I', 'L', 'M', 'N', 'O', 'P', 'R', 'S', 'T', 'W')
                GROUP BY mt.item,
                    Period
              ) ending ON CAST(i.item AS STRING) = CAST(ending.Item AS STRING)
                             AND FORMAT_DATE('%Y-%m', mt.TransDate) = SMCJob.Period

WHERE FORMAT_DATE('%Y-%m', DATE(mt.TransDate)) < '2022-01'
UNION ALL
SELECT
  DISTINCT
  i.item AS Item,
  FORMAT_DATE('%Y-%m', mt.TransDate) AS Periode,
  i.Description,
  i.UM,
  ch.Acct,
  ch.Description AS AcctDescription,
  i.ProductCode,
  pc.Description AS ProductCodeDescription,
  coitem.CustNum AS Customer,
  cust.name AS CustomerName,
  '' AS Warehouse,
  '' AS Location,
  '' AS TransType,
  'x-- Ending Balance --x' AS TransactionType,
  '' AS RefType,
  'Ending Balance' AS ReferenceType,
  ending.Material AS Material,
  ending.Labour AS Labour,
  ending.FixOH AS FixOH,
  ending.VariabelOH AS VariabelOH,
  ending.Outside AS Outside
FROM `mp_infor.items_new` i
  LEFT JOIN `mp_infor.product_codes_BQ` pc ON CAST(i.ProductCode AS STRING) = CAST(pc.ProductCode AS STRING)
  LEFT JOIN `mp_infor.distribution_productcodes` da ON CAST(i.ProductCode AS STRING) = CAST(da.ProductCode AS STRING)
  LEFT JOIN `mp_infor.COA` ch ON CAST(da.InvAcct AS STRING) = CAST(ch.Acct AS STRING)
  LEFT JOIN `mp_infor.COItems` coitem ON CAST(i.item AS STRING) = CAST(coitem.Item AS STRING)
  LEFT JOIN `mp_infor.MP_Customer` cust ON coitem.CustNum = cust.CustNum
  LEFT JOIN `mp_infor.material_transaction` mt ON CAST(i.item AS STRING) = CAST(mt.Item AS STRING)
  LEFT JOIN `mp_infor.material_tran_acct` mta ON CAST(mt.TransNum AS STRING) = CAST(mta.TransNum AS STRING)
  LEFT JOIN (     ----------------------------------------------------------------- Ending Balance-----------------------------------------------------------------
               SELECT 
                  mt.Item,
                  FORMAT_DATE('%Y-%m', mt.TransDate) AS Period,
                  SUM(mt.qty) AS Qty,
                  SUM(mt.MatlCost * mt.qty) AS Material,
                  SUM(mt.LbrCost * mt.qty) AS Labour,
                  SUM(mt.FovhdCost * mt.qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.qty) AS Outside
                FROM `mp_infor.material_transaction` mt
                WHERE mt.TransType IN ('A', 'B', 'C', 'D', 'F', 'G', 'H', 'I', 'L', 'M', 'N', 'O', 'P', 'R', 'S', 'T', 'W')
                GROUP BY mt.item,
                    Period
              ) ending ON CAST(i.item AS STRING) = CAST(ending.Item AS STRING)
                             AND FORMAT_DATE('%Y-%m', mt.TransDate) = ending.Period
WHERE FORMAT_DATE('%Y-%m', DATE(mt.TransDate)) < '2022-01'
UNION ALL
SELECT
  DISTINCT
  i.item AS Item,
  FORMAT_DATE('%Y-%m', DATE(mt.TransDate)) AS Periode,
  i.Description,
  i.UM,
  ch.Acct,
  ch.Description AS AcctDescription,
  i.ProductCode,
  pc.Description AS ProductCodeDescription,
  coitem.CustNum AS Customer,
  cust.name AS CustomerName,
  '' AS Warehouse,
  '' AS Location,
  '' AS TransType,
  '-- Begining Balance --' AS TransactionType,
  '' AS RefType,
  'Begining Balance' AS ReferenceType,
  begining.Material AS Material,
  begining.Labour AS Labour,
  begining.FixOH AS FixOH,
  begining.VariabelOH AS VariabelOH,
  begining.Outside AS Outside
FROM `mp_infor.items_new` i
  LEFT JOIN `mp_infor.product_codes_BQ` pc ON CAST(i.ProductCode AS STRING) = CAST(pc.ProductCode AS STRING)
  LEFT JOIN `mp_infor.distribution_productcodes` da ON CAST(i.ProductCode AS STRING) = CAST(da.ProductCode AS STRING)
  LEFT JOIN `mp_infor.COA` ch ON CAST(da.InvAcct AS STRING) = CAST(ch.Acct AS STRING)
  LEFT JOIN `mp_infor.COItems` coitem ON CAST(i.item AS STRING) = CAST(coitem.Item AS STRING)
  LEFT JOIN `mp_infor.MP_Customer` cust ON coitem.CustNum = cust.CustNum
  LEFT JOIN `mp_infor.material_transaction` mt ON CAST(i.item AS STRING) = CAST(mt.Item AS STRING)
  LEFT JOIN `mp_infor.material_tran_acct` mta ON CAST(mt.TransNum AS STRING) = CAST(mta.TransNum AS STRING)
  LEFT JOIN (
               SELECT 
                  mt.Item,
                  FORMAT_DATE('%Y-%m', DATE(mt.TransDate)) AS Period,
                  SUM(mt.qty) AS Qty,
                  SUM(mt.MatlCost * mt.qty) AS Material,
                  SUM(mt.LbrCost * mt.qty) AS Labour,
                  SUM(mt.FovhdCost * mt.qty) AS FixOH,
                  SUM(mt.VovhdCost * mt.qty) AS VariabelOH,
                  SUM(mt.OutCost * mt.qty) AS Outside
               FROM `mp_infor.material_transaction` mt
               WHERE mt.TransType IN ('A','B','C','D','F','G','H','I','L','M','N','O','P','R','S','T','W')
               GROUP BY mt.Item, Period
             ) begining 
       ON CAST(i.item AS STRING) = CAST(begining.Item AS STRING)
      AND FORMAT_DATE('%Y-%m', DATE_SUB(DATE(mt.TransDate), INTERVAL 1 MONTH)) = begining.Period
WHERE FORMAT_DATE('%Y-%m', DATE(mt.TransDate)) < '2022-01'
