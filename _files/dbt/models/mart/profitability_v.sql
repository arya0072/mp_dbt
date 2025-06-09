{{ 
    config(
    materialized='table'
    )
}}

--ACTUAL MP--
SELECT 
  'ACTUAL' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  cop.ue_ItemDesc AS ItemDesc,
  NULL AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  NULL AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  cop.ue_TransDate as Date,
  NULL AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  cop.ue_Qty as ProdQty,
  0 as ProdQtyChild,
  cop.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM {{ source ('mp_infor', 'cost_of_production_mp_new')}} cop
  LEFT JOIN {{ source ('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN {{ source('PlantControlGianyarDps', 'conversions_table')}} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType = 'Item'
UNION ALL
SELECT 
  'ACTUAL' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  header.ue_Item AS Item,
  header.ue_ItemDesc AS ItemDesc,
  cop.ue_ItemDesc AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  NULL AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  cop.ue_TransDate as Date,
  cop.ue_Item AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  0 as ProdQty,
  cop.ue_Qty as ProdQtyChild,
  header.ue_qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM {{ source ('mp_infor', 'cost_of_production_mp_new')}} cop
  LEFT JOIN {{ source ('mp_infor', 'cost_of_production_mp_new') }} header ON cop.ue_Job = header.ue_Job 
                                                        AND header.ue_ItemType = 'Item'
  LEFT JOIN {{ source ('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN {{ source('PlantControlGianyarDps', 'conversions_table') }}` conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType <> 'Item'
UNION ALL
--END ACTUAL MP--

--ACTUAL MPKB--
SELECT 
  'ACTUAL' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  cop.ue_ItemDesc AS ItemDesc,
  NULL AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  cop.ue_TransDate as Date,
  NULL AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  cop.ue_Qty as ProdQty,
  0 as ProdQtyChild,
  cop.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM {{ source ('mp_infor', 'cost_of_production_mpkb_new') }} cop
  LEFT JOIN {{ source ('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN {{ source('PlantControlGianyarDps', 'conversions_table') }} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType = 'Item'
UNION ALL
SELECT 
  'ACTUAL' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  header.ue_Item AS Item,
  header.ue_ItemDesc AS ItemDesc,
  cop.ue_ItemDesc AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  cop.ue_TransDate as Date,
  cop.ue_Item AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  0 as ProdQty,
  cop.ue_qty as ProdQtyChild,
  header.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM {{ source ('mp_infor', 'cost_of_production_mpkb_new') }} cop
  LEFT JOIN {{ source ('mp_infor', 'cost_of_production_mpkb_new') }} header ON cop.ue_Job = header.ue_Job 
                                                          AND header.ue_ItemType = 'Item'
  LEFT JOIN {{ source ('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN {{ source('PlantControlGianyarDps', 'conversions_table') }} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType <> 'Item'
UNION ALL
--END ACTUAL MPKB--

--ACTUAL JEMBRANA--
SELECT 
  'ACTUAL' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  cop.ue_ItemDesc AS ItemDesc,
  NULL AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  cop.ue_TransDate as Date,
  NULL AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  cop.ue_Qty as ProdQty,
  0 as ProdQtyChild,
  cop.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM {{ source ('mp_infor', 'cost_of_production_jmbr_new') }} cop
  LEFT JOIN {{ source ('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN {{ source('PlantControlGianyarDps', 'conversions_table') }} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType = 'Item'
UNION ALL
SELECT 
  'ACTUAL' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  header.ue_ItemDesc AS ItemDesc,
  cop.ue_ItemDesc AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  cop.ue_TransDate as Date,
  cop.ue_Item AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  0 as ProdQty,
  cop.ue_qty as ProdQtyChild,
  header.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM {{ source ('mp_infor', 'cost_of_production_jmbr_new') }} cop
  LEFT JOIN {{ source ('mp_infor', 'cost_of_production_jmbr_new') }} header ON cop.ue_Job = header.ue_Job 
                                                          AND header.ue_ItemType = 'Item'
  LEFT JOIN {{ source ('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN {{ source('PlantControlGianyarDps', 'conversions_table') }} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType <> 'Item'
UNION ALL
--END ACTUAL JEMBRANA--

--ABSORB MP--
SELECT 
  'ABSORB' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  cop.ue_ItemDesc AS ItemDesc,
  NULL AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  (SELECT MAX(ue_TransDate) FROM {{ source ('mp_infor', 'cost_of_production_mp_new') }} WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  NULL AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  cop.ue_Qty as ProdQty,
  0 as ProdQtyChild,
  cop.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM {{ source ('mp_infor', 'material_usage_mp_new') }} cop
  LEFT JOIN {{ source ('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN {{ source('PlantControlGianyarDps', 'conversions_table') }} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType = 'Item'
UNION ALL
SELECT 
  'ABSORB' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  header.ue_Item AS Item,
  header.ue_ItemDesc AS ItemDesc,
  cop.ue_ItemDesc AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  (SELECT MAX(ue_TransDate) FROM {{ source ('mp_infor', 'cost_of_production_mp_new') }} WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  cop.ue_Item AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  0 as ProdQty,
  cop.ue_qty as ProdQtyChild,
  header.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM {{ source ('mp_infor', 'material_usage_mp_new') }} cop
  LEFT JOIN {{ source ('mp_infor', 'material_usage_mp_new') }} header ON cop.ue_Job = header.ue_Job 
                                                    AND header.ue_ItemType = 'Item'
  LEFT JOIN {{ source ('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum       
  LEFT JOIN {{ source('PlantControlGianyarDps', 'conversions_table') }} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM                                      
WHERE cop.ue_ItemType <> 'Item'
UNION ALL
--END ABSORB MP--

--ABSORB MPKB--
SELECT 
  'ABSORB' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  cop.ue_ItemDesc AS ItemDesc,
  NULL AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  (SELECT MAX(ue_TransDate) FROM {{ source ('mp_infor', 'cost_of_production_mpkb_new') }} WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  NULL AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  cop.ue_Qty as ProdQty,
  0 as ProdQtyChild,
  cop.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM {{ source ('mp_infor', 'material_usage_mpkb_new') }} cop
  LEFT JOIN {{ source ('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN {{ source('PlantControlGianyarDps', 'conversions_table') }} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType = 'Item'
UNION ALL
SELECT 
  'ABSORB' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  header.ue_Item AS Item,
  header.ue_ItemDesc AS ItemDesc,
  cop.ue_ItemDesc AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  (SELECT MAX(ue_TransDate) FROM {{ source ('mp_infor', 'cost_of_production_mpkb_new') }} WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  cop.ue_Item AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  0 as ProdQty,
  cop.ue_qty as ProdQtyChild,
  header.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM {{ source ('mp_infor', 'material_usage_mpkb_new') }} cop
  LEFT JOIN {{ source ('mp_infor', 'material_usage_mpkb_new') }} header ON cop.ue_Job = header.ue_Job 
                                                      AND header.ue_ItemType = 'Item'
  LEFT JOIN {{ source ('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN {{ source('PlantControlGianyarDps', 'conversions_table') }} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType <> 'Item'
UNION ALL
--END ABSORB MPKB--

--ABSORB JEMBRANA--
SELECT 
  'ABSORB' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  cop.ue_ItemDesc AS ItemDesc,
  NULL AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  (SELECT MAX(ue_TransDate) FROM `mp_infor.cost_of_production_jmbr_new` WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  NULL AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  cop.ue_Qty as ProdQty,
  0 as ProdQtyChild,
  cop.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM `mp_infor.material_usage_jmbr_new` cop
  LEFT JOIN `mp_infor.Jobs` jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.JO_PostedBy` matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.conversions_table ` conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType = 'Item'
UNION ALL
SELECT 
  'ABSORB' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  header.ue_ItemDesc AS ItemDesc,
  cop.ue_ItemDesc AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  (SELECT MAX(ue_TransDate) FROM {{ source ('mp_infor', 'cost_of_production_jmbr_new') }} WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  cop.ue_Item AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  0 as ProdQty,
  cop.ue_qty as ProdQtyChild,
  header.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM `mp_infor.material_usage_jmbr_new` cop
  LEFT JOIN `mp_infor.material_usage_jmbr_new` header ON cop.ue_Job = header.ue_Job 
                                                      AND header.ue_ItemType = 'Item'
  LEFT JOIN `mp_infor.Jobs` jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.JO_PostedBy` matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.conversions_table ` conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType <> 'Item'
UNION ALL
--END ABSORB JEMBRANA--

--PLAN MP--
SELECT 
  'PLAN' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  cop.ue_ItemDesc AS ItemDesc,
  NULL AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  (SELECT MAX(ue_TransDate) FROM `mp_infor.cost_of_production_mp_new` WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  NULL AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  cop.ue_Qty as ProdQty,
  0 as ProdQtyChild,
  cop.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM `mp_infor.cost_of_production_PLN_mp` cop
  LEFT JOIN `mp_infor.Jobs` jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.JO_PostedBy` matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.conversions_table ` conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType = 'Item'
UNION ALL
SELECT 
  'PLAN' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  header.ue_Item AS Item,
  header.ue_ItemDesc AS ItemDesc,
  cop.ue_ItemDesc AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  (SELECT MAX(ue_TransDate) FROM `mp_infor.cost_of_production_mp_new` WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  cop.ue_Item AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  0 as ProdQty,
  cop.ue_qty as ProdQtyChild,
  header.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM `mp_infor.cost_of_production_PLN_mp` cop
  LEFT JOIN `mp_infor.cost_of_production_PLN_mp` header ON cop.ue_Job = header.ue_Job 
                                                        AND header.ue_ItemType = 'Item'
  LEFT JOIN `mp_infor.Jobs` jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.JO_PostedBy` matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.conversions_table ` conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType <> 'Item'
UNION ALL
--END PLAN MP--

--PLAN MPKB--
SELECT 
  'PLAN' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  cop.ue_ItemDesc AS ItemDesc,
  NULL AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  (SELECT MAX(ue_TransDate) FROM `mp_infor.cost_of_production_mpkb_new` WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  NULL AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  cop.ue_Qty as ProdQty,
  0 as ProdQtyChild,
  cop.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM `mp_infor.cost_of_production_PLN_mpkb` cop
  LEFT JOIN `mp_infor.Jobs` jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.JO_PostedBy` matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.conversions_table ` conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType = 'Item'
UNION ALL
SELECT 
  'PLAN' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  header.ue_Item AS Item,
  header.ue_ItemDesc AS ItemDesc,
  cop.ue_ItemDesc AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  (SELECT MAX(ue_TransDate) FROM `mp_infor.cost_of_production_mpkb_new` WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  cop.ue_Item AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  0 as ProdQty,
  cop.ue_qty as ProdQtyChild,
  header.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM `mp_infor.cost_of_production_PLN_mpkb` cop
  LEFT JOIN `mp_infor.cost_of_production_PLN_mpkb` header ON cop.ue_Job = header.ue_Job 
                                                          AND header.ue_ItemType = 'Item'
  LEFT JOIN `mp_infor.Jobs` jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.JO_PostedBy` matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.conversions_table ` conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType <> 'Item'
UNION ALL
--END PLAN MPKB--

--PLAN JEMBRANA--
SELECT 
  'PLAN' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  cop.ue_ItemDesc AS ItemDesc,
  NULL AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  (SELECT MAX(ue_TransDate) FROM `mp_infor.cost_of_production_jmbr_new` WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  NULL AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  cop.ue_Qty as ProdQty,
  0 as ProdQtyChild,
  cop.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM `mp_infor.cost_of_production_PLN_jmbr` cop
  LEFT JOIN `mp_infor.Jobs` jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.JO_PostedBy` matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.conversions_table ` conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType = 'Item'
UNION ALL
SELECT 
  'PLAN' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  header.ue_ItemDesc AS ItemDesc,
  cop.ue_ItemDesc AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  NULL AS ItemOverview,
  cop.ue_UM AS Um,
  NULL AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  NULL AS ConvUn,
  NULL AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SUN') AND cop.ue_ItemType = 'Material' THEN 'Packaging'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Bulk' THEN 'Cones' 
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Consumer Pack' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD','SCN') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Outsource Bulk' THEN 'Cones'  
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 6) IN ('SM-FGD') AND cop.ue_ItemType = 'Item' THEN '*Finished Goods - Sample'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Material' THEN 'Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFT') AND cop.ue_ItemType = 'Item' THEN '*HRF Filter'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Material' THEN 'Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('SFM') AND cop.ue_ItemType = 'Item' THEN '*Paper'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Material' THEN 'Cones'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('UCN') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('PCK') AND cop.ue_ItemType = 'Item' THEN '*Rubber'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('RAW') AND cop.ue_ItemType = 'Material' THEN 'Material'
    when SUBSTR(cop.ue_ProductCode, 1, 3) IN ('FGD') AND cop.ue_ItemType = 'Material' AND cop.ue_WCDesc = 'Packing Filter Tip' THEN 'Filter'
  ELSE 'Labor'
  END AS Category,
  cop.ue_WCDesc as WCDesc,
  cop.ue_ProdCodeDesc as ProdCodeDesc,
  (SELECT MAX(ue_TransDate) FROM `mp_infor.cost_of_production_jmbr_new` WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  cop.ue_Item AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  0 as ProdQty,
  cop.ue_qty as ProdQtyChild,
  header.ue_Qty AS ProdQtyAvg,
  0 as SalesQtyPcs,
  0 as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  NULL AS CurrCode,
  0 AS SalesGross,
  0 as DiscountUnit,
  0 AS SalesNett,
  0 AS SalesNettDomestic,
  0 AS COGSDomestic,
  0 AS COGSLabourSC,
  0 AS COGSMaterialSC,
  0 AS COGSFixOHSC,
  0 AS COGSVarOHSC,
  0 AS COGSOutSideSC,
  0 AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  cop.month_year AS MonthYear,
  NULL AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy
FROM `mp_infor.cost_of_production_PLN_jmbr` cop
  LEFT JOIN `mp_infor.cost_of_production_PLN_jmbr` header ON cop.ue_Job = header.ue_Job 
                                                          AND header.ue_ItemType = 'Item'
  LEFT JOIN `mp_infor.Jobs` jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix   
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.JO_PostedBy` matl ON cop.ue_Job = matl.RefNum 
  LEFT JOIN `mitraprodin-data-warehouse.PlantControlGianyarDps.conversions_table ` conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM                         
WHERE cop.ue_ItemType <> 'Item'
UNION ALL
--END PLAN JEMBRANA--

--SALES--
SELECT
  NULL AS CopType,
  'SALES' as TransType,
  NULL AS Site,
  sales.Item AS Item,
  sales.ItemDesc,
    NULL AS ItemChildDesc,
  NULL AS ItemType,
  sales.overview AS ItemOverview,
  sales.Um,
  sales.InvNum AS InvoiceNum,
  sales.ConvFactor,
  sales.ConvUn,
  sales.ExchRate,
  NULL AS Job,
  NULL AS Category,
  NULL as WCDesc,
  sales.ProductcodeDescription as ProdCodeDesc,
  sales.ShipDate as Date,
  NULL AS ItemChild,
  sales.CustNum AS CustomerNum,
  sales.CustName AS CustomerName,
  0 as ProdQty,
  0 as ProdQtyChild,
  0 AS ProdQtyAvg,
  sales.QtyPcs as SalesQtyPcs,
  sales.QtyInvoiced as SalesQty,
  0 as COPMaterial,
  0 as COPLabor,
  0 as COPFixOH,
  0 as COPVarOH,
  0 AS COPOutSide,
  0 AS COPTotalOutSide,
  0 AS COPTotal,
  sales.CurrCode,
  sales.ExtendedPrice AS SalesGross,
  sales.DiscAmt as DiscountUnit,
  sales.ExtendedNetPrice AS SalesNett,
  sales.DomesticExtendedPrice AS SalesNettDomestic,
  sales.DomesticExtendedCogs AS COGSDomestic,
  sales.CgsLbrTotal AS COGSLabourSC,
  sales.CgsMatlTotal AS COGSMaterialSC,
  sales.CgsFovhdTotal AS COGSFixOHSC,
  sales.CgsVovhdTotal AS COGSVarOHSC,
  sales.CgsOutTotal AS COGSOutSideSC,
  sales.CgsTotal AS COGSTotalSC,
  0 AS LaborCost,
  0 AS MaterialCost,
  0 AS FixOHCost,
  NULL AS OperNum,
  NULL AS ProductCode,
  NULL AS Suffix,
  NULL AS WorkCenter,
  NULL AS MonthYear,
  NULL AS Resource,
  NULL AS Warehouse,
  NULL AS UpdatedBy
FROM `mp_infor.salestransaction` sales

ORDER BY Job, ItemType