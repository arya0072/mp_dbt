{{ 
    config(
    materialized= 'table'
    )
}}

--ACTUAL MP--
SELECT DISTINCT 
  'ACTUAL' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  cop.ue_ItemDesc AS ItemDesc,
  '' AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  '' AS ItemOverview,
  cop.ue_UM AS Um,
  '' AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  '' AS ConvUn,
  '' AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 2) IN ('SM') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
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
  '' AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  cop.ue_Qty as ProdQty,
  0 as ProdQtyChild,
  cop.ue_Qty AS ProdQtyAvg,
  CAST(0 AS FLOAT64) as SalesQtyPcs,
  CAST(0 AS FLOAT64) as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  '' AS CurrCode,
  CAST(0 AS FLOAT64) AS SalesGross,
  CAST(0 AS FLOAT64) as DiscountUnit,
  CAST(0 AS FLOAT64) AS SalesNett,
  CAST(0 AS FLOAT64) AS SalesNettDomestic,
  CAST(0 AS FLOAT64) AS COGSDomestic,
  CAST(0 AS FLOAT64) AS COGSLabourSC,
  CAST(0 AS FLOAT64) AS COGSMaterialSC,
  CAST(0 AS FLOAT64) AS COGSFixOHSC,
  CAST(0 AS FLOAT64) AS COGSVarOHSC,
  CAST(0 AS FLOAT64) AS COGSOutSideSC,
  CAST(0 AS FLOAT64) AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  '' AS MonthYear,
  '' AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy,
  cop.ue_Qty as DetailQty,
  conv.Convertion AS DetailConvFactor
FROM {{ source('mp_infor', 'cost_of_production_mp_new') }} cop 
  LEFT JOIN {{ source('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN {{ ref('conversions_table') }} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType = 'Item'
UNION ALL
SELECT DISTINCT 
  'ACTUAL' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  header.ue_Item AS Item,
  header.ue_ItemDesc AS ItemDesc,
  cop.ue_ItemDesc AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  '' AS ItemOverview,
  cop.ue_UM AS Um,
  '' AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  '' AS ConvUn,
  '' AS ExchRate,
  cop.ue_Job AS Job,
  CASE
    when SUBSTR(cop.ue_ProductCode, 1, 2) IN ('SM') AND cop.ue_ItemType = 'Item' THEN '*Semi Finish Goods'
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
  header.ue_qty AS ProdQty,
  cop.ue_Qty as ProdQtyChild,
  header.ue_qty AS ProdQtyAvg,
  CAST(0 AS FLOAT64) as SalesQtyPcs,
  CAST(0 AS FLOAT64) as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  '' AS CurrCode,
  CAST(0 AS FLOAT64) AS SalesGross,
  CAST(0 AS FLOAT64) as DiscountUnit,
  CAST(0 AS FLOAT64) AS SalesNett,
  CAST(0 AS FLOAT64) AS SalesNettDomestic,
  CAST(0 AS FLOAT64) AS COGSDomestic,
  CAST(0 AS FLOAT64) AS COGSLabourSC,
  CAST(0 AS FLOAT64) AS COGSMaterialSC,
  CAST(0 AS FLOAT64) AS COGSFixOHSC,
  CAST(0 AS FLOAT64) AS COGSVarOHSC,
  CAST(0 AS FLOAT64) AS COGSOutSideSC,
  CAST(0 AS FLOAT64) AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  '' AS MonthYear,
  '' AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy,
  header.ue_Qty as DetailQty,
  detailconv.Convertion AS DetailConvFactor
FROM {{ source('mp_infor', 'cost_of_production_mp_new') }} cop
  LEFT JOIN {{ source('mp_infor', 'cost_of_production_mp_new') }} header ON cop.ue_Job = header.ue_Job 
                                                                         AND header.ue_ItemType = 'Item'
  LEFT JOIN {{ source('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN {{ ref('conversions_table') }} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
  LEFT JOIN {{ ref('conversions_table') }} detailconv ON header.ue_Item = detailconv.Item AND header.ue_UM = detailconv.UOM
WHERE cop.ue_ItemType <> 'Item'
UNION ALL
--END ACTUAL MP--

--ABSORB MP--
SELECT DISTINCT 
  'ABSORB' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  cop.ue_ItemDesc AS ItemDesc,
  '' AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  '' AS ItemOverview,
  cop.ue_UM AS Um,
  '' AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  '' AS ConvUn,
  '' AS ExchRate,
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
  (SELECT DISTINCT MAX(ue_TransDate) FROM `mp_infor.cost_of_production_mp_new` WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  '' AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  cop.ue_Qty as ProdQty,
  0 as ProdQtyChild,
  cop.ue_Qty AS ProdQtyAvg,
  CAST(0 AS FLOAT64) as SalesQtyPcs,
  CAST(0 AS FLOAT64) as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  '' AS CurrCode,
  CAST(0 AS FLOAT64) AS SalesGross,
  CAST(0 AS FLOAT64) as DiscountUnit,
  CAST(0 AS FLOAT64) AS SalesNett,
  CAST(0 AS FLOAT64) AS SalesNettDomestic,
  CAST(0 AS FLOAT64) AS COGSDomestic,
  CAST(0 AS FLOAT64) AS COGSLabourSC,
  CAST(0 AS FLOAT64) AS COGSMaterialSC,
  CAST(0 AS FLOAT64) AS COGSFixOHSC,
  CAST(0 AS FLOAT64) AS COGSVarOHSC,
  CAST(0 AS FLOAT64) AS COGSOutSideSC,
  CAST(0 AS FLOAT64) AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  '' AS MonthYear,
  '' AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy,
  cop.ue_Qty as DetailQty,
  conv.Convertion AS DetailConvFactor
FROM {{ source('mp_infor', 'material_usage_mp_new') }} cop
  LEFT JOIN {{ source('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN {{ ref('conversions_table') }} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType = 'Item'
UNION ALL
SELECT DISTINCT 
  'ABSORB' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  header.ue_Item AS Item,
  header.ue_ItemDesc AS ItemDesc,
  cop.ue_ItemDesc AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  '' AS ItemOverview,
  cop.ue_UM AS Um,
  '' AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  '' AS ConvUn,
  '' AS ExchRate,
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
  (SELECT DISTINCT MAX(ue_TransDate) FROM `mp_infor.cost_of_production_mp_new` WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  cop.ue_Item AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  header.ue_qty AS ProdQty,
  cop.ue_qty as ProdQtyChild,
  header.ue_Qty AS ProdQtyAvg,
  CAST(0 AS FLOAT64) as SalesQtyPcs,
  CAST(0 AS FLOAT64) as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  '' AS CurrCode,
  CAST(0 AS FLOAT64) AS SalesGross,
  CAST(0 AS FLOAT64) as DiscountUnit,
  CAST(0 AS FLOAT64) AS SalesNett,
  CAST(0 AS FLOAT64) AS SalesNettDomestic,
  CAST(0 AS FLOAT64) AS COGSDomestic,
  CAST(0 AS FLOAT64) AS COGSLabourSC,
  CAST(0 AS FLOAT64) AS COGSMaterialSC,
  CAST(0 AS FLOAT64) AS COGSFixOHSC,
  CAST(0 AS FLOAT64) AS COGSVarOHSC,
  CAST(0 AS FLOAT64) AS COGSOutSideSC,
  CAST(0 AS FLOAT64) AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  '' AS MonthYear,
  '' AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy,
  header.ue_Qty as DetailQty,
  detailconv.Convertion AS DetailConvFactor
FROM {{ source('mp_infor', 'material_usage_mp_new') }} cop
  LEFT JOIN {{ source('mp_infor', 'material_usage_mp_new') }} header ON cop.ue_Job = header.ue_Job 
                                                                     AND header.ue_ItemType = 'Item'
  LEFT JOIN {{ source('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum       
  LEFT JOIN {{ ref('conversions_table') }} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
  LEFT JOIN {{ ref('conversions_table') }} detailconv ON header.ue_Item = detailconv.Item AND header.ue_UM = detailconv.UOM                                    
WHERE cop.ue_ItemType <> 'Item'
UNION ALL
--END ABSORB MP--

--PLAN MP--
SELECT DISTINCT 
  'PLAN' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  cop.ue_Item AS Item,
  cop.ue_ItemDesc AS ItemDesc,
  '' AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  '' AS ItemOverview,
  cop.ue_UM AS Um,
  '' AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  '' AS ConvUn,
  '' AS ExchRate,
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
  (SELECT DISTINCT MAX(ue_TransDate) FROM `mp_infor.cost_of_production_mp_new` WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  '' AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  cop.ue_Qty as ProdQty,
  0 as ProdQtyChild,
  cop.ue_Qty AS ProdQtyAvg,
  CAST(0 AS FLOAT64) as SalesQtyPcs,
  CAST(0 AS FLOAT64) as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  '' AS CurrCode,
  CAST(0 AS FLOAT64) AS SalesGross,
  CAST(0 AS FLOAT64) as DiscountUnit,
  CAST(0 AS FLOAT64) AS SalesNett,
  CAST(0 AS FLOAT64) AS SalesNettDomestic,
  CAST(0 AS FLOAT64) AS COGSDomestic,
  CAST(0 AS FLOAT64) AS COGSLabourSC,
  CAST(0 AS FLOAT64) AS COGSMaterialSC,
  CAST(0 AS FLOAT64) AS COGSFixOHSC,
  CAST(0 AS FLOAT64) AS COGSVarOHSC,
  CAST(0 AS FLOAT64) AS COGSOutSideSC,
  CAST(0 AS FLOAT64) AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  '' AS MonthYear,
  '' AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy,
  cop.ue_Qty as DetailQty,
  conv.Convertion AS DetailConvFactor
FROM {{ source('mp_infor', 'cost_of_production_PLN_mp') }} cop
  LEFT JOIN {{ source('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN {{ ref('conversions_table') }} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
WHERE cop.ue_ItemType = 'Item'
UNION ALL
SELECT DISTINCT 
  'PLAN' AS CopType,
  'PROD' AS TransType,
  cop.site AS Site,
  header.ue_Item AS Item,
  header.ue_ItemDesc AS ItemDesc,
  cop.ue_ItemDesc AS ItemChildDesc,
  cop.ue_ItemType AS ItemType,
  '' AS ItemOverview,
  cop.ue_UM AS Um,
  '' AS InvoiceNum,
  conv.Convertion AS ConvFactor,
  '' AS ConvUn,
  '' AS ExchRate,
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
  (SELECT DISTINCT MAX(ue_TransDate) FROM `mp_infor.cost_of_production_mp_new` WHERE ue_Job = cop.ue_job AND ue_OperNum = cop.ue_OperNum AND ue_Suffix = cop.ue_Suffix) as Date,
  cop.ue_Item AS ItemChild,
  cop.ue_CustNum AS CustomerNum,
  cop.ue_CustName AS CustomerName,
  header.ue_qty AS ProdQty,
  cop.ue_qty as ProdQtyChild,
  header.ue_Qty AS ProdQtyAvg,
  CAST(0 AS FLOAT64) as SalesQtyPcs,
  CAST(0 AS FLOAT64) as SalesQty,
  cop.ue_TotalMaterialCost as COPMaterial,
  cop.ue_TotalLaborCost as COPLabor,
  cop.ue_TotalFovhdCost as COPFixOH,
  cop.ue_TotalVovhdCost as COPVarOH,
  cop.ue_OutsideCost AS COPOutSide,
  cop.ue_TotalOutsideCost AS COPTotalOutSide,
  cop.ue_TotalCost AS COPTotal,
  '' AS CurrCode,
  CAST(0 AS FLOAT64) AS SalesGross,
  CAST(0 AS FLOAT64) as DiscountUnit,
  CAST(0 AS FLOAT64) AS SalesNett,
  CAST(0 AS FLOAT64) AS SalesNettDomestic,
  CAST(0 AS FLOAT64) AS COGSDomestic,
  CAST(0 AS FLOAT64) AS COGSLabourSC,
  CAST(0 AS FLOAT64) AS COGSMaterialSC,
  CAST(0 AS FLOAT64) AS COGSFixOHSC,
  CAST(0 AS FLOAT64) AS COGSVarOHSC,
  CAST(0 AS FLOAT64) AS COGSOutSideSC,
  CAST(0 AS FLOAT64) AS COGSTotalSC,
  cop.ue_LaborCost AS LaborCost,
  cop.ue_MaterialCost AS MaterialCost,
  cop.ue_fovhdCost AS FixOHCost,
  cop.ue_OperNum AS OperNum,
  cop.ue_ProductCode AS ProductCode,
  cop.ue_suffix AS Suffix,
  cop.ue_WC AS WorkCenter,
  '' AS MonthYear,
  '' AS Resource,
  jobs.whse AS Warehouse,
  matl.ue_UpdatedBy AS UpdatedBy,
  header.ue_Qty as DetailQty,
  detailconv.Convertion AS DetailConvFactor
FROM {{ source('mp_infor', 'cost_of_production_PLN_mp') }} cop
  LEFT JOIN {{ source('mp_infor', 'cost_of_production_PLN_mp') }} header ON cop.ue_Job = header.ue_Job 
                                                                         AND header.ue_ItemType = 'Item'
  LEFT JOIN {{ source('mp_infor', 'Jobs') }} jobs ON cop.ue_Job = jobs.Job AND cop.ue_Suffix = jobs.Suffix
  LEFT JOIN {{ source('PlantControlGianyarDps', 'JO_PostedBy') }} matl ON cop.ue_Job = matl.RefNum
  LEFT JOIN {{ ref('conversions_table') }} conv ON cop.ue_Item = conv.Item AND cop.ue_UM = conv.UOM
  LEFT JOIN {{ ref('conversions_table') }} detailconv ON header.ue_Item = detailconv.Item AND header.ue_UM = detailconv.UOM  
WHERE cop.ue_ItemType <> 'Item'
UNION ALL
--END PLAN MP--

--SALES--
SELECT 
  '' AS CopType,
  'SALES' as TransType,
  '' AS Site,
  sales.Item AS Item,
  sales.ItemDesc,
    '' AS ItemChildDesc,
  '' AS ItemType,
  sales.overview AS ItemOverview,
  sales.Um,
  sales.InvNum AS InvoiceNum,
  CAST(sales.ConvFactor AS FLOAT64) AS ConvFactor,
  CAST(sales.ConvUn AS STRING) AS ConvUn,
  CAST(sales.ExchRate AS STRING) AS ExchRate,
  '' AS Job,
  '' AS Category,
  '' AS WCDesc,
  sales.ProductcodeDescription as ProdCodeDesc,
  sales.ShipDate as Date,
  '' AS ItemChild,
  sales.CustNum AS CustomerNum,
  sales.CustName AS CustomerName,
  CAST(0 AS FLOAT64) as ProdQty,
  CAST(0 AS FLOAT64) as ProdQtyChild,
  CAST(0 AS FLOAT64) AS ProdQtyAvg,
  sales.QtyPcs as SalesQtyPcs,
  sales.QtyInvoiced as SalesQty,
  CAST(0 AS FLOAT64) as COPMaterial,
  CAST(0 AS FLOAT64) as COPLabor,
  CAST(0 AS FLOAT64) as COPFixOH,
  CAST(0 AS FLOAT64) as COPVarOH,
  CAST(0 AS FLOAT64) AS COPOutSide,
  CAST(0 AS FLOAT64) AS COPTotalOutSide,
  CAST(0 AS FLOAT64) AS COPTotal,
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
  CAST(0 AS FLOAT64) AS LaborCost,
  CAST(0 AS FLOAT64) AS MaterialCost,
  CAST(0 AS FLOAT64) AS FixOHCost,
  '' AS OperNum,
  '' AS ProductCode,
  '' AS Suffix,
  '' AS WorkCenter,
  '' AS MonthYear,
  '' AS Resource,
  '' AS Warehouse,
  '' AS UpdatedBy,
  NULL as DetailQty,
  NULL AS DetailConvFactor
FROM {{ source('mp_infor', 'salestransaction') }} sales

ORDER BY Job, ItemType