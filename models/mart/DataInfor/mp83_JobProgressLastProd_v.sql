{{
  config(
    materialized= 'table'
  )
}}


WITH located_deduped AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ue_Item ORDER BY ue_QtyOnHandPcs DESC) AS rn
    FROM {{ source('mp_infor', 'mp131_stock_all_located') }} 
  ) t
  WHERE rn = 1
)


SELECT
  DISTINCT
  mp83_job_progress.ItDesc,
  stock.ue_Item AS Item,
  mp83_job_progress.ProdCode,
  mp83_job_progress.LastProductionDate,  
  stock.ue_Category,
  stock.ue_CustName,
  stock.ue_Customer,
  stock.ue_Description,
  stock.ue_ProdCode,
  stock.ue_QtyOnHand,
  stock.ue_QtyOnHandPcs,
  stock.ue_QtyOrder,
  stock.ue_QtyOrderPcs,
  stock.ue_QtyPlan,
  stock.ue_QtyPlanPcs,
  stock.ue_RemainQtyOnHand,
  stock.ue_RemainQtyOnHandPcs,
  stock.ue_UM,


  -- Kolom dari located_deduped
  located.ue_ItemOverview,
  located.ue_FG,
  located.ue_Customer AS CustDtl,
  located.ue_CustItem,
  located.ue_CustName AS CustDtlName,
  located.ue_CIDesc,
  located.ue_QtyOnHandPcs AS Located_QtyOnHandPcs,
  located.ue_EstimateFGSC


FROM {{ source('mp_infor', 'mp131_stock_co') }} stock


-- Join dengan last production date
LEFT JOIN (
  SELECT
    a.ItDesc,
    a.Item,
    a.ProdCode,
    a.StartDate AS LastProductionDate
  FROM {{ source('mp_infor', 'mp83_joborder_progress') }} a
  JOIN (
    SELECT
      Item,
      MAX(StartDate) AS start_date
    FROM {{ source('mp_infor', 'mp83_joborder_progress') }} 
    GROUP BY Item
  ) max_startdate
    ON a.Item = max_startdate.Item
    AND a.StartDate = max_startdate.start_date
) mp83_job_progress
  ON mp83_job_progress.Item = stock.ue_Item


-- Join dengan located yang sudah dide-duplicate
LEFT JOIN located_deduped located
  ON stock.ue_Item = located.ue_Item
  AND stock.ue_Category = 'SFG'


ORDER BY stock.ue_Item