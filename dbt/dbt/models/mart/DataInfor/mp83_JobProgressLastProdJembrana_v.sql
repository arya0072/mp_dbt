{{
  config(
    materialized= 'table'
  )
}}

SELECT
  DISTINCT
  mp83_job_progress.ItDesc,
  mp131_stock_co.ue_Item as Item,
  mp83_job_progress.ProdCode,
  mp83_job_progress.LastProductionDate,  
  mp131_stock_co.ue_Category,
  mp131_stock_co.ue_CustName,
  mp131_stock_co.ue_Customer,
  mp131_stock_co.ue_Description,
  mp131_stock_co.ue_ProdCode,
  mp131_stock_co.ue_QtyOnHand,
  mp131_stock_co.ue_QtyOnHandPcs,
  mp131_stock_co.ue_QtyOrder,
  mp131_stock_co.ue_QtyOrderPcs,
  mp131_stock_co.ue_QtyPlan,
  mp131_stock_co.ue_QtyPlanPcs,
  mp131_stock_co.ue_RemainQtyOnHand,
  mp131_stock_co.ue_RemainQtyOnHandPcs,
  mp131_stock_co.ue_UM
FROM {{ source('mp_infor', 'mp131_stock_co') }} mp131_stock_co
  LEFT JOIN (SELECT
                a.ItDesc,
                a.Item,
                a.ProdCode,
                a.StartDate as LastProductionDate
            FROM {{ source('mp_infor', 'mp83_joborder_progress_jemb') }} a
              JOIN (SELECT
                      Item,
                      max(StartDate) as start_date
                    FROM {{ source('mp_infor', 'mp83_joborder_progress_jemb') }}
                    group by Item) max_startdate ON a.Item = max_startdate.Item
                                                AND a.StartDate = max_startdate.start_date  
            ) mp83_job_progress ON mp83_job_progress.Item = mp131_stock_co.ue_Item
-- WHERE mp131_stock_co.ue_Item='900500234'
