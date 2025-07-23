{{
  config(
    materialized= 'table'
  )
}}

WITH FirstPurchase AS (
  SELECT
    PoItem AS Item,
    VenaddName AS Vendor,
    MAX(RcvdDate) AS FirstRcvdDate
  FROM {{ source('mp_infor', 'PO_Receipt') }}
  WHERE EXTRACT(YEAR FROM RcvdDate) = 2025
  GROUP BY PoItem, VenaddName
),

FirstPurchasePrice AS (
  SELECT
    a.PoItem AS Item,
    a.VenaddName AS Vendor,
    a.RcvdDate,
    a.DerItemCostConv,
    COALESCE(a.DerTotalCostConv, 0) / NULLIF(COALESCE(a.DerQtyReceivedConv, 0), 0) AS FirstActualPrice
  FROM {{ source('mp_infor', 'PO_Receipt') }} a
  JOIN FirstPurchase f
    ON a.PoItem = f.Item
    AND a.VenaddName = f.Vendor
    AND a.RcvdDate = f.FirstRcvdDate
)

SELECT
  a.Item,
  a.ProductcodeDescription,
  a.UM,
  a.Customer_Code,
  b.PoiDescription AS ItemDescription,
  b.PoiUM,
  b.RcvdDate,
  b.VenaddName,
  b.PoCurrCode,
  b.ExchRate,
  COALESCE(b.PoiQtyOrdered, 0) AS PoiQtyOrdered,
  COALESCE(b.PoiQtyOrderedConv, 0) AS PoiQtyOrderedConv,
  COALESCE(b.DerQtyReceivedConv, 0) AS DerQtyReceivedConv,
  COALESCE(b.DerTotalCostConv, 0) AS DerTotalCostConv,
  COALESCE(c.ConvFactor, 0) AS ConvFactor,
  COALESCE(d.itmUf_MP123_StandartCost, 0) AS StandartCost,

  -- Harga tahun sebelumnya, fallback ke harga pembelian pertama jika tidak ada standar harga
  AVG(COALESCE(stdPrice.StandartPrice, first.FirstActualPrice, 0)) AS Last_Year_Price,

  b.PoNum,
  AVG(COALESCE(stdPrice.StandartPrice, 0)) AS StandartPrice,

  -- Harga aktual
  AVG(CASE
    WHEN COALESCE(b.DerQtyReceivedConv, 0) = 0 THEN 0
    ELSE COALESCE(b.DerTotalCostConv, 0) / b.DerQtyReceivedConv
  END) AS ActualPrice,

  -- Variance per unit
  AVG(COALESCE(stdPrice.StandartPrice, first.FirstActualPrice, 0)) - 
  AVG(CASE
    WHEN COALESCE(b.DerQtyReceivedConv, 0) = 0 THEN 0
    ELSE COALESCE(b.DerTotalCostConv, 0) / b.DerQtyReceivedConv
  END) AS PriceVariance,

  -- Variance total
  (COALESCE(b.DerQtyReceivedConv, 0)) * (AVG(COALESCE(stdPrice.StandartPrice, first.FirstActualPrice, 0)) - 
  AVG(CASE
    WHEN COALESCE(b.DerQtyReceivedConv, 0) = 0 THEN 0
    ELSE COALESCE(b.DerTotalCostConv, 0) / b.DerQtyReceivedConv
  END)
  ) AS TotalVariance

FROM {{ ref('item_productcode_v') }} a
  LEFT JOIN {{ source('mp_infor', 'PO_Receipt') }} b ON a.Item = b.PoItem
  LEFT JOIN {{ source('mp_infor', 'uom_conversion') }} c ON a.Item = c.Item AND a.UM = c.ToUM
  LEFT JOIN {{ source('mp_infor', 'StandartCost') }} d ON a.Item = d.Item
  LEFT JOIN (
  SELECT
    PoItem AS Item,
    VenaddName AS Vendor,
    SUM(DerTotalCostConv) / NULLIF(SUM(DerQtyReceivedConv), 0) AS StandartPrice
  FROM {{ source('mp_infor', 'PO_Receipt') }} 
  WHERE RcvdDate BETWEEN '2024-01-01' AND '2024-12-31'
  GROUP BY PoItem, VenaddName
) stdPrice
  ON a.Item = stdPrice.Item AND b.VenaddName = stdPrice.Vendor

LEFT JOIN FirstPurchasePrice first ON a.Item = first.Item AND b.VenaddName = first.Vendor

-- WHERE b.PoNum = 'PO-0005443'

GROUP BY
  a.Item,
  a.ProductcodeDescription,
  a.UM,
  a.Customer_Code,
  b.PoiDescription,
  b.PoiUM,
  b.RcvdDate,
  b.VenaddName,
  b.PoCurrCode,
  b.ExchRate,
  b.PoiQtyOrdered,
  b.PoiQtyOrderedConv,
  b.DerQtyReceivedConv,
  b.DerTotalCostConv,
  c.ConvFactor,
  d.itmUf_MP123_StandartCost,
  stdPrice.StandartPrice,
  first.FirstActualPrice,
  b.PoNum
