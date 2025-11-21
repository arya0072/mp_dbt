{{
  config(
    materialized= 'table'
  )
}}

select
  'FILLED' AS type_unit_price,
    vendor_contract.vendnum,
    vendor_contract.vendaddrname,
    vendor_contract.item,
    vendor_contract.itemdescription,
    vendor_contract.leadtime,
    vendor_contract.rank,
    vendor_contract.standar_order_min,
    vendor_contract.vendor_order_min,
    a.break_qty,
    a.unit_price
FROM {{ source('mp_infor', 'vendorContract') }} vendor_contract
  JOIN {{ source('mp_infor', 'vendorContractPrice') }} a ON vendor_contract.Item = a.Item
where a.unit_price <> 0 OR a.unit_price IS NOT NULL
UNION ALL
select
  'BLANK' AS type_unit_price,
    vendor_contract.vendnum,
    vendor_contract.vendaddrname,
    vendor_contract.item,
    vendor_contract.itemdescription,
    vendor_contract.leadtime,
    vendor_contract.rank,
    vendor_contract.standar_order_min,
    vendor_contract.vendor_order_min,
    a.break_qty,
    a.unit_price
FROM {{ source('mp_infor', 'vendorContract') }} vendor_contract
  JOIN {{ source('mp_infor', 'vendorContractPrice') }} a ON vendor_contract.Item = a.Item
where a.unit_price = 0 OR a.unit_price IS NULL