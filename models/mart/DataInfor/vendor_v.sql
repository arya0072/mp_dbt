{{
  config(
    materialized= 'table'
  )
}}

select
    m_vendor.vendnum,
    m_vendor.name,
    m_vendor.vadaddr_1,
    m_vendor.vadcity,
    m_vendor.vadcountry,
    m_vendor.currcode,
    m_vendor.vendtype,
    m_vendor.stat,
    m_vendor.taxregnum2,
    m_vendor.taxcode1,
    m_vendor.venuf_mp15_vendortype,
    m_vendor.deravgontimedelpercent,
    m_vendor.category,
    m_vendor.termscode,
    m_vendor.terdescription,
    m_vendor.externalemailaddr,
    a.reevaluationdate,
    a.status AS status_evaluation
from {{ source('mp_infor', 'm_vendor') }} m_vendor
  LEFT JOIN {{ source('mp_infor', 'vendorEvaluation') }} a ON m_vendor.VendNum = a.VendNum 