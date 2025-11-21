{{
  config(
    materialized= 'table'
  )
}}

SELECT
  CASE 
    WHEN a.job_location NOT IN ('Jembrana') THEN 'Outsite Jembrana'
    ELSE 'Jembrana' 
  END AS job_location,
  a.ktp,
  a.full_name,
  'Applicant' AS status,
  a.Unprocessedd AS date,
  a.Form,
  a.Gender
FROM {{ source('mp_infor', 'ListUnprocessed') }} a 
WHERE a.Unprocessedd IS NOT NULL 


UNION ALL

SELECT
   CASE 
    WHEN a.job_location NOT IN ('Jembrana') THEN 'Outsite Jembrana'
    ELSE 'Jembrana' 
  END AS job_location,
  a.ktp,
  a.full_name,
  'Interview' AS status,
  a.interview AS date,
  a.Form,
  a.Gender
FROM {{ source('mp_infor', 'ListUnprocessed') }} a
WHERE a.interview IS NOT NULL 

UNION ALL

SELECT
   CASE 
    WHEN a.job_location NOT IN ('Jembrana') THEN 'Outsite Jembrana'
    ELSE 'Jembrana' 
  END AS job_location,
  a.ktp,
  a.full_name,
  'Induction' AS status,
  a.ready_to_post_to_HRIS AS date,
  a.Form,
  a.Gender
FROM {{ source('mp_infor', 'ListUnprocessed') }} a
WHERE a.ready_to_post_to_HRIS IS NOT NULL 


UNION ALL

SELECT
   CASE 
    WHEN a.job_location NOT IN ('Jembrana') THEN 'Outsite Jembrana'
    ELSE 'Jembrana' 
  END AS job_location,
  a.ktp,
  a.full_name,
  'JoinStatus' AS status,
  a.posted_to_HRIS AS date,
  a.Form,
  a.Gender
FROM {{ source('mp_infor', 'ListUnprocessed') }} a
WHERE a.posted_to_HRIS IS NOT NULL 

