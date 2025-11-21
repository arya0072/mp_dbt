{{
  config(
    materialized= 'table'
  )
}}

SELECT distinct
  Date(a.Unprocessedd) as Apply_Date,
  Date(a.interview) as Interview_Date,
  Date(a.ready_to_post_to_HRIS) as EmailJoin_Date,
  DAte(a.posted_to_HRIS) as Join_Date,
  a.ktp,
  a.full_name,
  a.regency,
  a.district,
  a.village,
  a.age,
  CASE 
    WHEN EXTRACT(YEAR FROM a.Unprocessedd) = 2024 AND a.candidate_location = 'Jembrana' Then 'Jembrana'
    -- WHEN EXTRACT(YEAR FROM a.Unprocessedd) = 2025 AND a.Job_location = 'Jembrana' Then 'Jembrana'
    WHEN EXTRACT(YEAR FROM a.Unprocessedd) = 2025 AND a.candidate_location = 'Jembrana' Then 'Jembrana'
    WHEN EXTRACT(YEAR FROM a.Unprocessedd) = 2025 AND a.candidate_location IS null AND a.Job_location = 'Jembrana'  Then 'Jembrana'
    ELSE 'Gianyar'
  END AS location,
  a.candidate_location,
  a.Job_location,
  a.reject,
  a.media,
  a.referal,
  CASE
    When a.position = 'Peserta Magang'  THEN 'Applicant'
    When a.position = 'Rolling Girl - Cones' THEN 'Applicant'
    When a.position = 'Rolling Girl - Filter' THEN 'Applicant'
    When a.position = 'Glue Girl' THEN 'Applicant'
    When a.position = 'Control Girl' THEN 'Applicant'
   Else 'Not Identify'
 End as Applicant
FROM {{ source('mp_infor', 'ListUnprocessed') }} a
  JOIN (SELECT 
        ktp, 
        MAX(Unprocessedd) AS Apply_Date
    FROM {{ source('mp_infor', 'ListUnprocessed') }}
    GROUP BY ktp) max_unprocessed ON a.ktp = max_unprocessed.ktp 
                                  AND a.Unprocessedd = max_unprocessed.Apply_Date
-- where a.ktp = '3318217107840002'