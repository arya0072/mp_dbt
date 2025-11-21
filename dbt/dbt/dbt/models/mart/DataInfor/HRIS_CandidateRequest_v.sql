{{
  config(
    materialized= 'table'
  )
}}

 SELECT b.recruitment_request_code,
    b.title,
    b.full_name,
    b.ktp,
    b.approve_date,
    b.hire_date,
    b.request_date,
    b.kondisi_selisih,
    b.jumlah_tahun,
    b.jumlah_bulan,
    b.jumlah_hari,
    b.employee_code,
    b.grade,
    b.expired_date_published,
    b.date_approved_5,
    b.approved_sla_days,
    b.join_date,
    b.job_title,
    b.division,
    b.department,
    b.selisih_hari,
    b.kondisi_selisih_hari,
    b.sla,
    b.sla_days,
        CASE
            WHEN b.selisih_hari <= b.sla_days THEN 'Meet SLA'
            ELSE 'Tidak Meet SLA'
        END AS kondisi
   FROM ( SELECT a.recruitment_request_code,
            a.title,
            a.full_name,
            a.ktp,
            a.approve_date,
            a.hire_date,
            a.request_date,
            a.selisih_hari,
                CASE
                    WHEN a.selisih_hari <= 30 THEN '1 Month'
                    WHEN a.selisih_hari > 30 AND a.selisih_hari <= 60 THEN '2 Month'
                    WHEN a.selisih_hari > 60 AND a.selisih_hari <= 90 THEN '3 Month'
                    ELSE '-'
                END AS kondisi_selisih,
                CASE
                    WHEN a.selisih_hari <= 30 THEN 30
                    WHEN a.selisih_hari > 30 AND a.selisih_hari <= 60 THEN 60
                    WHEN a.selisih_hari > 60 AND a.selisih_hari <= 90 THEN 90
                    ELSE 0
                END AS kondisi_selisih_hari,
            floor(a.selisih_hari / 365) AS jumlah_tahun,
            floor(mod(a.selisih_hari, 365) / 30) AS jumlah_bulan,
            mod(a.selisih_hari, 30) AS jumlah_hari,
            a.employee_code,
            a.grade,
            a.sla,
                CASE
                    WHEN a.sla = '1 Month' THEN 30
                    WHEN a.sla = '2 Month' THEN 60
                    WHEN a.sla = '3 Month' THEN 90
                    ELSE NULL
                END AS sla_days,
            a.expired_date_published,
            a.date_approved_5,
            CASE
              WHEN a.sla = '1 Month' THEN DATE_ADD(COALESCE(a.date_approved_5, a.approve_date), INTERVAL 31 DAY)
              WHEN a.sla = '2 Month' THEN DATE_ADD(COALESCE(a.date_approved_5, a.approve_date), INTERVAL 62 DAY)
              WHEN a.sla = '3 Month' THEN DATE_ADD(COALESCE(a.date_approved_5, a.approve_date), INTERVAL 92 DAY)
              ELSE NULL
            END AS approved_sla_days,
                -- CASE
                --     WHEN a.sla = '1 Month' THEN COALESCE(a.date_approved_5, a.approve_date) + '31 days'
                --     WHEN a.sla = '2 Month' THEN COALESCE(a.date_approved_5, a.approve_date) + '62 days'
                --     WHEN a.sla = '3 Month' THEN COALESCE(a.date_approved_5, a.approve_date) + '92 days'
                --     ELSE NULL
                -- END AS approved_sla_days,
            a.join_date,
            a.job_title,
            a.division,
            a.department
           FROM ( SELECT can_position.recruitment_request_code,
                    can_position.title,
                    can_position.full_name,
                    can_position.ktp,
                    can_position.approve_date,
                    can_position.hire_date,
                    rec_request.request_date,
                    DATE_DIFF(can_position.join_date, can_position.approve_date, DAY) AS selisih_hari,
                    users_all.code as employee_code,
                    can_position.grade,
                    can_position.sla,
                    can_position.sla_days,
                    can_position.expired_date_published,
                    rec_request.date_approved_5,
                    can_position.join_date,
                    can_position.job_title,
                    can_position.division,
                    can_position.department
                   FROM {{ source('mp_infor', 'HRIS_CandidatePosition') }} can_position
                     LEFT JOIN {{ source('mp_infor', 'HRIS_CandidateRecRequest') }} rec_request ON can_position.recruitment_request_code = rec_request.form_number
                     LEFT JOIN {{ source('mp_infor', 'hris_user') }} users_all ON can_position.ktp = users_all.ktp) a) b
  WHERE b.grade IN ('1','2','3','4','5','6','7','8','9','10')
