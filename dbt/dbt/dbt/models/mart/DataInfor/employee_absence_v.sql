{{
  config(
    materialized= 'table'
  )
}}

SELECT 
a.absence_date,
a.week_number,
a.nik,
a.employee_name,
a.job_title,
a.work_hour,
a.periode,
a.realtime_in,
a.realtime_out,
a.ot_hour_min_break,
a.division,
a.department,
a.unit,
a.section,
a.emp_code,
user_absence.mins_late as minutes_late,
user_absence.mins_work as minutes_work,
user_absence.mins_overtime as minutes_overtime,
a.shift,
a.shift_group,
a.time_in_schedule,
a.time_out_schedule,
a.join_date,
a.leave_date,
a.sub_unit,
a.section_group,
a.is_overtime,
a.ot_type,
a.ot_hour,
a.ot_reasons,
a.day_type,
a.status_approve,
a.tap_in_location,
a.tap_out_location,
a.user_location,
a.id_user_absence,
CASE
    WHEN user_absence.mins_late = 0 THEN 'On Time'
    WHEN user_absence.mins_late > 0 AND user_absence.mins_late <=5 THEN 'In Tolerance'
    WHEN user_absence.mins_late >5 THEN 'Late'
    ELSE 'Early'
  END AS punctuality
FROM {{ source('mp_infor', 'employee_absence') }} a 
  LEFT JOIN {{ source('mp_infor', 'user_absence') }} user_absence ON a.id_user_absence =  user_absence.id_user_absence

