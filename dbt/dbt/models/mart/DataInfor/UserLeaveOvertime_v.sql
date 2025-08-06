{{
  config(
    materialized= 'table'
  )
}}

SELECT
  a.id,
  a.id_m_user,
  a.NIK,
  a.employee_name,
  a.id_m_leave_type_group,
  a.LeaveTypeGroup,
  a.leave_type,
  a.note,
  a.StatusApproved,
  a.leave_date,
  a.month_period,
  user.Division,
  user.Department,
  user.Unit,
  user.SubUnit,
  user.JobTitle,
  user.Code,
  user.Position,
  user.Gender,
  user.Location,
  a.start_time,
  a.end_time,
      TIMESTAMP_DIFF(
    SAFE.PARSE_TIMESTAMP('%F %H:%M', CONCAT('1970-01-01 ', a.end_time)),
    SAFE.PARSE_TIMESTAMP('%F %H:%M', CONCAT('1970-01-01 ', a.start_time)),
    MINUTE
  ) / 60.0 AS TotalJamLembur
FROM {{ source('mp_infor', 'userleave_detail') }} a
  LEFT JOIN {{ source('mp_infor', 'hris_user') }} user ON a.id_m_user = user.id_m_user
WHERE a.id_m_leave_type_group = 5