{{
  config(
    materialized= 'table'
  )
}}

SELECT
  a.UserId,
  a.Username,
  a.EditLevel,
  a.SuperUserFlag,
  a.UserDesc,
  a.Status,   
  b.GroupId,
  b.GroupName,
  b.GroupDesc,
  c.ObjectName1,
  c.ObjectType,
  'Form' AS ObjectTypeName,
  c.BulkUpdatePrivilege,
  c.DeletePrivilege,
  c.EditPrivilege,
  c.ExecutePrivilege,
  c.InsertPrivilege,
  c.ReadPrivilege,
  c.UpdatePrivilege,
  c.UserFlag,
  'Users' AS UserFlagName,
  d.FullName AS Employee,
  d.Department,
  d.Unit,
  d.Position,
  d.JobTitle
FROM {{ source('mp_infor', 'Infor_User') }} a
  LEFT JOIN {{ source('mp_infor', 'Infor_UserGroup') }} b ON a.UserId = b.UserId
  LEFT JOIN {{ source('mp_infor', 'Infor_UserAuth') }} c ON a.UserId = c.id
  LEFT JOIN {{ source('mp_infor', 'hris_user') }} d ON a.Username = d.EmailProdin
WHERE c.ObjectType = '0'
AND c.UserFlag = '1'
UNION ALL
SELECT
  a.UserId,
  a.Username,
  a.EditLevel,
  a.SuperUserFlag,
  a.UserDesc,
  a.Status,   
  b.GroupId,
  b.GroupName,
  b.GroupDesc,
  c.ObjectName1,
  c.ObjectType,
  'Form' AS ObjectTypeName,
  c.BulkUpdatePrivilege,
  c.DeletePrivilege,
  c.EditPrivilege,
  c.ExecutePrivilege,
  c.InsertPrivilege,
  c.ReadPrivilege,
  c.UpdatePrivilege,
  c.UserFlag,
  'Groups' AS UserFlagName,
   d.FullName AS Employee,
  d.Department,
  d.Unit,
  d.Position,
  d.JobTitle
FROM {{ source('mp_infor', 'Infor_User') }} a
  LEFT JOIN {{ source('mp_infor', 'Infor_UserGroup') }} b ON a.UserId = b.UserId
  LEFT JOIN {{ source('mp_infor', 'Infor_UserAuth') }} c ON b.GroupId = c.id
  LEFT JOIN {{ source('mp_infor', 'hris_user') }} d ON a.Username = d.EmailProdin
WHERE c.ObjectType = '0'
AND c.UserFlag = '0'