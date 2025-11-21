{{
  config(
    materialized= 'table'
  )
}}

SELECT
  a.FaNum as asset_number,
  asset_num.id_device,
  a.famUf_MP33_ModelNumber as model_number,
  a.famUf_MP33_SerialNumber as serial_number,
  a.FaDesc as description,
  a.famUf_MP32_LongDesc as long_desctription,
  CASE 
    WHEN a.FaStat= 'A' THEN 'ACTIVE'
    ELSE 'NOT ACTIVE'
  END AS status,
  a.AcqDate as date_acquired,
  a.DateToStartDepr as date_start_depretiation,
  a.InvDate as last_inventory_date,
  a.DisposeDate as disposal_date,
  assets_note.RecordDate as note_record_date,
  department.Description AS department,
  a.Loc AS location,
  a.Tag as tag,
  CASE 
    WHEN a.famUf_MP33_Condition= '1' THEN 'Disposal'
    WHEN a.famUf_MP33_Condition= '2' THEN 'Need To Be Repair'
    WHEN a.famUf_MP33_Condition= '3' THEN 'Good Condition'
    WHEN a.famUf_MP33_Condition= '4' THEN 'Mint Condition'
    ELSE '-'
  END AS condition,
  a.LifeMonths as life_months,
  assets_note.ue_NoteSubject as note_subject,
  assets_note.ue_NoteContent as note_content,
  devices.MACAddress as mac_address,
  devices.Platform as platform,
  devices.OSVersionName as os_version,
  devices.InvDevice_Model as device_model,
  devices.User_Name as username,
  devices.User_Email as user_email,
  devices.InvDevice_Manufacturer as manufacturer,
  CASE 
    WHEN devices.User_Name = '' THEN 'No User'
    ELSE 'User'
  END AS status_no_user,
  devices.InvAndroidBattery_Health,
  devices.InvAndroidBattery_ChargeLevel,
  devices.InvAndroidBattery_Technology,
  devices.InvAndroidBattery_Temperature,
  devices.InvAndroidBattery_Voltage,
  a.famUf_MP33_EmpNum as emp_number
FROM {{ source('mp_infor', 'fix_assets') }}  a
  LEFT JOIN {{ source('mp_infor', 'fix_assets_note') }} assets_note ON a.FaNum = assets_note.FaNum
  LEFT JOIN {{ source('mp_infor', 'm_department') }} department ON a.Dept = department.dept
  LEFT JOIN {{ source('mp_infor', 'miradore_asset_number') }} asset_num ON a.FaNum = asset_num.asset_number
  LEFT JOIN {{ source('mp_infor', 'miradore_devices') }} devices ON asset_num.id_device = devices.id
where  a.famUf_MP33_Condition in ('2','3','4')