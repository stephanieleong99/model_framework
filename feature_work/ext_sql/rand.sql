SELECT
  a.phone AS recipient_phone,
  a.*
FROM
  mart_waimai_risk.dim_user_property_risk a
WHERE
  a.last_date >= '20150901' AND a.punish_status = 0
LIMIT 100000