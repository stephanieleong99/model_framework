SELECT
  *
FROM
  (
    SELECT
      a.phone AS recipient_phone,
      a.*
    FROM
      mart_waimai_risk.dim_user_property_risk a
    WHERE
      a.last_date >= '20150901' AND a.punish_status = 0
    LIMIT 100000
    UNION ALL
    SELECT
      a.phone AS recipient_phone,
      a.*
    FROM
      mart_waimai_risk.dim_user_property_risk a
    WHERE
      a.last_date >= '20150901' AND a.punish_status = 1
  ) a
  LEFT JOIN
  (
    SELECT *
    FROM test.test_dj
    WHERE dt = "20160120") b
    ON
      a.recipient_phone = b.recipient_phone
