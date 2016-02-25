SELECT
  a.recipient_phone                                        AS recipient_phone,
  sum(b.user_actual_loc_dist_m) / count(a.recipient_phone) AS user_actual_loc_dist_m_mean,
  sum(b.user_poi_actual_dist_m) / count(a.recipient_phone) AS user_poi_actual_dist_m_mean,
  count(b.recipient_phone) as order_num
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
  ) a LEFT JOIN
  (SELECT
     recipient_phone,
     user_poi_actual_dist_m,
     user_actual_loc_dist_m
   FROM mart_waimai_risk.detail_waybill_peisong
   WHERE dt > "20150601") b ON a.recipient_phone = b.recipient_phone
GROUP BY a.recipient_phone

SELECT
  a.recipient_phone                                        AS recipient_phone,
  sum(b.user_actual_loc_dist_m) / count(a.recipient_phone) AS user_actual_loc_dist_m_mean,
  sum(b.user_poi_actual_dist_m) / count(a.recipient_phone) AS user_poi_actual_dist_m_mean
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
  ) a LEFT JOIN
  (SELECT
     recipient_phone,
     user_poi_actual_dist_m,
     user_actual_loc_dist_m
   FROM mart_waimai_risk.detail_waybill_peisong
   WHERE dt > "20150601") b ON a.recipient_phone = b.recipient_phone
GROUP BY a.recipient_phonene