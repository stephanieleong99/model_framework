-- 提取用户下单距离 特征.
--
SELECT
  a.recipient_phone                        AS recipient_phone,
  wm_poi_id                                AS poi_id,
  sum(b.user_actual_loc_dist_m) / count(1) AS user_actual_loc_dist_m,
  count(b.recipient_phone) as order_num,
  max(a.punish_status)                     AS punish_status
FROM
  (
    SELECT
      DISTINCT
      a.phone AS recipient_phone,
      a.punish_status
    FROM
      mart_waimai_risk.dim_user_property_risk a
    WHERE
      a.last_date >= '20150901' AND a.punish_status = 0 and ord_num > 20
    LIMIT 300000
    UNION ALL
    SELECT
      DISTINCT
      a.phone AS recipient_phone,
      a.punish_status
    FROM
      mart_waimai_risk.dim_user_property_risk a
    WHERE
      a.last_date >= '20150901' AND a.punish_status = 1
  ) a LEFT JOIN
  (SELECT
     recipient_phone,
     wm_poi_id,
     sum(user_actual_loc_dist_m) / count(1) as user_actual_loc_dist_m
   FROM mart_waimai_risk.fact_act_base_risk
   WHERE dt > "20150301" AND user_actual_loc_in_sp_area = 0
   GROUP BY recipient_phone, wm_poi_id) b ON a.recipient_phone = b.recipient_phone
GROUP BY a.recipient_phone, wm_poi_id
having count(b.recipient_phone) > 0
