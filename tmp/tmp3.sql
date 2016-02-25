SELECT
  recipient_phone,
  count(DISTINCT if(order_source <> 3, d_wm_order_uuid, NULL)) AS uuid_num,
  count(DISTINCT user_id)                                      AS userid_num,
  count(DISTINCT d_pay_account)                                AS pacct_num,
  count(DISTINCT a.wm_poi_id)                                  AS poi_num,
  count(DISTINCT b.aor_id),
  count(DISTINCT a.city_id),
  count(a.id)                                                  AS ord_num,
  sum(a.original_price),
  sum(a.d_wm_charge_fee),
  min(a.dt)                                                    AS first_date,
  max(a.dt)                                                    AS last_date,
  count(DISTINCT a.dt)                                         AS date_num,
  count(if(d_first_pur_act = 1, a.id, NULL))                   AS fstpur_num,
  count(DISTINCT if(b.punish_number > 0, a.wm_poi_id, NULL)),
  count(DISTINCT a.recipient_address),
  sum(a.is_phone_diff_city),
  sum(a.is_phone_diff_prov),
  sum(a.is_ip_diff_city),
  sum(a.is_ip_diff_prov),
  sum(if((from_unixtime(a.order_time, 'mm') BETWEEN 10 AND 13) OR (from_unixtime(a.order_time, 'mm') BETWEEN 16 AND 19),
         0, 1)),
  sum(if(a.user_actual_loc_in_sp_area == 0, 1, 0)),
  max(if(COALESCE(c.phone, -1) = -1 OR c.valid = 0, if(coalesce(g.value, -1) = -1 OR c.valid = 0, 0, 2), 1)),
  max(COALESCE(d.max_duration_length, 0)),
  max(e.dm_num),
  max(e.dm_set),
  if(max(coalesce(f.phone, -1)) = -1, 0, 1)
FROM
  mart_waimai_risk.fact_act_base_risk a
  LEFT OUTER JOIN
  mart_waimai_risk.dim_poi_risk b
    ON
      a.wm_poi_id = b.wm_poi_id
  LEFT OUTER JOIN
  mart_waimai_risk.wm_phone_blacklist c
    ON
      a.recipient_phone = c.phone
  LEFT OUTER JOIN
  mart_waimai_risk.risk_ban_activity_confirm g
    ON
      a.recipient_phone = g.value AND g.valid = 1
  LEFT OUTER JOIN
  mart_waimai_risk.dim_phone_continous_risk d
    ON
      a.recipient_phone = d.phone
  LEFT OUTER JOIN
  mart_waimai_risk.aggr_order_detail__phone_risk e
    ON
      a.recipient_phone = e.phone
  LEFT OUTER JOIN
  origin_waimai.waimai_cos__wm_ghb_workorder_qr f
    ON
      a.recipient_phone = f.phone AND f.questionid = 44
WHERE
  a.dt <= '$now.datekey'
GROUP BY
  recipient_phone