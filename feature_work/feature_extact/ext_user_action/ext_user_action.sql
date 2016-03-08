SELECT

  --     sum(a.mean_time_before_submit) / count(a.uuid),
  --     sum(a.mean_visit_pois_before_submit) / count(a.uuid),
  --     sum(a.mean_search_times) / count(a.uuid),
  c.phone                                      AS phone,
  sum(b.payed_arrived_rate) / count(b.uuid)    AS payed_arrived_rate,
  sum(b.in_scope_rate) / count(b.uuid)         AS in_scope_rate,
  sum(b.mean_dis) / count(b.uuid)              AS mean_dis,
  sum(b.aor_id_num) / count(b.uuid)            AS aor_id_num,
  sum(b.recipient_address_num) / count(b.uuid) AS recipient_address_num,
  sum(b.order_num) / count(b.uuid)             AS fei_order_num,
  max(d.is_1hour2ord)                             is_1hour2ord,
  max(d.1hour2ord_days)                           1hour2ord_days,
  max(d.is_7day14ord)                             is_7day14ord,
  max(d.max_continue_order_num)                   max_continue_order_num,
  max(d.all_order_ratio)                          all_order_ratio,
  max(d.all_time_before_120s)                     all_time_before_120s,
  max(e.ord_num)                                  ord_num,
  max(e.act_cost)                                 act_cost,
  max(e.punish_poi_num)                           punish_poi_num,
  max(e.addr_num)                                 addr_num,
  max(e.none_high_cnt)                            none_high_cnt,
  max(e.max_duration_length)                      max_duration_length,
  max(e.dm_num)                                   dm_num,
  max(e.punish_status)                            punish_status
FROM
  --   获取用户行为
  --   (SELECT
  --      uuid                                     AS uuid,
  --      sum(time_before_submit) / count(1)       AS mean_time_before_submit,
  --      sum(visit_pois_before_submit) / count(1) AS mean_visit_pois_before_submit,
  --      sum(search_times) / count(1)             AS mean_search_times
  --    FROM mart_waimai_risk.dim_user_action
  --    WHERE dt > 20150601
  --    GROUP BY uuid) a
  --   -- 支付方式,配送,
  --   LEFT JOIN
  (
    SELECT
      d_wm_order_uuid                            AS uuid,
      sum(wm_order_pay_type) / (count(1)*2)          AS payed_arrived_rate,
      -- 货到付款占比
      sum(user_actual_loc_in_sp_area) / count(1) AS in_scope_rate,
      sum(user_actual_loc_dist_m) / count(1)     AS mean_dis,
      count(DISTINCT (aor_id))                   AS aor_id_num,
      count(DISTINCT (recipient_address))        AS recipient_address_num,
      count(1)                                   AS order_num
    FROM mart_waimai_risk.fact_ord_submitted_risk
    WHERE dt > "20150601"
    GROUP BY d_wm_order_uuid

  ) b
  -- ON a.uuid = b.d_wm_order_uuid
  INNER JOIN
  (
    SELECT
      uuid,
      phone
    FROM test.dim_user_relation__uuid_phone__line) c ON c.uuid = b.uuid
  LEFT JOIN
  (
    SELECT
      recipient_phone,
      is_1hour2ord,
      1hour2ord_days,
      is_7day14ord,
      max_continue_order_num,
      all_order_ratio,
      all_time_before_120s
    FROM mart_waimai_risk.dim_risk_user__bayes_validate_data
  ) d ON c.phone = d.recipient_phone
  LEFT JOIN (SELECT
               phone,
               ord_num,
               act_cost,
               punish_poi_num,
               addr_num,
               none_high_cnt,
               punish_status,
               max_duration_length,
               dm_num
             FROM mart_waimai_risk.dim_user_property_risk
             WHERE ord_num > 10) e ON e.phone = c.phone
GROUP BY c.phone



phone string COMMENT '封禁用户',
payed_arrived_rate double COMMENT '货到付款占比',
in_scope_rate double COMMENT '是否在配送范围 占比',
mean_dis double COMMENT '平均配送距离',
aor_id_num int COMMENT '蜂窝数目',
recipient_address_num int COMMENT '接收地址数目',
order_num int COMMENT '总订单数量',
is_1hour2ord int COMMENT '是否一小时两单',
1hour2ord_days int COMMENT '一小时两单天数',
is_7day14ord int COMMENT '是否7天14单',
max_continue_order_num int COMMENT '最长联续单数',
all_order_ratio double COMMENT '访购率',
all_time_before_120s int COMMENT '提单前120s占比',
ord_num int COMMENT '活动订单数量',
act_cost int COMMENT '实际话费',
punish_poi_num int COMMENT '处罚商家数目',
addr_num int COMMENT '地址数量',
none_high_cnt double COMMENT '非高峰期下单数量',
max_duration_length double COMMENT '最大持续时间',
dm_num int COMMENT '机型数量',
punish_status int COMMENT '是否封禁'