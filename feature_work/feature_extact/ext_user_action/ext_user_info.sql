SELECT

  --     sum(a.mean_time_before_submit) / count(a.uuid),
  --     sum(a.mean_visit_pois_before_submit) / count(a.uuid),
  --     sum(a.mean_search_times) / count(a.uuid),
  b.recipient_phone             AS phone,
  sum(b.payed_arrived_rate)     AS payed_arrived_rate,
  sum(b.in_scope_rate)          AS in_scope_rate,
  sum(b.mean_dis)               AS mean_dis,
  sum(b.aor_id_num)             AS aor_id_num,
  sum(b.recipient_address_num)  AS recipient_address_num,
  sum(b.order_num)              AS fei_order_num,
  sum(d.food_continus_days)     AS food_continus_days,
  sum(d.7day14ord_days)         AS 7day14ord_days,
  sum(d.outofrange_days)        AS outofrange_days,
  sum(d.max_continue_order_num) AS max_continue_order_num,
  max(d.all_order_ratio)        AS all_order_ratio,
  sum(d.aver_order)             AS aver_order,
  max(d.start_order_ratio)      AS start_order_ratio,
  max(d.start_time_before_120s) AS start_time_before_120s,
  max(d.all_time_before_120s)   AS all_time_before_120s,
  sum(e.ord_num)                   act_ord_num,
  sum(e.act_cost)                  act_cost,
  sum(e.punish_poi_num)            punish_poi_num,
  sum(e.addr_num)                  addr_num,
  sum(e.none_high_cnt)             none_high_cnt,
  sum(e.max_duration_length)       max_duration_length,
  sum(e.dm_num)                    dm_num,
  max(e.punish_status)             punish_status
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
      recipient_phone                            AS recipient_phone,
      sum(wm_order_pay_type) / (count(1) * 2)    AS payed_arrived_rate,
      -- 货到付款占比
      sum(user_actual_loc_in_sp_area) / count(1) AS in_scope_rate,
      sum(CASE WHEN user_actual_loc_dist_m < 1000000 AND user_actual_loc_dist_m > 0
        THEN user_actual_loc_dist_m
          ELSE 0 END) / sum(CASE WHEN user_actual_loc_dist_m < 1000000 AND user_actual_loc_dist_m > 0
        THEN 1
                            ELSE 0 END)          AS mean_dis,
      count(DISTINCT (aor_id))                   AS aor_id_num,
      count(DISTINCT (recipient_address))        AS recipient_address_num,
      count(1)                                   AS order_num
    FROM mart_waimai_risk.fact_ord_submitted_risk
    WHERE dt > "20150601"
    GROUP BY recipient_phone
    --     LIMIT 500

  ) b
  -- ON a.uuid = b.d_wm_order_uuid
  LEFT JOIN
  (
    SELECT
      b.recipient_phone,
      max(a.food_continus_days)     AS food_continus_days,
      max(a.1hour2ord_days)         AS 1hour2ord_days,
      max(a.7day14ord_days)         AS 7day14ord_days,
      max(a.outofrange_days)        AS outofrange_days,
      max(a.max_continue_order_num) AS max_continue_order_num,
      max(a.all_order_ratio)        AS all_order_ratio,
      max(a.aver_order)             AS aver_order,
      max(a.all_time_before_120s)   AS all_time_before_120s,
      max(a.start_time_before_120s) AS start_time_before_120s,
      max(a.start_order_ratio)      AS start_order_ratio
    FROM
      mart_waimai_risk.aggr_dapan_analysis__usr_risk a
      INNER JOIN
      (
        SELECT
          user_id,
          phone recipient_phone
        FROM
          test.dim_user_relation__userid_phone__line
      ) b
        ON
          a.user_id = b.user_id
    GROUP BY
      b.recipient_phone
  ) d ON b.recipient_phone = d.recipient_phone
  INNER JOIN
  (SELECT
     phone,
     ord_num,
     act_cost,
     punish_poi_num,
     addr_num,
     none_high_cnt,
     punish_status,
     max_duration_length,
     dm_num,
     last_date
   FROM mart_waimai_risk.dim_user_property_risk
   WHERE ord_num > 10 ) e ON e.phone = b.recipient_phone
GROUP BY b.recipient_phone
LIMIT 50000



phone string COMMENT '封禁用户',
payed_arrived_rate DOUBLE COMMENT '货到付款占比',
in_scope_rate DOUBLE COMMENT '是否在配送范围 占比',
mean_dis DOUBLE COMMENT '平均配送距离',
aor_id_num INT COMMENT '蜂窝数目',
recipient_address_num INT COMMENT '接收地址数目',
order_num INT COMMENT '总订单数量',
is_1hour2ord INT COMMENT '是否一小时两单',
1hour2ord_days INT COMMENT '一小时两单天数',
is_7day14ord INT COMMENT '是否7天14单',
max_continue_order_num INT COMMENT '最长联续单数',
all_order_ratio DOUBLE COMMENT '访购率',
all_time_before_120s INT COMMENT '提单前120s占比',
ord_num INT COMMENT '活动订单数量',
act_cost INT COMMENT '实际话费',
punish_poi_num INT COMMENT '处罚商家数目',
addr_num INT COMMENT '地址数量',
none_high_cnt DOUBLE COMMENT '非高峰期下单数量',
max_duration_length DOUBLE COMMENT '最大持续时间',
dm_num INT COMMENT '机型数量',
punish_status INT COMMENT '是否封禁'