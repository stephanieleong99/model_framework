SELECT

  --     sum(a.mean_time_before_submit) / count(a.uuid),
  --     sum(a.mean_visit_pois_before_submit) / count(a.uuid),
  --     sum(a.mean_search_times) / count(a.uuid),
  b.recipient_phone                                                   AS phone,
  max(a.day_total_order_num)                                          AS day_total_order_num,
  max(a.mean_order_mean_jiange)                                       AS mean_order_mean_jiange,
  max(a.mean_time_before_submit)                                      AS mean_time_before_submit,
  max(a.mean_search_times)                                            AS mean_search_times,
  max(a.mean_visit_pois_before_submit)                                AS mean_visit_pois_before_submit,
  max(mean_is_use_envelope)                                           AS mean_is_use_envelope,
  max(mean_preview_times)                                             AS mean_preview_times,
  max(mean_check_comment_times)                                       AS mean_check_comment_times,
  max(mean_comment_times_bf_submit)                                   AS mean_comment_times_bf_submit,
  max(mean_has_check_orders)                                          AS mean_has_check_orders,
  max(mean_in_poi_with_orderlist)                                     AS mean_in_poi_with_orderlist,
  max(mean_hmean_as_share_envelope)                                   AS mean_hmean_as_share_envelope,
  max(mean_comment_times_af_submit)                                   AS mean_comment_times_af_submit,
  max(mean_time_after_submit)                                         AS mean_time_after_submit,
  max(bb.fir_one_poi_rate)                                            AS fir_one_poi_rate,
  max(bb.sec_one_poi_rate)                                            AS sec_one_poi_rate,
  max(bc.order_behave_rate)                                           AS order_behave_rate,
  max(bd.user_suggest_type)                                           AS user_suggest_type,
  sum(b.online_pay_rate)                                              AS online_pay_rate,
  sum(b.in_scope_rate)                                                AS in_scope_rate,
  sum(b.mean_dis)                                                     AS mean_dis,
  sum(b.aor_id_num)                                                   AS aor_id_num,
  sum(b.recipient_address_num)                                        AS recipient_address_num,
  sum(b.order_num)                                                    AS fei_order_num,
  sum(d.food_continus_days)                                           AS food_continus_days,
  sum(d.7day14ord_days)                                               AS 7day14ord_days,
  sum(d.outofrange_days)                                              AS outofrange_days,
  sum(d.max_continue_order_num)                                       AS max_continue_order_num,
  max(d.all_order_ratio)                                              AS all_order_ratio,
  sum(d.aver_order)                                                   AS aver_order,
  max(d.start_order_ratio)                                            AS start_order_ratio,
  max(d.start_time_before_120s)                                       AS start_time_before_120s,
  max(d.all_time_before_120s)                                         AS all_time_before_120s,
  sum(e.ord_num)                                                      AS act_ord_num,
  sum(e.act_cost_rate) / count(DISTINCT b.recipient_phone)            AS act_cost_per_ord,
  sum(e.punish_poi_num)                                               AS punish_poi_num,
  sum(e.addr_num)                                                     AS addr_num,
  sum(e.none_high_cnt_rate) / count(DISTINCT b.recipient_phone)       AS none_high_cnt,
  sum(e.max_duration_length)                                          AS max_duration_length,
  sum(e.dm_num)                                                       AS dm_num,
  concat_ws(',', collect_set(cast(dm_set AS string)))                 AS dm_set,
  sum(out_range_ord_cnt_act_rate) / count(DISTINCT b.recipient_phone) AS out_range_ord_cnt_act_rate,
  max(e.last_date)                                                    AS last_date,
  avg(mean_punish_period_ord_rate)                                    AS mean_punish_period_ord_rate,
  avg(mean_punish_poi_ord_rate)                                       AS mean_punish_poi_ord_rate,
  max(neg_type)                                                       AS neg_type,
  max(e.celue_source)                                                 AS celue_source,
  max(e.punish_poi_ord)                                            AS punish_poi_ord,
  max(e.punish_status)                                                AS punish_status
FROM
  --  用户行为特征
  (SELECT
     b.recipient_phone                                               recipient_phone,
     sum(a.day_total_order_num) / count(b.recipient_phone)           day_total_order_num,
     sum(abs(a.mean_order_mean_jiange)) / count(b.recipient_phone)   mean_order_mean_jiange,
     sum(a.mean_time_before_submit) / count(b.recipient_phone)       mean_time_before_submit,
     sum(a.mean_search_times) / count(b.recipient_phone)             mean_search_times,
     sum(a.mean_visit_pois_before_submit) / count(b.recipient_phone) mean_visit_pois_before_submit,
     sum(a.is_use_envelope) / count(b.recipient_phone)         AS    mean_is_use_envelope,
     sum(a.preview_times) / count(b.recipient_phone)           AS    mean_preview_times,
     sum(a.check_comment_times) / count(b.recipient_phone)     AS    mean_check_comment_times,
     sum(a.comment_times_bf_submit) / count(b.recipient_phone) AS    mean_comment_times_bf_submit,
     sum(a.has_check_orders) / count(b.recipient_phone)        AS    mean_has_check_orders,
     sum(a.in_poi_with_orderlist) / count(b.recipient_phone)   AS    mean_in_poi_with_orderlist,
     sum(a.has_share_envelope) / count(b.recipient_phone)      AS    mean_hmean_as_share_envelope,
     sum(a.comment_times_af_submit) / count(b.recipient_phone) AS    mean_comment_times_af_submit,
     sum(a.time_after_submit) / count(b.recipient_phone)       AS    mean_time_after_submit
   FROM (SELECT
           uuid                                      AS uuid,
           sum(total_order_num) / count(dt)          AS day_total_order_num,
           sum(CASE WHEN order_mean_jiange > 0 AND order_mean_jiange < 10800
             THEN 1
               ELSE 0 END) / count(1)                AS mean_order_mean_jiange,
           sum(time_before_submit) / count(dt)       AS mean_time_before_submit,
           sum(search_times) / count(dt)             AS mean_search_times,
           sum(visit_pois_before_submit) / count(dt) AS mean_visit_pois_before_submit,
           sum(is_use_envelope) / count(dt)          AS is_use_envelope,
           sum(preview_times) / count(dt)            AS preview_times,
           sum(check_comment_times) / count(dt)      AS check_comment_times,
           sum(comment_times_bf_submit) / count(dt)  AS comment_times_bf_submit,
           sum(has_check_orders) / count(dt)         AS has_check_orders,
           sum(in_poi_with_orderlist) / count(dt)    AS in_poi_with_orderlist,
           sum(has_share_envelope) / count(dt)       AS has_share_envelope,
           sum(comment_times_af_submit) / count(dt)  AS comment_times_af_submit,
           sum(time_after_submit) / count(dt)        AS time_after_submit
         FROM mart_waimai_risk.stray_wm_user_action_dt
         WHERE dt > '20150601'
         GROUP BY uuid) a
     INNER JOIN (
                  SELECT
                    uuid,
                    phone recipient_phone
                  FROM
                    test.dim_user_relation__uuid_phone__line
                ) b
       ON
         a.uuid = b.uuid
   GROUP BY b.recipient_phone) a
  -- 基础特征
  LEFT JOIN
  (
    SELECT
      recipient_phone                            AS recipient_phone,
      sum(wm_order_pay_type - 1) / count(1)      AS online_pay_rate,
      -- 货到付款占比
      sum(user_actual_loc_in_sp_area) / count(1) AS in_scope_rate,
      sum(CASE WHEN user_actual_loc_dist_m != 999999999 AND user_actual_loc_dist_m > 8000
        THEN 1
          ELSE 0 END) / count(1)                 AS mean_dis,
      count(DISTINCT (aor_id))                   AS aor_id_num,
      count(DISTINCT (recipient_address))        AS recipient_address_num,
      count(1)                                   AS order_num
    FROM mart_waimai_risk.fact_ord_submitted_risk
    WHERE dt > '20150601'
    GROUP BY recipient_phone
    --     LIMIT 500
  ) b
    ON a.recipient_phone = b.recipient_phone
  LEFT JOIN (
              SELECT
                a.recipient_phone,
                sum(CASE WHEN a.rn == 1
                  THEN a.total
                    ELSE 0 END) / sum(a.total) AS fir_one_poi_rate,
                sum(CASE WHEN a.rn <= 2
                  THEN a.total
                    ELSE 0 END) / sum(a.total) AS sec_one_poi_rate
              FROM (SELECT
                      recipient_phone,
                      wm_poi_id,
                      count(*) AS  total,
                      ROW_NUMBER() OVER(PARTITION BY recipient_phone ORDER BY count(*) DESC) AS rn
                    FROM mart_waimai_risk.fact_ord_submitted_risk
                    WHERE dt > '20150601'
                    GROUP BY recipient_phone, wm_poi_id
                   ) a
              GROUP BY a.recipient_phone
            ) bb ON b.recipient_phone = bb.recipient_phone
  LEFT JOIN (
              --     用户非订单日的是否有用户行为。
              SELECT
                u.recipient_phone,
                cast(sum(order_dates) / sum(behaves_dates) AS FLOAT) order_behave_rate
              FROM (
                     SELECT
                       d_wm_order_uuid,
                       COUNT(DISTINCT dt) order_dates
                     FROM mart_waimai_risk.fact_ord_submitted_risk
                     WHERE dt > '20150601'
                     GROUP BY d_wm_order_uuid) a
                LEFT JOIN
                (
                  SELECT
                    uuid,
                    count(DISTINCT dt) behaves_dates
                  FROM mart_waimai.fact_xianfu_waimai_log_access
                  WHERE dt > '20150601'
                  GROUP BY uuid) b ON a.d_wm_order_uuid = b.uuid
                INNER JOIN
                (
                  SELECT
                    uuid,
                    phone recipient_phone
                  FROM
                    test.dim_user_relation__uuid_phone__line
                ) u ON u.uuid = a.d_wm_order_uuid
              GROUP BY u.recipient_phone
            ) bc ON b.recipient_phone = bc.recipient_phone
  LEFT JOIN (
              --     用户区域
              SELECT
                b.phone,
                max(user_suggest_type) user_suggest_type
              FROM
                (SELECT *
                 FROM mart_waimai_risk.fact_user_categoried
                 WHERE dt > '20150601') a
                INNER JOIN
                test.dim_user_relation__userid_phone__line b
                  ON a.userid = b.user_id
              GROUP BY
                b.phone
            ) bd ON b.recipient_phone = bd.phone
  --   用户大盘特征.
  LEFT JOIN (
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
  (
    SELECT
      a.phone AS                         phone,
      a.ord_num,
      a.act_cost / ord_num               act_cost_rate,
      a.punish_poi_num,
      a.addr_num,
      a.none_high_cnt / ord_num          none_high_cnt_rate,
      a.punish_status,
      a.max_duration_length,
      a.dm_num,
      regexp_replace(a.dm_set, ',', '.') dm_set,
      a.out_range_ord_cnt_act / ord_num  out_range_ord_cnt_act_rate,
      a.last_date,
      a.type  AS                         celue_source,
      b.punish_poi_ord as punish_poi_ord ,
      b.mean_punish_period_ord_rate,
      b.mean_punish_poi_ord_rate,
      b.neg_type
    FROM (
           SELECT
             a.type,
             b.*
           FROM (SELECT *
                 FROM mart_waimai_risk.wm_phone_blacklist
                 WHERE valid = 1 AND (type = 2 OR type = 1 OR type = 3)) a -- 14890
             INNER JOIN (SELECT *
                         FROM mart_waimai_risk.dim_user_property_risk
                         WHERE punish_status = 1 AND ord_num >= 10 AND last_date > '20150601') b
               ON a.phone = b.phone -- 12857b
           UNION ALL
           SELECT
             0 as type,
             b.*
           FROM mart_waimai_risk.dim_user_property_risk b
           WHERE punish_status = 0 AND ord_num >= 10 AND last_date > '20150601'
         ) a
      LEFT JOIN
      (SELECT
         b.phone,
         a.mean_punish_period_ord_rate,
         a.mean_punish_poi_ord_rate,
         a.punish_poi_ord,
         a.neg_type
       FROM (SELECT
               user_id,
               avg(punish_period_ord_rate) AS mean_punish_period_ord_rate,
               avg(punish_poi_ord_rate)    AS mean_punish_poi_ord_rate,
               max(neg_type)               AS neg_type,
               sum(punish_poi_ord)         AS punish_poi_ord
             FROM test.stray_wm_negetive_user_in_punish_period a
             WHERE dt > '20150601'
             GROUP BY user_id) a
         INNER JOIN
         test.dim_user_relation__userid_phone__line b ON a.user_id = b.user_id
      ) b ON a.phone = b.phone
    WHERE a.ord_num >= 10) e ON e.phone = b.recipient_phone
-- WHERE b.recipient_phone = '18600218186'
GROUP BY b.recipient_phone