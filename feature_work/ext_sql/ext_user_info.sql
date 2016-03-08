SELECT TRANSFORM(d.d_wm_order_uuid, d.id, e.wm_poi_id, e.order_source, d_appver, d.page_url_list)
  USING 'python parser_wm_order_all_page_url_0727.py'
  AS (d_wm_order_uuid, id, poi_id, order_source, appver, page_url_list)
--select d.d_wm_order_uuid,d.id ,e.wm_poi_id as poi_id,e.order_source,d_appver as appver,d.page_url_list
FROM
  (

    SELECT TRANSFORM(c.*)
    USING 'python merge_wm_order_serverlog_new_0727.py'
      AS (d_wm_order_uuid, id, page_url_list)

    FROM (
           SELECT
             d_wm_order_uuid,
             id,
             order_source,
             time,
             wm_ctime,
             page_url,
             customerId,
             dId,
             order_time
           FROM (
                  SELECT
                    d_wm_order_uuid,
                    order_time,
                    id,
                    order_source
                  FROM
                    mart_waimai_risk.fact_act_base_risk
                  WHERE dt = '$now.datekey' AND d_wm_order_uuid <> '' AND d_wm_order_uuid IS NOT NULL
                ) a
             LEFT OUTER JOIN
             (
               SELECT
                 uuid,
                 time,
                 wm_ctime,
                 page_url,
                 customerId,
                 dId
               FROM
                 mart_waimai.fact_xianfu_waimai_log_access
               WHERE dt = '$now.datekey' AND uuid <> '' AND uuid IS NOT NULL AND page_url <> '' AND page_url IS NOT NULL
             ) b
               ON (a.d_wm_order_uuid = b.uuid)
           WHERE page_url IS NOT NULL
         distribute BY d_wm_order_uuid
sort BY d_wm_order_uuid
)c

)d
LEFT OUTER JOIN
( SELECT id, wm_poi_id, order_source, d_appver
FROM
mart_waimai_risk.fact_act_base_risk
WHERE dt='$now.datekey'
)e
ON (d.id=e.id)
