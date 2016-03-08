select *
FROM
  (

    SELECT *
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
                    mart_waimai_risk.fact_ord_submitted_risk
                  WHERE dt = '20160302' AND d_wm_order_uuid <> '' AND d_wm_order_uuid IS NOT NULL
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
               WHERE dt = '20160302' AND uuid <> '' AND uuid IS NOT NULL AND page_url <> '' AND page_url IS NOT NULL
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
mart_waimai_risk.fact_ord_submitted_risk
WHERE dt='20160302'
)e
ON (d.id=e.id)
limit 200000