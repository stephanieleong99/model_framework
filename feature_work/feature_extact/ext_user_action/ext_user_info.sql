SELECT
  transform(
  a.uuid,
  a.time,
  a.wm_ctime,
  a.page_url,
  a.customerId,
  a.dId)
USING 'python cal_order_action.py.py -m online '
AS (order_id, poi_id,start_time, submit_time, time_before_submit,
                                   visit_pois_before_submit, search_times)
FROM
(select * from mart_waimai.fact_xianfu_waimai_log_access
WHERE dt = '20160302' AND uuid <> '' AND uuid IS NOT NULL AND page_url <> '' AND page_url IS NOT NULL
distribute by uuid
sort by uuid
) a
