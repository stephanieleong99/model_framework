SELECT

  a.uuid,
  a.time,
  a.wm_ctime,
  a.page_url,
  a.customerId,
  a.dId
FROM
(SELECT
  uuid,
  time,
  wm_ctime,
  page_url,
  customerId,
  dId
FROM
mart_waimai.fact_xianfu_waimai_log_access
WHERE dt = '20150910' AND uuid <> '' AND uuid IS NOT NULL AND page_url <> '' AND page_url IS NOT NULL
distribute by uuid
sort by uuid) a