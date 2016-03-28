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
   WHERE dt = '20160322' AND uuid <> '' AND uuid IS NOT NULL AND page_url <> '' AND page_url IS NOT NULL AND
         uuid = '521BF886936908E7E1A697E70B264C5B43794C1C22CBB68EF232784E5DCD6C3A'
  distribute BY uuid
sort BY uuid, time) a
