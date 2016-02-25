SELECT
  wm_poi_id,
  recipient_phone,
  count(id) AS ord_num
FROM
  mart_waimai_risk.fact_ord_arranged_risk a
  JOIN
  (
    SELECT phone
    FROM
      mart_waimai_risk.dim_user_punish_and_complain_record_risk
    WHERE
      punish_type = 1
      OR
      punish_type = 2
      AND
      dt BETWEEN '20160101' AND '20160129') b
    ON
      a.recipient_phone = b.phone
WHERE
  wm_poi_id IN (255718, 191410)
AND
a.dt BETWEEN '20160101' AND '20160129'
GROUP BY recipient_phone, wm_poi_id