select b.uuid,name
from (SELECT
  a.uuid,
  split(regexp_replace(substr(a.phone_list, 2, length(a.phone_list) - 2), '"', ''), ',') phones
FROM (SELECT
        a.uuid,
        phone_list
      FROM mart_waimai_risk.dim_user_relation__uuid_risk a
     LATERAL VIEW json_tuple(a.uuid_relations, 'recipient_phone') js AS phone_list
LIMIT 1000) a ) b
lateral view explode(b.phones) col3 as name;

