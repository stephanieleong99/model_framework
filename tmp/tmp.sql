SELECT
  a.phone AS phone,
  1       AS is_tuancan,
  1       AS is_nodiscount,
  1       AS is_interval_long,
  1       AS is_discount_low
FROM test.dim_user_testusers

  union ALL
-- 测试账号
SELECT
  13161362350 AS phone,
  1           AS is_tuancan,
  1           AS is_nodiscount,
  1           AS is_interval_long,
  1           AS is_discount_low
UNION ALL
-- 测试账号
SELECT
  15910857012 AS phone,
  1           AS is_tuancan,
  1           AS is_nodiscount,
  1           AS is_interval_long,
  1           AS is_discount_low
UNION ALL
-- 测试账号
SELECT
  15201587849 AS phone,
  1           AS is_tuancan,
  1           AS is_nodiscount,
  1           AS is_interval_long,
  1           AS is_discount_low
UNION ALL
-- 测试账号
SELECT
  13488896081 AS phone,
  1           AS is_tuancan,
  1           AS is_nodiscount,
  1           AS is_interval_long,
  1           AS is_discount_low
UNION ALL
-- 测试账号
SELECT
  18515357626 AS phone,
  1           AS is_tuancan,
  1           AS is_nodiscount,
  1           AS is_interval_long,
  1           AS is_discount_low
UNION ALL
-- 测试账号
SELECT
  18511303864 AS phone,
  1           AS is_tuancan,
  1           AS is_nodiscount,
  1           AS is_interval_long,
  1           AS is_discount_low