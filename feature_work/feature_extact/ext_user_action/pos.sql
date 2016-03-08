SELECT *
FROM test.dim_user_feature
WHERE punish_status = 1
UNION ALL
SELECT *
FROM test.dim_user_feature
     TABLESAMPLE(BUCKET 1 OUT OF 20 ON order_num)
WHERE punish_status = 0