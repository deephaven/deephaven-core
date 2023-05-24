SELECT
  Time1 as "Timestamp", 1 as "PARENT"
FROM
  my_time_1
UNION ALL
SELECT
  Time2 as "Timestamp", 2 as "PARENT"
FROM
  my_time_2
ORDER BY "Timestamp"
