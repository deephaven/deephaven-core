SELECT
  *
FROM
  time_1,
  time_2,
  time_3
WHERE
  time_1.I = time_2.I
  AND time_2.I = time_3.I
  AND time_1."R" <= time_2."R"
  AND time_2."R" <= time_3."R"
