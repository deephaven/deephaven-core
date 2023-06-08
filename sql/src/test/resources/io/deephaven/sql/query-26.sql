SELECT
  *
FROM
  my_time
WHERE
  (
    I + 1 <= 5 + 1
    or I >= 10
  )
  and (I IS NOT NULL)
  and (B IS NOT NULL)
