SELECT
  source,
  name
FROM
  (
    SELECT
      name,
      1 AS source
    FROM
      AUTHORS
    UNION ALL
    SELECT
      title AS name,
      2 AS source
    FROM
      BOOKS
  )
