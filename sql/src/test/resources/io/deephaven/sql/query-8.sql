SELECT
  source,
  name
FROM
  (
    SELECT
      Name AS name,
      1 AS source
    FROM
      authors
    UNION ALL
    SELECT
      Title AS name,
      2 AS source
    FROM
      books
  )
