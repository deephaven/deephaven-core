SELECT
  MyTable.b,
  a
FROM
  (
    VALUES
      (1, 'one'),
      (2, NULL),
      (3, 'three')
  ) AS MyTable(a, b)
